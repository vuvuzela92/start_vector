"""Сохранение страниц WB Order Feed в PostgreSQL."""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src_oop.core.database import Database
from src_oop.jobs.orders_feed.config import (
    DB_COLUMNS,
    KEY_COLUMNS,
    TABLE_NAME,
)
from src_oop.jobs.orders_feed.models import (
    OrderFeedBase,
    OrderFeedSaveResult,
    WBOrderFeedRecord,
)

logger = logging.getLogger(__name__)


class OrderFeedRepository:
    """Создаёт витрину при первом запуске и выполняет идемпотентный upsert страниц."""

    def save(self, dataframe: pd.DataFrame) -> OrderFeedSaveResult:
        """Сохраняет страницу сразу после получения, чтобы сбой не потерял предыдущие страницы."""
        input_rows = len(dataframe.index)
        prepared, dropped = self._drop_rows_with_missing_keys(dataframe)
        deduplicated, collapsed = self._deduplicate_by_keys(prepared)
        if deduplicated.empty:
            return OrderFeedSaveResult(input_rows, 0, dropped, collapsed)
        self.create_table()
        self._upsert(deduplicated)
        logger.info(
            "Страница Order Feed сохранена через upsert | table=%s | rows=%s",
            TABLE_NAME,
            len(deduplicated.index),
        )
        return OrderFeedSaveResult(input_rows, len(deduplicated.index), dropped, collapsed)

    def create_table(self) -> None:
        """Создаёт таблицу, PostgreSQL enum-типы и аналитические индексы из ORM-модели."""
        OrderFeedBase.metadata.create_all(Database.get_engine(), checkfirst=True)

    def _upsert(self, dataframe: pd.DataFrame) -> None:
        """Обновляет текущий статус заказа по составному primary key `(account, srid)`."""
        prepared = dataframe.loc[:, list(DB_COLUMNS)].astype(object).where(
            pd.notna(dataframe), None
        )
        records = prepared.to_dict(orient="records")
        statement = insert(WBOrderFeedRecord.__table__).values(records)
        update_columns = {
            column.name: getattr(statement.excluded, column.name)
            for column in WBOrderFeedRecord.__table__.columns
            if column.name not in KEY_COLUMNS
        }
        upsert_statement = statement.on_conflict_do_update(
            index_elements=list(KEY_COLUMNS),
            set_=update_columns,
        )
        with Database.get_engine().begin() as connection:
            connection.execute(upsert_statement)

    def _drop_rows_with_missing_keys(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Отбрасывает строки без account или srid, поскольку их нельзя безопасно обновлять."""
        if dataframe.empty:
            return dataframe.copy(), 0
        missing_mask = dataframe.loc[:, list(KEY_COLUMNS)].isnull().any(axis=1)
        count = int(missing_mask.sum())
        if count:
            logger.warning(
                "Строки Order Feed без уникального ключа пропущены | rows=%s | keys=%s",
                count,
                KEY_COLUMNS,
            )
        return dataframe.loc[~missing_mask].copy(), count

    def _deduplicate_by_keys(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Оставляет самый новый статус заказа при дублях внутри одной страницы WB."""
        if dataframe.empty:
            return dataframe.copy(), 0
        ordered = dataframe.sort_values("updated_at", kind="stable", na_position="first")
        result = ordered.drop_duplicates(subset=list(KEY_COLUMNS), keep="last").copy()
        collapsed = len(dataframe.index) - len(result.index)
        if collapsed:
            logger.warning(
                "Дубли Order Feed внутри страницы свёрнуты по новейшему статусу | rows=%s",
                collapsed,
            )
        return result, collapsed
