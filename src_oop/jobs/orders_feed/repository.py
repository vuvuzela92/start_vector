"""Сохранение страниц WB Order Feed в PostgreSQL."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from sqlalchemy.dialects.postgresql import insert

from src_oop.core.database import Database
from src_oop.jobs.orders_feed.config import KEY_COLUMNS, TABLE_NAME
from src_oop.jobs.orders_feed.models import (
    OrderFeedBase,
    OrderFeedSaveResult,
    WBOrderFeedRecord,
)
from src_oop.jobs.orders_feed.schemas.database import OrderFeedDatabaseRow

logger = logging.getLogger(__name__)


class OrderFeedRepository:
    """Создаёт витрину при первом запуске и выполняет идемпотентный upsert страниц."""

    def save(self, rows: Sequence[OrderFeedDatabaseRow]) -> OrderFeedSaveResult:
        """Сохраняет страницу сразу после получения, чтобы сбой не потерял предыдущие страницы."""
        input_rows = len(rows)
        deduplicated, collapsed = self._deduplicate_by_keys(rows)
        if not deduplicated:
            return OrderFeedSaveResult(input_rows, 0, 0, collapsed)
        self.create_table()
        self._upsert(deduplicated)
        logger.info(
            "Страница Order Feed сохранена через upsert | table=%s | rows=%s",
            TABLE_NAME,
            len(deduplicated),
        )
        return OrderFeedSaveResult(input_rows, len(deduplicated), 0, collapsed)

    def create_table(self) -> None:
        """Создаёт таблицу, PostgreSQL enum-типы и аналитические индексы из ORM-модели."""
        OrderFeedBase.metadata.create_all(
            Database.get_engine(),
            tables=[WBOrderFeedRecord.__table__],
            checkfirst=True,
        )
        logger.info(
            "Таблица Order Feed и связанные enum-типы готовы | table=%s",
            TABLE_NAME,
        )

    def _upsert(self, rows: Sequence[OrderFeedDatabaseRow]) -> None:
        """Обновляет текущий статус заказа по составному primary key `(account, srid)`."""
        records = [row.model_dump(mode="python") for row in rows]
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

    def _deduplicate_by_keys(
        self,
        rows: Sequence[OrderFeedDatabaseRow],
    ) -> tuple[list[OrderFeedDatabaseRow], int]:
        """Оставляет самый новый статус заказа при дублях внутри одной страницы WB."""
        rows_by_key: dict[tuple[str, str], OrderFeedDatabaseRow] = {}
        for row in rows:
            key = (row.account, row.srid)
            current = rows_by_key.get(key)
            if current is None or row.updated_at >= current.updated_at:
                rows_by_key[key] = row
        result = list(rows_by_key.values())
        collapsed = len(rows) - len(result)
        if collapsed:
            logger.warning(
                "Дубли Order Feed внутри страницы свёрнуты по новейшему статусу | rows=%s",
                collapsed,
            )
        return result, collapsed
