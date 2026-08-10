from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.orders.config import DB_COLUMNS, KEY_COLUMNS, SCHEMA_DEFINITION, TABLE_NAME

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class OrdersSaveResult:
    """Итог подготовки и записи заказов WB в PostgreSQL."""

    input_rows: int
    rows_after_missing_keys_filter: int
    rows_after_key_deduplication: int
    dropped_missing_key_rows: int
    collapsed_duplicate_rows: int
    written_rows: int


class OrdersRepository:
    """Слой сохранения заказов WB в PostgreSQL."""

    def save(self, dataframe: pd.DataFrame) -> OrdersSaveResult:
        """Фильтрует технически неполные строки и выполняет upsert заказов.

        Бизнес-правило: повторный запуск за тот же период обновляет строку по
        ключу `(date, srid)`, а не создает дубль заказа.
        """
        input_rows = len(dataframe.index)
        prepared_df, dropped_missing_key_rows = self._drop_rows_with_missing_keys(dataframe)
        rows_after_missing_keys_filter = len(prepared_df.index)

        deduplicated_df, collapsed_duplicate_rows = self._deduplicate_by_keys(prepared_df)
        rows_after_key_deduplication = len(deduplicated_df.index)

        if deduplicated_df.empty:
            logger.warning(
                "После подготовки заказов WB не осталось строк для записи в PostgreSQL."
            )
            return OrdersSaveResult(
                input_rows=input_rows,
                rows_after_missing_keys_filter=rows_after_missing_keys_filter,
                rows_after_key_deduplication=rows_after_key_deduplication,
                dropped_missing_key_rows=dropped_missing_key_rows,
                collapsed_duplicate_rows=collapsed_duplicate_rows,
                written_rows=0,
            )

        Database.sync_data_to_postgres(
            table_name=TABLE_NAME,
            data=self._prepare_dataframe_for_database(deduplicated_df),
            schema_definition=SCHEMA_DEFINITION,
            unique_keys=KEY_COLUMNS,
        )
        logger.info(
            "Upsert заказов WB завершен | table=%s | written_rows=%s",
            TABLE_NAME,
            len(deduplicated_df.index),
        )
        return OrdersSaveResult(
            input_rows=input_rows,
            rows_after_missing_keys_filter=rows_after_missing_keys_filter,
            rows_after_key_deduplication=rows_after_key_deduplication,
            dropped_missing_key_rows=dropped_missing_key_rows,
            collapsed_duplicate_rows=collapsed_duplicate_rows,
            written_rows=len(deduplicated_df.index),
        )

    def _drop_rows_with_missing_keys(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Удаляет строки без ключевых полей, чтобы upsert заказов не прерывался ошибкой БД."""
        if dataframe.empty:
            return dataframe.copy(), 0

        missing_key_mask = dataframe.loc[:, list(KEY_COLUMNS)].isnull().any(axis=1)
        missing_count = int(missing_key_mask.sum())
        if missing_count:
            sample_rows = (
                dataframe.loc[missing_key_mask, list(KEY_COLUMNS)]
                .head(10)
                .to_dict(orient="records")
            )
            logger.warning(
                "Удаляются заказы WB с пустыми ключами | key_columns=%s | rows=%s | sample_rows=%s",
                KEY_COLUMNS,
                missing_count,
                sample_rows,
            )

        return dataframe.loc[~missing_key_mask].copy(), missing_count

    def _deduplicate_by_keys(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Сворачивает дубли заказов по ключу, защищая повторную загрузку WB от конфликтов в batch."""
        if dataframe.empty:
            return dataframe.copy(), 0

        duplicate_mask = dataframe.duplicated(subset=list(KEY_COLUMNS), keep=False)
        duplicate_rows = dataframe.loc[duplicate_mask].copy()
        duplicate_count = len(duplicate_rows.index)
        if duplicate_count:
            top_duplicate_keys = (
                duplicate_rows.groupby(list(KEY_COLUMNS), dropna=False)
                .size()
                .reset_index(name="duplicate_count")
                .sort_values(by="duplicate_count", ascending=False)
                .head(10)
                .to_dict(orient="records")
            )
            logger.warning(
                "Найдены дубли заказов WB по ключу, сохраняется последняя строка | key_columns=%s | duplicate_rows=%s | top_duplicate_keys=%s",
                KEY_COLUMNS,
                duplicate_count,
                top_duplicate_keys,
            )

        deduplicated_df = dataframe.drop_duplicates(
            subset=list(KEY_COLUMNS),
            keep="last",
        ).copy()
        collapsed_rows = duplicate_count - len(
            duplicate_rows.drop_duplicates(subset=list(KEY_COLUMNS), keep="last").index
        )
        return deduplicated_df, max(collapsed_rows, 0)

    def _prepare_dataframe_for_database(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Финально готовит заказы к передаче в общий upsert PostgreSQL.

        Бизнес-правило: отсутствующие даты отмены и другие пустые поля WB должны
        записываться как SQL NULL, а не как pandas-значения `NaT` или `NA`.
        """
        db_dataframe = dataframe.loc[:, list(DB_COLUMNS)].copy()
        return db_dataframe.astype(object).where(pd.notna(db_dataframe), None)
