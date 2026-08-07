from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.jobs.advert_spend.config import (
    DB_COLUMNS,
    KEY_COLUMNS,
    SCHEMA_DEFINITION,
    TABLE_NAME,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AdvertSpendSaveResult:
    input_rows: int
    rows_after_missing_keys_filter: int
    rows_after_key_deduplication: int
    dropped_missing_key_rows: int
    collapsed_duplicate_rows: int
    written_rows: int
    cleaned_days: int


class AdvertSpendRepository:
    """Записывает рекламные затраты WB в PostgreSQL."""

    def __init__(self, database_cls: type[Database] = Database) -> None:
        self.database_cls = database_cls

    def save(
        self,
        dataframe: pd.DataFrame,
        date_from: date,
        date_to: date,
        accounts: Sequence[str],
    ) -> AdvertSpendSaveResult:
        input_rows = len(dataframe.index)
        cleaned_days = (date_to - date_from).days + 1
        prepared_df, dropped_missing_key_rows = self._drop_rows_with_missing_keys(dataframe)
        rows_after_missing_keys_filter = len(prepared_df.index)
        deduplicated_df, collapsed_duplicate_rows = self._deduplicate_by_keys(prepared_df)
        rows_after_key_deduplication = len(deduplicated_df.index)
        self._clean_rows_for_period_and_accounts(
            date_from=date_from,
            date_to=date_to,
            accounts=accounts,
        )

        if deduplicated_df.empty:
            logger.warning("После подготовки advert_spend нет строк для записи.")
            return AdvertSpendSaveResult(
                input_rows=input_rows,
                rows_after_missing_keys_filter=rows_after_missing_keys_filter,
                rows_after_key_deduplication=rows_after_key_deduplication,
                dropped_missing_key_rows=dropped_missing_key_rows,
                collapsed_duplicate_rows=collapsed_duplicate_rows,
                written_rows=0,
                cleaned_days=cleaned_days,
            )

        self.database_cls.sync_data_to_postgres(
            table_name=TABLE_NAME,
            data=deduplicated_df.loc[:, list(DB_COLUMNS)],
            schema_definition=SCHEMA_DEFINITION,
            unique_keys=KEY_COLUMNS,
        )
        logger.info(
            "Upsert advert_spend завершён | table=%s | written_rows=%s",
            TABLE_NAME,
            len(deduplicated_df.index),
        )
        return AdvertSpendSaveResult(
            input_rows=input_rows,
            rows_after_missing_keys_filter=rows_after_missing_keys_filter,
            rows_after_key_deduplication=rows_after_key_deduplication,
            dropped_missing_key_rows=dropped_missing_key_rows,
            collapsed_duplicate_rows=collapsed_duplicate_rows,
            written_rows=len(deduplicated_df.index),
            cleaned_days=cleaned_days,
        )

    def _drop_rows_with_missing_keys(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        if dataframe.empty:
            return dataframe.copy(), 0

        missing_key_mask = dataframe.loc[:, list(KEY_COLUMNS)].isnull().any(axis=1)
        missing_count = int(missing_key_mask.sum())
        if missing_count:
            logger.warning(
                "Удаляются строки advert_spend с пустыми ключами | key_columns=%s | rows=%s | sample_rows=%s",
                KEY_COLUMNS,
                missing_count,
                dataframe.loc[missing_key_mask, list(KEY_COLUMNS)]
                .head(10)
                .to_dict(orient="records"),
            )
        return dataframe.loc[~missing_key_mask].copy(), missing_count

    def _deduplicate_by_keys(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        if dataframe.empty:
            return dataframe.copy(), 0

        duplicate_mask = dataframe.duplicated(subset=list(KEY_COLUMNS), keep=False)
        duplicate_rows = dataframe.loc[duplicate_mask].copy()
        duplicate_count = len(duplicate_rows.index)
        if duplicate_count:
            grouped_duplicates = (
                duplicate_rows.groupby(list(KEY_COLUMNS), dropna=False)
                .size()
                .reset_index(name="duplicate_count")
                .sort_values(by="duplicate_count", ascending=False)
                .head(10)
                .to_dict(orient="records")
            )
            logger.warning(
                "Найдены дубли advert_spend по ключу, сохраняется последняя строка | key_columns=%s | duplicate_rows=%s | top_duplicate_keys=%s",
                KEY_COLUMNS,
                duplicate_count,
                grouped_duplicates,
            )

        deduplicated_df = dataframe.drop_duplicates(
            subset=list(KEY_COLUMNS),
            keep="last",
        ).copy()
        collapsed_rows = duplicate_count - len(
            duplicate_rows.drop_duplicates(subset=list(KEY_COLUMNS), keep="last").index
        )
        return deduplicated_df, max(collapsed_rows, 0)

    def _clean_rows_for_period_and_accounts(
        self,
        date_from: date,
        date_to: date,
        accounts: Sequence[str],
    ) -> None:
        clean_accounts = sorted({account for account in accounts if account})
        if not clean_accounts:
            logger.warning(
                "Очистка advert_spend пропущена: нет аккаунтов с успешным ответом API."
            )
            return

        logger.info(
            "Очистка advert_spend перед записью | table=%s | date_from=%s | date_to=%s | accounts=%s",
            TABLE_NAME,
            date_from.isoformat(),
            date_to.isoformat(),
            clean_accounts,
        )
        sql_stmt = text(
            f"DELETE FROM {TABLE_NAME} WHERE date::date BETWEEN :date_from AND :date_to AND account = :account"
        )
        engine = self.database_cls.get_engine()
        with engine.begin() as connection:
            for account in clean_accounts:
                connection.execute(
                    sql_stmt,
                    {
                        "date_from": date_from,
                        "date_to": date_to,
                        "account": account,
                    },
                )
