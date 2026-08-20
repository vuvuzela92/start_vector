from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.funnel_sales.config import DB_COLUMNS, EXCLUDED_NM_IDS_BY_DATE, KEY_COLUMNS, SCHEMA_DEFINITION, TABLE_NAME

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FunnelSalesSaveResult:
    """Итог подготовки и записи ежедневной воронки продаж WB в PostgreSQL."""

    input_rows: int
    rows_after_drop_duplicates: int
    rows_after_business_deduplication: int
    rows_after_exclusions: int
    removed_exact_duplicates: int
    collapsed_business_duplicates: int
    excluded_rows: int
    written_rows: int


class FunnelSalesRepository:
    """Слой подготовки и сохранения ежедневной воронки продаж WB в PostgreSQL."""

    def save(self, dataframe: pd.DataFrame) -> FunnelSalesSaveResult:
        """Применяет legacy-правила очистки и выполняет upsert в funnel_daily.

        Бизнес-правила: сначала удаляются полные дубли строк, затем по ключу
        `(nm_id, date)` сохраняется запись с максимальным `orders_sum`, а при
        равенстве метрик приоритет получает строка с большим `open_count`.
        """
        input_rows = len(dataframe.index)
        deduplicated_df = dataframe.drop_duplicates().copy()
        rows_after_drop_duplicates = len(deduplicated_df.index)
        removed_exact_duplicates = input_rows - rows_after_drop_duplicates

        business_df, collapsed_business_duplicates = self._deduplicate_by_business_key(deduplicated_df)
        rows_after_business_deduplication = len(business_df.index)

        filtered_df, excluded_rows = self._drop_excluded_rows(business_df)
        rows_after_exclusions = len(filtered_df.index)

        if filtered_df.empty:
            logger.warning(
                "После подготовки ежедневной воронки продаж WB не осталось строк для записи в PostgreSQL."
            )
            return FunnelSalesSaveResult(
                input_rows=input_rows,
                rows_after_drop_duplicates=rows_after_drop_duplicates,
                rows_after_business_deduplication=rows_after_business_deduplication,
                rows_after_exclusions=rows_after_exclusions,
                removed_exact_duplicates=removed_exact_duplicates,
                collapsed_business_duplicates=collapsed_business_duplicates,
                excluded_rows=excluded_rows,
                written_rows=0,
            )

        Database.sync_data_to_postgres(
            table_name=TABLE_NAME,
            data=self._prepare_dataframe_for_database(filtered_df),
            schema_definition=SCHEMA_DEFINITION,
            unique_keys=KEY_COLUMNS,
        )
        logger.info(
            "Upsert ежедневной воронки продаж WB завершён | table=%s | written_rows=%s",
            TABLE_NAME,
            len(filtered_df.index),
        )
        return FunnelSalesSaveResult(
            input_rows=input_rows,
            rows_after_drop_duplicates=rows_after_drop_duplicates,
            rows_after_business_deduplication=rows_after_business_deduplication,
            rows_after_exclusions=rows_after_exclusions,
            removed_exact_duplicates=removed_exact_duplicates,
            collapsed_business_duplicates=collapsed_business_duplicates,
            excluded_rows=excluded_rows,
            written_rows=len(filtered_df.index),
        )

    def _deduplicate_by_business_key(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Сворачивает конфликтующие строки воронки по ключу `(nm_id, date)`.

        Бизнес-логика: legacy-скрипт старался оставить самую сильную запись дня,
        ориентируясь на `orders_sum`, а затем на `open_count`. Здесь это правило
        делается детерминированным и повторяемым.
        """
        if dataframe.empty:
            return dataframe.copy(), 0

        sorted_df = dataframe.sort_values(
            by=["nm_id", "date", "orders_sum", "open_count"],
            ascending=[True, True, False, False],
            na_position="last",
        ).copy()
        deduplicated_df = sorted_df.drop_duplicates(subset=list(KEY_COLUMNS), keep="first").copy()
        collapsed_rows = len(sorted_df.index) - len(deduplicated_df.index)
        if collapsed_rows > 0:
            logger.warning(
                "Свернуты конфликтующие строки ежедневной воронки WB по ключу `(nm_id, date)` | collapsed_rows=%s",
                collapsed_rows,
            )
        return deduplicated_df, max(collapsed_rows, 0)

    def _drop_excluded_rows(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Удаляет исторически исключённые строки, которые legacy-сценарий считал некорректными.

        Бизнес-логика: правило сохранено без изменений, чтобы перенос в OOP не
        вернул обратно проблемные записи в аналитическую витрину.
        """
        if dataframe.empty:
            return dataframe.copy(), 0

        exclusion_mask = pd.Series(False, index=dataframe.index)
        for nm_id, iso_dates in EXCLUDED_NM_IDS_BY_DATE:
            blocked_dates = {date.fromisoformat(value) for value in iso_dates}
            exclusion_mask = exclusion_mask | (
                (dataframe["nm_id"] == nm_id) & dataframe["date"].isin(blocked_dates)
            )

        excluded_rows = int(exclusion_mask.sum())
        if excluded_rows:
            logger.warning(
                "Удалены исторически исключённые строки ежедневной воронки WB | excluded_rows=%s | rules=%s",
                excluded_rows,
                EXCLUDED_NM_IDS_BY_DATE,
            )
        return dataframe.loc[~exclusion_mask].copy(), excluded_rows

    def _prepare_dataframe_for_database(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Финально готовит daily funnel к передаче в общий upsert PostgreSQL.

        Бизнес-правило: пустые значения воронки должны записываться как SQL NULL,
        чтобы пропуски не ломали аналитику и не превращались в служебные `NA`.
        """
        db_dataframe = dataframe.loc[:, list(DB_COLUMNS)].copy()
        return db_dataframe.astype(object).where(pd.notna(db_dataframe), None)
