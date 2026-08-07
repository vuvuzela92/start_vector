from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from src_oop.jobs.advert_spend.config import (
    COLUMN_RENAME_MAPPING,
    DATE_ONLY_COLUMNS,
    DATETIME_COLUMNS,
    DB_COLUMNS,
    INT_COLUMNS,
    NUMERIC_COLUMNS,
    TEXT_COLUMNS,
)

logger = logging.getLogger(__name__)

SPECIAL_TEXT_NULLS = {"", "nan", "nat", "none", "null", "inf", "-inf", "+inf"}


class AdvertSpendNormalizer:
    """Нормализует строки рекламных затрат WB перед записью в PostgreSQL."""

    def normalize(
        self,
        account_payloads: list[list[dict]],
        date_from: date,
        date_to: date,
    ) -> pd.DataFrame:
        raw_rows = [
            row
            for payload in account_payloads
            for row in payload
            if isinstance(row, dict)
        ]
        if not raw_rows:
            logger.info("Нет строк рекламных затрат WB для нормализации.")
            return pd.DataFrame(columns=list(DB_COLUMNS))

        dataframe = pd.DataFrame.from_records(raw_rows)
        logger.info(
            "Старт нормализации advert_spend | rows_before=%s | columns_before=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )

        if "updTime" in dataframe.columns:
            dataframe["date"] = pd.to_datetime(
                dataframe["updTime"],
                format="ISO8601",
                errors="coerce",
            ).dt.date

        dataframe = self._filter_by_local_upd_date(
            dataframe=dataframe,
            date_from=date_from,
            date_to=date_to,
        )
        if dataframe.empty:
            logger.info(
                "После фильтрации по локальной дате advert_spend нет строк | date_from=%s | date_to=%s",
                date_from.isoformat(),
                date_to.isoformat(),
            )
            return pd.DataFrame(columns=list(DB_COLUMNS))

        normalized_df = dataframe.rename(columns=COLUMN_RENAME_MAPPING).copy()

        missing_columns = [column for column in DB_COLUMNS if column not in normalized_df.columns]
        for column in missing_columns:
            normalized_df[column] = pd.NA

        if missing_columns:
            logger.info(
                "В DataFrame advert_spend добавлены отсутствующие колонки | missing_columns=%s",
                missing_columns,
            )

        for column in INT_COLUMNS:
            source_series = self._sanitize_special_strings(normalized_df[column])
            numeric_series = pd.to_numeric(source_series, errors="coerce")
            numeric_series = numeric_series.mask(np.isinf(numeric_series), pd.NA)
            whole_number_mask = numeric_series.notna() & ((numeric_series % 1) != 0)
            normalized_df[column] = numeric_series.mask(whole_number_mask, pd.NA).astype("Int64")

        for column in NUMERIC_COLUMNS:
            source_series = self._sanitize_special_strings(normalized_df[column])
            numeric_series = pd.to_numeric(source_series, errors="coerce")
            normalized_df[column] = numeric_series.mask(np.isinf(numeric_series), pd.NA)

        for column in DATETIME_COLUMNS:
            normalized_df[column] = pd.to_datetime(
                self._sanitize_special_strings(normalized_df[column]),
                format="ISO8601",
                errors="coerce",
            )

        for column in DATE_ONLY_COLUMNS:
            normalized_df[column] = pd.to_datetime(
                self._sanitize_special_strings(normalized_df[column]),
                errors="coerce",
            ).dt.date

        for column in TEXT_COLUMNS:
            normalized_df[column] = normalized_df[column].map(
                lambda value: None if pd.isna(value) else str(value)
            )

        normalized_df = normalized_df.loc[:, list(DB_COLUMNS)].astype(object)
        normalized_df = normalized_df.where(pd.notnull(normalized_df), None)
        logger.info(
            "Нормализация advert_spend завершена | rows_after=%s | columns=%s",
            len(normalized_df.index),
            list(normalized_df.columns),
        )
        return normalized_df

    def _sanitize_special_strings(self, series: pd.Series) -> pd.Series:
        string_series = series.astype("string").str.strip().str.lower()
        return series.mask(string_series.isin(SPECIAL_TEXT_NULLS), pd.NA)

    def _filter_by_local_upd_date(
        self,
        dataframe: pd.DataFrame,
        date_from: date,
        date_to: date,
    ) -> pd.DataFrame:
        if "date" not in dataframe.columns:
            raise ValueError("В ответе advert_spend отсутствует updTime для фильтрации по дате.")

        date_mask = dataframe["date"].between(date_from, date_to)
        dropped_rows = int((~date_mask).sum())
        if dropped_rows:
            logger.info(
                "Отфильтрованы строки advert_spend вне локального периода updTime | date_from=%s | date_to=%s | dropped_rows=%s",
                date_from.isoformat(),
                date_to.isoformat(),
                dropped_rows,
            )
        return dataframe.loc[date_mask].copy()
