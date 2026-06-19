from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from src_oop.jobs.advert.config import (
    DATE_COLUMNS,
    DB_COLUMNS,
    INT_COLUMNS,
    NUMERIC_2_COLUMNS,
    NUMERIC_4_COLUMNS,
    NUMERIC_COLUMNS,
    TEXT_COLUMNS,
)

logger = logging.getLogger(__name__)

SPECIAL_TEXT_NULLS = {"", "nan", "nat", "none", "null", "inf", "-inf", "+inf"}


class AdvertStatsNormalizer:
    """Нормализует DataFrame рекламной статистики перед записью в PostgreSQL."""

    def normalize(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        logger.info(
            "Старт нормализации advert_stat | rows_before=%s | columns_before=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )
        normalized_df = dataframe.copy()

        extra_columns = sorted(set(normalized_df.columns) - set(DB_COLUMNS))
        if extra_columns:
            logger.warning(
                "Лишние колонки в DataFrame advert_stat будут проигнорированы | extra_columns=%s",
                extra_columns,
            )

        missing_columns = [column for column in DB_COLUMNS if column not in normalized_df.columns]
        for column in missing_columns:
            normalized_df[column] = pd.NA

        if missing_columns:
            logger.info(
                "В DataFrame advert_stat добавлены отсутствующие колонки | missing_columns=%s",
                missing_columns,
            )

        invalid_masks: dict[str, pd.Series] = {}

        for column in INT_COLUMNS:
            source_series = self._sanitize_special_strings(normalized_df[column])
            numeric_series = pd.to_numeric(source_series, errors="coerce")
            numeric_series = numeric_series.mask(np.isinf(numeric_series), pd.NA)

            whole_number_mask = numeric_series.notna() & ((numeric_series % 1) != 0)
            invalid_mask = source_series.notna() & numeric_series.isna()
            invalid_masks[column] = invalid_mask | whole_number_mask

            normalized_df[column] = numeric_series.mask(whole_number_mask, pd.NA).astype("Int64")

        for column in NUMERIC_2_COLUMNS:
            source_series = self._sanitize_special_strings(normalized_df[column])
            numeric_series = pd.to_numeric(source_series, errors="coerce")
            numeric_series = numeric_series.mask(np.isinf(numeric_series), pd.NA)
            invalid_masks[column] = source_series.notna() & numeric_series.isna()
            normalized_df[column] = numeric_series.round(2)

        for column in NUMERIC_4_COLUMNS:
            source_series = self._sanitize_special_strings(normalized_df[column])
            numeric_series = pd.to_numeric(source_series, errors="coerce")
            numeric_series = numeric_series.mask(np.isinf(numeric_series), pd.NA)
            invalid_masks[column] = source_series.notna() & numeric_series.isna()
            normalized_df[column] = numeric_series.round(4)

        for column in DATE_COLUMNS:
            normalized_df[column] = pd.to_datetime(
                self._sanitize_special_strings(normalized_df[column]),
                errors="coerce",
            ).dt.date

        for column in TEXT_COLUMNS:
            normalized_df[column] = normalized_df[column].map(
                lambda value: None if pd.isna(value) else str(value)
            )

        self._log_invalid_samples(normalized_df, invalid_masks)

        normalized_df = normalized_df.loc[:, list(DB_COLUMNS)]
        inf_columns = self._find_inf_columns(normalized_df)
        logger.info(
            "Нормализация advert_stat завершена | rows_after=%s | extra_columns=%s | missing_columns=%s | inf_columns=%s",
            len(normalized_df.index),
            extra_columns,
            missing_columns,
            inf_columns,
        )
        if inf_columns:
            raise ValueError(
                f"После нормализации в advert_stat остались infinite values: {inf_columns}"
            )

        normalized_df = normalized_df.astype(object)
        normalized_df = normalized_df.where(pd.notnull(normalized_df), None)
        return normalized_df

    def _sanitize_special_strings(self, series: pd.Series) -> pd.Series:
        string_series = series.astype("string").str.strip().str.lower()
        return series.mask(string_series.isin(SPECIAL_TEXT_NULLS), pd.NA)

    def _find_inf_columns(self, dataframe: pd.DataFrame) -> list[str]:
        inf_columns: list[str] = []
        for column in INT_COLUMNS + list(NUMERIC_COLUMNS):
            numeric_series = pd.to_numeric(dataframe[column], errors="coerce")
            if np.isinf(numeric_series).any():
                inf_columns.append(column)
        return inf_columns

    def _log_invalid_samples(
        self,
        dataframe: pd.DataFrame,
        invalid_masks: dict[str, pd.Series],
        sample_size: int = 5,
    ) -> None:
        invalid_columns = [column for column, mask in invalid_masks.items() if mask.any()]
        if not invalid_columns:
            return

        combined_mask = pd.Series(False, index=dataframe.index)
        for mask in invalid_masks.values():
            combined_mask = combined_mask | mask

        sample_columns = [
            column
            for column in ("campaign_id", "article_id", "date", "account")
            if column in dataframe.columns
        ]
        sample_columns.extend(
            column for column in invalid_columns if column not in sample_columns
        )
        sample_rows = dataframe.loc[combined_mask, sample_columns].head(sample_size)
        logger.warning(
            "Найдены некорректные значения до записи advert_stat | invalid_columns=%s | sample_rows=%s",
            invalid_columns,
            sample_rows.to_dict(orient="records"),
        )
