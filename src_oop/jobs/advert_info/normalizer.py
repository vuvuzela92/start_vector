from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from src_oop.jobs.advert_info.config import (
    BOOLEAN_COLUMNS,
    DATETIME_COLUMNS,
    DB_COLUMNS,
    INT_COLUMNS,
    TEXT_COLUMNS,
)

logger = logging.getLogger(__name__)

SPECIAL_TEXT_NULLS = {"", "nan", "nat", "none", "null", "inf", "-inf", "+inf"}


class AdvertInfoNormalizer:
    """Нормализует данные рекламных кампаний WB перед записью в PostgreSQL."""

    def normalize(self, account_payloads: list[dict]) -> pd.DataFrame:
        rows = self._flatten_campaigns(account_payloads)
        if not rows:
            logger.info("Нет строк рекламных кампаний WB для нормализации.")
            return pd.DataFrame(columns=list(DB_COLUMNS))

        dataframe = pd.DataFrame.from_records(rows)
        logger.info(
            "Старт нормализации advert_info | rows_before=%s | columns_before=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )

        missing_columns = [column for column in DB_COLUMNS if column not in dataframe.columns]
        for column in missing_columns:
            dataframe[column] = pd.NA

        if missing_columns:
            logger.info(
                "В DataFrame advert_info добавлены отсутствующие колонки | missing_columns=%s",
                missing_columns,
            )

        normalized_df = dataframe.copy()
        for column in INT_COLUMNS:
            source_series = self._sanitize_special_strings(normalized_df[column])
            numeric_series = pd.to_numeric(source_series, errors="coerce")
            numeric_series = numeric_series.mask(np.isinf(numeric_series), pd.NA)
            whole_number_mask = numeric_series.notna() & ((numeric_series % 1) != 0)
            normalized_df[column] = numeric_series.mask(whole_number_mask, pd.NA).astype("Int64")

        for column in BOOLEAN_COLUMNS:
            normalized_df[column] = normalized_df[column].map(
                lambda value: None if pd.isna(value) else bool(value)
            )

        for column in DATETIME_COLUMNS:
            normalized_df[column] = pd.to_datetime(
                self._sanitize_special_strings(normalized_df[column]),
                format="ISO8601",
                errors="coerce",
            )

        for column in TEXT_COLUMNS:
            normalized_df[column] = normalized_df[column].map(
                lambda value: None if pd.isna(value) else str(value)
            )

        normalized_df = normalized_df.loc[:, list(DB_COLUMNS)].astype(object)
        normalized_df = normalized_df.where(pd.notnull(normalized_df), None)
        logger.info(
            "Нормализация advert_info завершена | rows_after=%s | columns=%s",
            len(normalized_df.index),
            list(normalized_df.columns),
        )
        return normalized_df

    def _flatten_campaigns(self, account_payloads: list[dict]) -> list[dict]:
        campaign_rows: list[dict] = []
        for account_data in account_payloads:
            if not account_data or "adverts" not in account_data:
                continue

            account_name = account_data.get("account", "Unknown")
            for advert in account_data.get("adverts", []) or []:
                if not isinstance(advert, dict):
                    continue

                nm_settings = advert.get("nm_settings", [{}])[0] if advert.get("nm_settings") else {}
                bids = nm_settings.get("bids_kopecks", {}) if isinstance(nm_settings, dict) else {}
                settings = advert.get("settings", {}) or {}
                placements = settings.get("placements", {}) or {}

                campaign_rows.append(
                    {
                        "account": account_name,
                        "campaign_id": advert.get("id"),
                        "campaign_name": settings.get("name"),
                        "bid_type": advert.get("bid_type"),
                        "nm_id": nm_settings.get("nm_id") if isinstance(nm_settings, dict) else None,
                        "search_bid": bids.get("search"),
                        "recommendations_bid": bids.get("recommendations"),
                        "payment_type": settings.get("payment_type"),
                        "recommendations": placements.get("recommendations"),
                        "search": placements.get("search"),
                        "created_at_campaign": advert.get("timestamps", {}).get("created"),
                    }
                )

        return campaign_rows

    def _sanitize_special_strings(self, series: pd.Series) -> pd.Series:
        string_series = series.astype("string").str.strip().str.lower()
        return series.mask(string_series.isin(SPECIAL_TEXT_NULLS), pd.NA)
