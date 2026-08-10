from __future__ import annotations

import logging

import pandas as pd

from src_oop.jobs.orders.config import (
    BOOLEAN_COLUMNS,
    COLUMN_RENAME_MAP,
    DATE_COLUMNS,
    DATETIME_COLUMNS,
    DB_COLUMNS,
    INTEGER_COLUMNS,
    NUMERIC_COLUMNS,
    SOURCE_COLUMNS,
)

logger = logging.getLogger(__name__)


class OrdersNormalizer:
    """Нормализует сырой ответ WB в структуру таблицы orders."""

    def normalize(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Готовит заказы WB к upsert в PostgreSQL.

        Бизнес-правила: `date_from` хранит
        исходную дату-время заказа, а `date` приводится к календарной дате для
        уникального ключа `(date, srid)`.
        """
        if dataframe.empty:
            return pd.DataFrame(columns=list(DB_COLUMNS))

        prepared_df = dataframe.copy()
        self._ensure_source_columns(prepared_df)

        prepared_df = prepared_df.loc[:, list(SOURCE_COLUMNS)].rename(
            columns=COLUMN_RENAME_MAP
        )
        prepared_df["date_from"] = pd.to_datetime(prepared_df["date"], errors="coerce")
        prepared_df["date"] = prepared_df["date_from"].dt.date

        self._coerce_typed_columns(prepared_df)
        prepared_df = prepared_df.loc[:, list(DB_COLUMNS)]
        prepared_df = self._replace_missing_values_with_none(prepared_df)

        logger.info(
            "Нормализованы заказы WB для записи в PostgreSQL | rows=%s | columns=%s",
            len(prepared_df.index),
            list(prepared_df.columns),
        )
        return prepared_df

    def _ensure_source_columns(self, dataframe: pd.DataFrame) -> None:
        """Добавляет отсутствующие необязательные поля WB, чтобы загрузка не падала на пустых колонках."""
        for column in SOURCE_COLUMNS:
            if column not in dataframe.columns:
                dataframe[column] = None

    def _coerce_typed_columns(self, dataframe: pd.DataFrame) -> None:
        """Приводит типы колонок orders к формату, ожидаемому схемой PostgreSQL."""
        for column in DATETIME_COLUMNS:
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")

        for column in DATE_COLUMNS:
            date_series = pd.to_datetime(dataframe[column], errors="coerce")
            dataframe[column] = date_series.dt.date

        for column in INTEGER_COLUMNS:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").astype(
                "Int64"
            )

        for column in NUMERIC_COLUMNS:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").round(2)

        for column in BOOLEAN_COLUMNS:
            dataframe[column] = dataframe[column].astype("boolean")

    def _replace_missing_values_with_none(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Заменяет pandas-пропуски на None, чтобы PostgreSQL получил NULL вместо NaT/NA.

        Бизнес-правило: пустые даты отмены и другие незаполненные поля WB не
        должны прерывать загрузку заказов, потому что для действующих заказов
        `cancel_date` штатно отсутствует.
        """
        return dataframe.astype(object).where(pd.notna(dataframe), None)
