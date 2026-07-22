from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from src_oop.jobs.fbo_supplies.config import (
    FBO_SUPPLIES_COLUMN_MAPPING,
    FBO_SUPPLIES_COLUMNS,
)
from src_oop.jobs.fbo_supplies.google_sheets_client import (
    FboSuppliesGoogleSheetsClient,
)
from src_oop.jobs.fbo_supplies.repository import FboSuppliesRepository

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FboSuppliesService:
    """Оркестрирует получение и выгрузку данных по ФБО-отгрузкам."""

    repository: FboSuppliesRepository
    sheets_client: FboSuppliesGoogleSheetsClient

    def run(self) -> None:
        logger.info("Старт задачи по выгрузке ФБО-отгрузок.")
        dataframe = self.repository.fetch_orders_by_region()
        prepared_dataframe = self._prepare_for_google_sheets(dataframe)
        self.sheets_client.upload_dataframe(prepared_dataframe)
        logger.info(
            "Задача по выгрузке ФБО-отгрузок завершена успешно: rows=%s",
            len(prepared_dataframe.index),
        )

    def _prepare_for_google_sheets(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        logger.info(
            "Подготавливаем DataFrame по ФБО-отгрузкам к выгрузке: rows=%s columns=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )

        renamed_dataframe = dataframe.rename(columns=FBO_SUPPLIES_COLUMN_MAPPING)
        ordered_dataframe = renamed_dataframe.reindex(columns=FBO_SUPPLIES_COLUMNS)

        if ordered_dataframe.empty:
            logger.warning(
                "После подготовки DataFrame по ФБО-отгрузкам пуст. В Google Sheets будут записаны только заголовки."
            )
            return ordered_dataframe

        safe_dataframe = ordered_dataframe.copy().astype(object)
        safe_dataframe = safe_dataframe.map(self._normalize_cell_value)
        return safe_dataframe.fillna("")

    @staticmethod
    def _normalize_cell_value(value: object) -> object:
        # Подготавливаем значения к безопасной записи в Google Sheets:
        # убираем пустые pandas-значения и приводим даты/Decimal к предсказуемому виду.
        if value is None or pd.isna(value):
            return ""

        if isinstance(value, pd.Timestamp):
            return value.isoformat()

        if isinstance(value, datetime):
            return value.isoformat(sep=" ")

        if isinstance(value, date):
            return value.isoformat()

        if isinstance(value, Decimal):
            normalized = value.normalize()
            return int(normalized) if normalized == normalized.to_integral() else float(value)

        if isinstance(value, float):
            return "" if pd.isna(value) else value

        return value
