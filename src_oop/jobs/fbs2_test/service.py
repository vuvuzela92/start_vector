"""Подготовка витрины отгрузок к публикации."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from src_oop.jobs.fbs2_test.config import SHIPMENTS_COLUMNS, STOCKS_COLUMNS
from src_oop.jobs.fbs2_test.google_sheets_client import (
    PurchasersDatabaseGoogleSheetsClient,
)
from src_oop.jobs.fbs2_test.repository import ShipmentsRepository

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ShipmentsService:
    """Оркестрирует формирование и публикацию ежедневной витрины отгрузок."""

    repository: ShipmentsRepository
    sheets_client: PurchasersDatabaseGoogleSheetsClient

    def run(self) -> None:
        """Запускает полный сценарий обновления вкладки «БД Отгрузки».

        Сценарий получает из PostgreSQL движения и остатки по закреплённому
        списку товаров, приводит значения к формату Google Sheets и полностью
        обновляет вкладку карты БД закупщиков.
        """

        logger.info("Старт выгрузки ежедневной витрины отгрузок.")
        dataframe = self.repository.fetch_shipments()
        prepared_dataframe = self._prepare_for_google_sheets(dataframe)
        self.sheets_client.upload_dataframe(prepared_dataframe)
        logger.info(
            "Выгрузка ежедневной витрины отгрузок завершена: строк=%s.",
            len(prepared_dataframe.index),
        )

    def _prepare_for_google_sheets(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Нормализует типы витрины перед отправкой в Google Sheets.

        Метод защищает выгрузку от неподдерживаемых значений pandas и Decimal,
        чтобы наличие пропусков либо нулевых движений в БД не прерывало
        обновление листа закупщиков.
        """

        prepared_dataframe = dataframe.reindex(columns=SHIPMENTS_COLUMNS).copy()
        if prepared_dataframe.empty:
            logger.warning(
                "Для записи в Google Sheets подготовлены только заголовки витрины отгрузок."
            )
            return prepared_dataframe

        safe_dataframe = prepared_dataframe.astype(object).map(
            self._normalize_cell_value,
        )
        return safe_dataframe.fillna("")

    @staticmethod
    def _normalize_cell_value(value: object) -> object:
        """Приводит значение ячейки к безопасному формату Google Sheets.

        Вспомогательный метод защищает основной сценарий выгрузки от дат,
        Decimal и пропусков, которые иначе могут быть записаны некорректно или
        привести к ошибке сериализации запроса к Google Sheets.
        """

        if value is None or pd.isna(value):
            return ""
        if isinstance(value, pd.Timestamp):
            return value.date().isoformat()
        if isinstance(value, datetime):
            return value.isoformat(sep=" ")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            normalized = value.normalize()
            return int(normalized) if normalized == normalized.to_integral() else float(value)
        return value


@dataclass(slots=True)
class StocksService:
    """Оркестрирует формирование и публикацию ежедневной витрины остатков."""

    repository: ShipmentsRepository
    sheets_client: PurchasersDatabaseGoogleSheetsClient

    def run(self) -> None:
        """Запускает полное обновление вкладки «БД Остатки».

        Сценарий получает остатки заданных товаров из PostgreSQL, приводит
        значения к формату Google Sheets и полностью заменяет данные целевой
        вкладки, чтобы закупщики работали с актуальными снимками остатков.
        """

        logger.info("Старт выгрузки ежедневной витрины остатков.")
        dataframe = self.repository.fetch_stocks()
        prepared_dataframe = self._prepare_for_google_sheets(dataframe)
        self.sheets_client.upload_dataframe(prepared_dataframe)
        logger.info(
            "Выгрузка ежедневной витрины остатков завершена: строк=%s.",
            len(prepared_dataframe.index),
        )

    def _prepare_for_google_sheets(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Нормализует остатки перед публикацией в Google Sheets.

        Метод сохраняет заданный порядок колонок и не допускает передачи в
        Google Sheets пропусков, дат и Decimal в неподдерживаемом виде, чтобы
        обновление листа не прерывалось из-за особенностей типов PostgreSQL.
        """

        prepared_dataframe = dataframe.reindex(columns=STOCKS_COLUMNS).copy()
        if prepared_dataframe.empty:
            logger.warning(
                "Для записи в Google Sheets подготовлены только заголовки витрины остатков."
            )
            return prepared_dataframe

        safe_dataframe = prepared_dataframe.astype(object).map(
            ShipmentsService._normalize_cell_value,
        )
        return safe_dataframe.fillna("")
