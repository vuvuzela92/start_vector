"""Чтение витрины отгрузок из PostgreSQL."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.fin_reports_analyze.system_penalties_analytics import FbsDatabase
from src_oop.jobs.fbs2_test.config import (
    SHIPMENTS_COLUMNS,
    SHIPMENTS_DATABASE_QUERY,
    STOCKS_COLUMNS,
    STOCKS_DATABASE_QUERY,
    WMS_REMAINS_WILDS_QUERY,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ShipmentsRepository:
    """Получает ежедневную витрину движений товаров для отчёта закупщиков."""

    database_cls: type[Database] = Database
    fbs_database_cls: type[FbsDatabase] = FbsDatabase

    def fetch_shipments(self) -> pd.DataFrame:
        """Читает отгрузки, перемещения, возвраты и остатки для выгрузки в карту БД.

        Метод обслуживает отчёт закупщиков: по каждому товару и дате он возвращает
        строки, в том числе с нулевыми движениями, чтобы лист был полной основой
        для последующих формул и анализа динамики.
        """

        logger.info("Запрашиваем в PostgreSQL ежедневную витрину отгрузок.")
        dataframe = self.database_cls.read_sql_to_dataframe(SHIPMENTS_DATABASE_QUERY)
        logger.info(
            "Получена ежедневная витрина отгрузок: строк=%s.",
            len(dataframe.index),
        )

        if dataframe.empty:
            logger.warning("Запрос витрины отгрузок вернул пустой результат.")
            return pd.DataFrame(columns=SHIPMENTS_COLUMNS)

        return dataframe

    def fetch_stocks(self) -> pd.DataFrame:
        """Читает ежедневные остатки по товарам для вкладки «БД Остатки».

        Метод обслуживает отдельную витрину закупщиков: отбирает только заданные
        товары и даты после 31 августа 2026 года, чтобы в лист не попадали
        остатки по постороннему ассортименту или устаревшие снимки.
        """

        logger.info("Запрашиваем в PostgreSQL ежедневную витрину остатков.")
        dataframe = self.database_cls.read_sql_to_dataframe(STOCKS_DATABASE_QUERY)
        logger.info(
            "Получена ежедневная витрина остатков: строк=%s.",
            len(dataframe.index),
        )

        if dataframe.empty:
            logger.warning("Запрос витрины остатков вернул пустой результат.")
            return pd.DataFrame(columns=STOCKS_COLUMNS)

        return dataframe

    def fetch_wms_remains_wilds(self) -> list[str]:
        """Возвращает отсортированный список `wild` для поля `fbs2`.

        Бизнес-логика:
        список формируется из `card_data`, а не из текущего листа Google Sheets,
        поэтому в `WMS остатки` попадают только корректные значения вида
        `wild123`. Сортировка по числовой части сохраняет порядок исходного
        SQL-сценария и делает выгрузку воспроизводимой.
        """

        logger.info("Запрашиваем список wild для обновления поля fbs2.")
        dataframe = self.fbs_database_cls.read_sql_to_dataframe(
            WMS_REMAINS_WILDS_QUERY,
            params={},
        )
        if dataframe.empty:
            logger.warning("Запрос списка wild для поля fbs2 вернул пустой результат.")
            return []

        wilds = (
            dataframe["wild"]
            .fillna("")
            .astype(str)
            .str.strip()
            .loc[lambda values: values.ne("")]
            .tolist()
        )
        logger.info("Получен список wild для поля fbs2: rows=%s.", len(wilds))
        return wilds
