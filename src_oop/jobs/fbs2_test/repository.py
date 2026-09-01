"""Чтение витрины отгрузок из PostgreSQL."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.fbs2_test.config import (
    SHIPMENTS_COLUMNS,
    SHIPMENTS_DATABASE_QUERY,
    STOCKS_COLUMNS,
    STOCKS_DATABASE_QUERY,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ShipmentsRepository:
    """Получает ежедневную витрину движений товаров для отчёта закупщиков."""

    database_cls: type[Database] = Database

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
