from __future__ import annotations

import logging

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.sales_analyze.config import SALES_ANALYZE_QUERY

logger = logging.getLogger(__name__)


class SalesAnalyzeRepository:
    """
    Читает из PostgreSQL агрегированную аналитику продаж по нашим складам.

    Бизнес-назначение:
    инкапсулирует доступ к SQL-выгрузке, чтобы запуск задачи не смешивал
    инфраструктурное чтение из БД с логикой публикации отчёта в Google Sheets.
    """

    def get_sales_warehouse_analytics(self) -> pd.DataFrame:
        """
        Возвращает набор данных для витрины аналитики складов в Google Sheets.

        Бизнес-сценарий:
        задача собирает выручку по товарам, складам, датам заказа и аккаунтам
        только для наших seller-складов, чтобы команда видела ежедневную
        динамику продаж во вкладке `Аналитика складов` таблицы `Новый товар`.
        """

        dataframe = Database.read_sql_to_dataframe(SALES_ANALYZE_QUERY)
        logger.info(
            "Из PostgreSQL прочитана аналитика продаж по складам | rows=%s | columns=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )
        return dataframe
