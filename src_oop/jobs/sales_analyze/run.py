from __future__ import annotations

import logging

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.sales_analyze.config import SALES_ANALYZE_SHEET_CONFIG
from src_oop.jobs.sales_analyze.repository import SalesAnalyzeRepository

logger = logging.getLogger(__name__)


def update_sales_warehouse_analytics() -> None:
    """
    Запускает полный сценарий выгрузки аналитики продаж по нашим складам.

    Бизнес-сценарий:
    функция читает агрегированные продажи seller-складов из PostgreSQL и
    полностью обновляет вкладку `Аналитика складов` в таблице `Новый товар`,
    чтобы команда работала с актуальной дневной выручкой по складам, товарам и
    аккаунтам без ручной сборки отчёта.
    """

    logger.info(
        "Старт выгрузки аналитики продаж по складам | table=%s | sheet=%s",
        SALES_ANALYZE_SHEET_CONFIG.table_title,
        SALES_ANALYZE_SHEET_CONFIG.sheet_title,
    )
    repository = SalesAnalyzeRepository()
    dataframe = repository.get_sales_warehouse_analytics()

    google_tabs = GoogleTabs(
        table_title=SALES_ANALYZE_SHEET_CONFIG.table_title,
        sheet_title=SALES_ANALYZE_SHEET_CONFIG.sheet_title,
    )
    google_tabs.set_df_to_google(dataframe)
    logger.info(
        "Выгрузка аналитики продаж по складам завершена | rows=%s | table=%s | sheet=%s",
        len(dataframe.index),
        SALES_ANALYZE_SHEET_CONFIG.table_title,
        SALES_ANALYZE_SHEET_CONFIG.sheet_title,
    )
