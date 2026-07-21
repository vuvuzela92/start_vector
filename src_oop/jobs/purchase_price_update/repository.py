from __future__ import annotations

import logging

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.purchase_price_update.config import PURCHASE_PRICE_UPDATE_QUERY

logger = logging.getLogger(__name__)


class PurchasePriceUpdateRepository:
    """
    Слой чтения исходных закупочных цен из PostgreSQL.

    Назначение:
    изолирует доступ к базе данных от бизнес-логики подготовки изменений.
    Благодаря этому обработку данных можно тестировать отдельно от БД.
    """

    def __init__(self, database_cls: type[Database] = Database) -> None:
        self._database_cls = database_cls

    def fetch_latest_purchase_prices(self, days_count: int) -> pd.DataFrame:
        """
        Возвращает последние закупочные цены за указанное число дней.

        Назначение:
        выполняет SQL-запрос, который повторяет логику старого скрипта,
        и возвращает сырые данные для дальнейшей обработки.

        Параметры:
        `days_count` — сколько прошлых дней нужно взять в окно выборки.

        Возвращаемый результат:
        `pandas.DataFrame` с колонками, которые нужны processor для расчета
        новых закупочных цен и построения отчета.

        Возможные исключения:
        может пробросить исключения драйвера БД или SQLAlchemy, если база
        недоступна или запрос завершился с ошибкой.

        Особенности поведения:
        на этом уровне нет логики сопоставления с UNIT и нет фильтрации по
        признаку "неизменяемая цена" — это делается позже в processor.
        """

        logger.info(
            "Начинаем чтение закупочных цен из PostgreSQL: days_count=%s",
            days_count,
        )
        dataframe = self._database_cls.read_sql_to_dataframe(
            PURCHASE_PRICE_UPDATE_QUERY,
            params={"days_count": days_count},
        )
        logger.info(
            "Закупочные цены из PostgreSQL успешно загружены: rows=%s columns=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )
        return dataframe
