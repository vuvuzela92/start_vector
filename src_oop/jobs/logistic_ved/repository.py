from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.logistic_ved.config import LOGISTIC_VED_ORDERS_BY_REGION_QUERY

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LogisticVedRepository:
    """Слой чтения агрегированных данных по заказам из PostgreSQL."""

    database_cls: type[Database] = Database

    def fetch_orders_by_region(self) -> pd.DataFrame:
        logger.info("Выполняется SQL-запрос logistic_ved по заказам за последние 7 дней.")
        dataframe = self.database_cls.read_sql_to_dataframe(
            LOGISTIC_VED_ORDERS_BY_REGION_QUERY,
        )
        logger.info("SQL logistic_ved завершен: rows=%s", len(dataframe.index))

        if dataframe.empty:
            logger.warning("SQL logistic_ved вернул пустой результат.")
            return pd.DataFrame(
                columns=[
                    "local_vendor_code",
                    "nm_id",
                    "oblast_okrug_name",
                    "orders_cnt",
                ]
            )

        return dataframe
