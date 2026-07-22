from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.fbo_supplies.config import FBO_SUPPLIES_ORDERS_BY_REGION_QUERY

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FboSuppliesRepository:
    """Слой чтения агрегированных заказов ФБО из PostgreSQL."""

    database_cls: type[Database] = Database

    def fetch_orders_by_region(self) -> pd.DataFrame:
        logger.info(
            "Запрашиваем из PostgreSQL агрегированные ФБО-заказы по регионам за последние 7 дней."
        )
        dataframe = self.database_cls.read_sql_to_dataframe(
            FBO_SUPPLIES_ORDERS_BY_REGION_QUERY,
        )
        logger.info("SQL-запрос по ФБО-отгрузкам выполнен: rows=%s", len(dataframe.index))

        if dataframe.empty:
            logger.warning("SQL-запрос по ФБО-отгрузкам вернул пустой результат.")
            return pd.DataFrame(
                columns=[
                    "local_vendor_code",
                    "nm_id",
                    "oblast_okrug_name",
                    "orders_cnt",
                ]
            )

        return dataframe
