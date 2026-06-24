from __future__ import annotations

import logging

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.bukh_docs.week_n_redeem_queries import WEEK_N_REDEEM_QUERY

logger = logging.getLogger(__name__)


class WeekNRedeemRepository:
    """Слой чтения данных week_n_redeem из PostgreSQL."""

    def __init__(self, database_cls: type[Database] = Database) -> None:
        self._database_cls = database_cls

    def fetch_dataframe(self) -> pd.DataFrame:
        logger.info("Выполняется SQL week_n_redeem.")
        dataframe = self._database_cls.read_sql_to_dataframe(WEEK_N_REDEEM_QUERY)
        logger.info(
            "Получен DataFrame week_n_redeem: rows=%s columns=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )
        return dataframe
