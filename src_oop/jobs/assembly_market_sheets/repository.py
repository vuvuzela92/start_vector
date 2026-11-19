"""Чтение статусной витрины сборочных заданий из PostgreSQL."""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text

from src_oop.core.database import Database

logger = logging.getLogger(__name__)


class AssemblyMarketSheetsRepository:
    """Предоставляет явные поля статусов для публикации в Google Sheets."""

    def fetch_statuses(self) -> pd.DataFrame:
        """Читает ID задания и оба статуса без загрузки лишних колонок."""
        query = text(
            """SELECT assembly_task_id, wb_status, supplier_status
               FROM status_assembly_task"""
        )
        try:
            with Database.get_engine().connect() as connection:
                dataframe = pd.read_sql(query, connection)
        except Exception as error:
            logger.error(
                "Не удалось прочитать статусы сборочных заданий из PostgreSQL | error_type=%s",
                type(error).__name__,
            )
            raise
        logger.info("Статусы сборочных заданий прочитаны из PostgreSQL | rows=%s", len(dataframe))
        return dataframe
