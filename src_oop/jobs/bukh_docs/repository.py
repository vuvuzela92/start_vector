from __future__ import annotations

import logging

import pandas as pd

from src_oop.core.database import Database
from src_oop.jobs.bukh_docs.config import (
    REDEEM_NOTIFICATION_SCHEMA,
    REDEEM_NOTIFICATION_TABLE_NAME,
    REDEEM_NOTIFICATION_UNIQUE_KEYS,
    WEEKLY_REPORT_SCHEMA,
    WEEKLY_REPORT_TABLE_NAME,
    WEEKLY_REPORT_UNIQUE_KEYS,
)
from src_oop.jobs.bukh_docs.models import SaveResult

logger = logging.getLogger(__name__)


class BukhDocsRepository:
    """Слой записи бухгалтерских документов WB в PostgreSQL."""

    def __init__(self, database_cls: type[Database] = Database) -> None:
        self._database_cls = database_cls

    def save_weekly_reports(self, dataframe: pd.DataFrame) -> SaveResult:
        return self._save_dataframe(
            table_name=WEEKLY_REPORT_TABLE_NAME,
            dataframe=dataframe,
            schema_definition=WEEKLY_REPORT_SCHEMA,
            unique_keys=WEEKLY_REPORT_UNIQUE_KEYS,
        )

    def save_redeem_notifications(self, dataframe: pd.DataFrame) -> SaveResult:
        return self._save_dataframe(
            table_name=REDEEM_NOTIFICATION_TABLE_NAME,
            dataframe=dataframe,
            schema_definition=REDEEM_NOTIFICATION_SCHEMA,
            unique_keys=REDEEM_NOTIFICATION_UNIQUE_KEYS,
        )

    def _save_dataframe(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        schema_definition: dict[str, object],
        unique_keys: tuple[str, ...],
    ) -> SaveResult:
        if dataframe.empty:
            logger.info("Пропуск записи в БД: table=%s пустой DataFrame", table_name)
            return SaveResult(
                table_name=table_name,
                input_rows=0,
                written_rows=0,
                status="success",
                warnings=["Пустой набор данных: запись в БД пропущена."],
            )

        try:
            logger.info(
                "Подготовка записи в БД: table=%s rows=%s columns=%s unique_keys=%s",
                table_name,
                len(dataframe.index),
                list(dataframe.columns),
                list(unique_keys),
            )
            self._database_cls.sync_data_to_postgres(
                table_name=table_name,
                data=dataframe,
                schema_definition=schema_definition,
                unique_keys=unique_keys,
            )
            logger.info(
                "Сохранение в БД завершено: table=%s rows=%s",
                table_name,
                len(dataframe.index),
            )
            return SaveResult(
                table_name=table_name,
                input_rows=len(dataframe.index),
                written_rows=len(dataframe.index),
                status="success",
            )
        except Exception as error:
            logger.exception("Ошибка сохранения в БД: table=%s error=%s", table_name, error)
            return SaveResult(
                table_name=table_name,
                input_rows=len(dataframe.index),
                written_rows=0,
                status="failed",
                errors=[str(error)],
            )
