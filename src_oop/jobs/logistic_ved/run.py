from __future__ import annotations

import logging

from src_oop.jobs.logistic_ved.config import shipments_fbo
from src_oop.jobs.logistic_ved.google_sheets_client import (
    LogisticVedGoogleSheetsClient,
)
from src_oop.jobs.logistic_ved.repository import LogisticVedRepository
from src_oop.jobs.logistic_ved.service import LogisticVedService

logger = logging.getLogger(__name__)


def logistic_ved_run() -> None:
    """Точка входа для ручного запуска выгрузки logistic_ved."""

    logger.info("Инициализация компонентов задачи logistic_ved.")
    service = LogisticVedService(
        repository=LogisticVedRepository(),
        sheets_client=LogisticVedGoogleSheetsClient(
            table_title=shipments_fbo["title"],
            sheet_title=shipments_fbo["white_orders_sheet"],
        ),
    )
    service.run()
