from __future__ import annotations

import logging

from src_oop.jobs.fbo_supplies.config import SHIPMENTS_FBO_TABLE
from src_oop.jobs.fbo_supplies.google_sheets_client import (
    FboSuppliesGoogleSheetsClient,
)
from src_oop.jobs.fbo_supplies.repository import FboSuppliesRepository
from src_oop.jobs.fbo_supplies.service import FboSuppliesService

logger = logging.getLogger(__name__)


def fbo_supplies_run() -> None:
    """Точка входа для ручного запуска выгрузки данных по ФБО-отгрузкам."""

    logger.info("Инициализируем компоненты задачи по ФБО-отгрузкам.")
    service = FboSuppliesService(
        repository=FboSuppliesRepository(),
        sheets_client=FboSuppliesGoogleSheetsClient(
            table_title=SHIPMENTS_FBO_TABLE["title"],
            sheet_title=SHIPMENTS_FBO_TABLE["orders_by_region_sheet"],
        ),
    )
    service.run()
