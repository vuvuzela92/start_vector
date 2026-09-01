"""Точка входа задачи выгрузки ежедневных отгрузок."""

from __future__ import annotations

from src_oop.jobs.fbs2_test.config import (
    PURCHASERS_DATABASE_SPREADSHEET_ID,
    PURCHASERS_DATABASE_TABLE_TITLE,
    SHIPMENTS_SHEET_TITLE,
    STOCKS_SHEET_TITLE,
)
from src_oop.jobs.fbs2_test.google_sheets_client import (
    PurchasersDatabaseGoogleSheetsClient,
)
from src_oop.jobs.fbs2_test.repository import ShipmentsRepository
from src_oop.jobs.fbs2_test.service import ShipmentsService, StocksService


def fbs2_test_movements_run() -> None:
    """Обновляет вкладку «БД Отгрузки» карты БД закупщиков актуальной витриной.

    Точка входа создаёт компоненты сценария и запускает полный путь от SQL-запроса
    PostgreSQL до полной публикации результата в целевой Google Sheets.
    """

    service = ShipmentsService(
        repository=ShipmentsRepository(),
        sheets_client=PurchasersDatabaseGoogleSheetsClient(
            spreadsheet_id=PURCHASERS_DATABASE_SPREADSHEET_ID,
            table_title=PURCHASERS_DATABASE_TABLE_TITLE,
            sheet_title=SHIPMENTS_SHEET_TITLE,
        ),
    )
    service.run()


def fbs2_test_stocks_run() -> None:
    """Обновляет вкладку «БД Остатки» карты БД закупщиков актуальными остатками.

    Точка входа создаёт компоненты отдельной витрины остатков и запускает путь
    от SQL-запроса PostgreSQL до полной публикации результата в Google Sheets.
    """

    service = StocksService(
        repository=ShipmentsRepository(),
        sheets_client=PurchasersDatabaseGoogleSheetsClient(
            spreadsheet_id=PURCHASERS_DATABASE_SPREADSHEET_ID,
            table_title=PURCHASERS_DATABASE_TABLE_TITLE,
            sheet_title=STOCKS_SHEET_TITLE,
        ),
    )
    service.run()
