"""Конфигурация job выгрузки баланса продавцов WB в Google Sheets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

BALANCE_URL = "https://finance-api.wildberries.ru/api/v1/account/balance"

REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 4
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 65
MAX_CONCURRENT_ACCOUNTS = 3

GOOGLE_WRITE_RETRY_ATTEMPTS = 4
GOOGLE_WRITE_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

CREDS_FILE = Path(__file__).resolve().parents[3] / "creds" / "creds.json"


@dataclass(frozen=True, slots=True)
class SheetConfig:
    """Описывает лист Google Sheets для публикации баланса продавцов.

    Бизнес-сценарий:
    финансовая команда читает витрину в таблице ДДС, поэтому job хранит
    стабильный `spreadsheet_id` и точные стартовые колонки выгрузки, чтобы не
    зависеть от неуникальных названий таблиц и не затирать соседние блоки.
    """

    spreadsheet_id: str
    sheet_title: str
    table_title: str
    start_column_index: int
    financial_reports_start_column_index: int = 12


SHEET_CONFIG = SheetConfig(
    spreadsheet_id="1icYbaiYBNT7t4XodKzC2gxEb0C87yRwglUGXz15TH_8",
    sheet_title="Переменные",
    table_title="ДДС",
    start_column_index=6,
    financial_reports_start_column_index=12,
)

# Начальная дата финансового среза задана бизнес-сценарием отчёта.
FINANCIAL_REPORTS_START_DATE = date(2026, 9, 14)
FINANCIAL_REPORTS_COLUMN_COUNT = 3
FINANCIAL_REPORTS_COLUMNS: tuple[str, ...] = ("date", "account", "total_to_pay")

COLUMN_RENAME_MAP: dict[str, str] = {
    "account": "Аккаунт",
    "currency": "Валюта",
    "current": "Текущий баланс",
    "for_withdraw": "Доступно к выводу",
    "updated_at_export": "Дата обновления выгрузки",
}

PRIORITY_COLUMNS: tuple[str, ...] = (
    "Аккаунт",
    "Валюта",
    "Текущий баланс",
    "Доступно к выводу",
    "Дата обновления выгрузки",
)

FINANCIAL_REPORTS_COLUMN_RENAME_MAP: dict[str, str] = {
    "date": "Дата",
    "account": "Аккаунт",
    "total_to_pay": "Сумма к перечислению",
}
