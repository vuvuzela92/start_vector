from __future__ import annotations

import os
from datetime import timedelta

from sqlalchemy import BigInteger, DateTime, Numeric, String

DOCUMENT_LIST_URL = "https://documents-api.wildberries.ru/api/v1/documents/list"
DOCUMENT_DOWNLOAD_URL = "https://documents-api.wildberries.ru/api/v1/documents/download/all"

DOCUMENT_CATEGORIES: tuple[str, str] = (
    "weekly-implementation-report",
    "redeem-notification",
)

DEFAULT_DAYS_BACK = 28
DEFAULT_DATE_TO_OFFSET = timedelta(days=1)
DOCUMENT_LIST_DATE_TO_LAG = timedelta(
    days=int(os.getenv("WB_DOCS_LIST_DATE_TO_LAG_DAYS", "7"))
)
DOCUMENTS_BATCH_LIMIT = 50
REQUEST_TIMEOUT_SECONDS = 60
DOWNLOAD_TIMEOUT_SECONDS = 30
RETRY_ATTEMPTS = 5
RETRY_BASE_DELAY_SECONDS = 10
MAX_CONCURRENCY = 3
FILTER_WEEKLY_BY_REPORT_DATE = (
    os.getenv("WB_DOCS_FILTER_WEEKLY_BY_REPORT_DATE", "False").strip().lower()
    in {"1", "true", "yes", "on"}
)

WEEKLY_REPORT_TABLE_NAME = "weekly_implementation_report"
REDEEM_NOTIFICATION_TABLE_NAME = "redeem_notification"
WEEK_N_REDEEM_TABLE_TITLE = "ОТЧЕТ за 2026 пров v.2.0"
WEEK_N_REDEEM_SHEET_TITLE = "БД"

WEEKLY_REPORT_UNIQUE_KEYS: tuple[str, ...] = ("doc_num", "№", "account")
REDEEM_NOTIFICATION_UNIQUE_KEYS: tuple[str, ...] = ("doc_name", "№", "account")

WEEKLY_REPORT_SCHEMA = {
    "№": String(10),
    "title": String(255),
    "supporting_document": String(255),
    "date": DateTime,
    "doc_num": Numeric(12, 2),
    "sum_rub": Numeric(12, 2),
    "vat_rub": Numeric(12, 2),
    "account": String(255),
}

REDEEM_NOTIFICATION_SCHEMA = {
    "№": String(255),
    "wild": String(255),
    "subject_name": String(255),
    "quantity": BigInteger,
    "sum_rub_with_vat": Numeric(12, 2),
    "vat_rate": String(10),
    "vat_sum_rub": Numeric(12, 2),
    "kiz": String(255),
    "doc_name": String(255),
    "account": String(255),
}
