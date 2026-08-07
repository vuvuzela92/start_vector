from __future__ import annotations

from sqlalchemy import BigInteger, DateTime, Numeric, String

SPEND_URL = "https://advert-api.wildberries.ru/adv/v1/upd"

TABLE_NAME = "advert_spend"
KEY_COLUMNS: tuple[str, ...] = ("advert_id", "upd_num", "upd_time")
DATE_COLUMN = "date"

# WB отдает camelCase-поля, а таблица в PostgreSQL ожидает snake_case.
COLUMN_RENAME_MAPPING: dict[str, str] = {
    "updTime": "upd_time",
    "campName": "camp_name",
    "paymentType": "payment_type",
    "updNum": "upd_num",
    "updSum": "upd_sum",
    "advertId": "advert_id",
    "advertType": "advert_type",
    "advertStatus": "advert_status",
}

DB_COLUMNS: tuple[str, ...] = (
    "upd_time",
    "camp_name",
    "payment_type",
    "upd_num",
    "upd_sum",
    "advert_id",
    "advert_type",
    "advert_status",
    "date",
    "account",
    "currency",
)

INT_COLUMNS: tuple[str, ...] = (
    "upd_num",
    "advert_id",
    "advert_type",
    "advert_status",
)
NUMERIC_COLUMNS: tuple[str, ...] = ("upd_sum",)
DATETIME_COLUMNS: tuple[str, ...] = ("upd_time",)
DATE_ONLY_COLUMNS: tuple[str, ...] = ("date",)
TEXT_COLUMNS: tuple[str, ...] = ("camp_name", "payment_type", "account", "currency")

SCHEMA_DEFINITION = {
    "upd_time": DateTime(timezone=True),
    "camp_name": String(255),
    "payment_type": String(255),
    "upd_num": BigInteger,
    "upd_sum": Numeric,
    "advert_id": BigInteger,
    "advert_type": BigInteger,
    "advert_status": BigInteger,
    "date": DateTime,
    "account": String,
    "currency": String,
}

# WB может дообновлять расходы с задержкой, поэтому перед записью освежаем хвост периода.
DAYS_TO_CLEAN = 3

REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 10
RETRY_BASE_SLEEP_SECONDS = 1
RETRY_MAX_SLEEP_SECONDS = 60
MAX_CONCURRENT_ACCOUNTS = 4
