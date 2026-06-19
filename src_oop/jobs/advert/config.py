from __future__ import annotations

from sqlalchemy import BigInteger, Date, Numeric, String

FULLSTATS_URL = "https://advert-api.wildberries.ru/adv/v3/fullstats"
ADVERTS_URL = "https://advert-api.wildberries.ru/api/advert/v2/adverts"

CAMPAIGN_STATUSES: tuple[int, ...] = (7, 9, 11)
SUPPORTED_BID_TYPES: tuple[str, ...] = ("unified", "manual")

TABLE_NAME = "advert_stat"
KEY_COLUMNS: tuple[str, ...] = ("campaign_id", "date", "article_id")

INT_COLUMNS: list[str] = [
    "campaign_id",
    "article_id",
    "atbs",
    "canceled",
    "clicks",
    "views",
    "orders",
    "shks",
    "atbs_pc",
    "canceled_pc",
    "clicks_pc",
    "views_pc",
    "orders_pc",
    "shks_pc",
    "atbs_android",
    "canceled_android",
    "clicks_android",
    "views_android",
    "orders_android",
    "shks_android",
    "atbs_ios",
    "canceled_ios",
    "clicks_ios",
    "views_ios",
    "orders_ios",
    "shks_ios",
]

NUMERIC_2_COLUMNS: list[str] = [
    "sum",
    "sum_price",
    "sum_price_pc",
    "sum_price_android",
    "sum_price_ios",
]

NUMERIC_4_COLUMNS: list[str] = [
    "cpc",
    "cpm",
    "cr",
    "ctr",
    "avg_position",
    "cr_pc",
    "ctr_pc",
    "cr_android",
    "ctr_android",
    "cr_ios",
    "ctr_ios",
]

DATE_COLUMNS: list[str] = ["date"]
TEXT_COLUMNS: list[str] = ["account"]

DB_COLUMNS: tuple[str, ...] = tuple(
    INT_COLUMNS
    + NUMERIC_2_COLUMNS
    + NUMERIC_4_COLUMNS
    + DATE_COLUMNS
    + TEXT_COLUMNS
)

NUMERIC_COLUMNS: tuple[str, ...] = tuple(NUMERIC_2_COLUMNS + NUMERIC_4_COLUMNS)

SCHEMA_DEFINITION = {
    "campaign_id": BigInteger,
    "article_id": BigInteger,
    "atbs": BigInteger,
    "canceled": BigInteger,
    "clicks": BigInteger,
    "views": BigInteger,
    "orders": BigInteger,
    "shks": BigInteger,
    "sum": Numeric(12, 2),
    "sum_price": Numeric(12, 2),
    "cpc": Numeric(12, 4),
    "cpm": Numeric(12, 4),
    "cr": Numeric(12, 4),
    "ctr": Numeric(12, 4),
    "avg_position": Numeric(12, 4),
    "atbs_pc": BigInteger,
    "canceled_pc": BigInteger,
    "clicks_pc": BigInteger,
    "views_pc": BigInteger,
    "orders_pc": BigInteger,
    "shks_pc": BigInteger,
    "sum_price_pc": Numeric(12, 2),
    "cr_pc": Numeric(12, 4),
    "ctr_pc": Numeric(12, 4),
    "atbs_android": BigInteger,
    "canceled_android": BigInteger,
    "clicks_android": BigInteger,
    "views_android": BigInteger,
    "orders_android": BigInteger,
    "shks_android": BigInteger,
    "sum_price_android": Numeric(12, 2),
    "cr_android": Numeric(12, 4),
    "ctr_android": Numeric(12, 4),
    "atbs_ios": BigInteger,
    "canceled_ios": BigInteger,
    "clicks_ios": BigInteger,
    "views_ios": BigInteger,
    "orders_ios": BigInteger,
    "shks_ios": BigInteger,
    "sum_price_ios": Numeric(12, 2),
    "cr_ios": Numeric(12, 4),
    "ctr_ios": Numeric(12, 4),
    "account": String(255),
    "date": Date,
}

REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
RETRY_ATTEMPTS = MAX_RETRIES
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 120
FULLSTATS_INTERVAL_SECONDS = 20
CAMPAIGN_BATCH_SIZE = 50
MAX_FULLSTATS_RANGE_DAYS = 31
MAX_CONCURRENT_ACCOUNTS = 4

PLATFORM_COLUMN_MAPPING: dict[int, dict[str, str]] = {
    1: {
        "atbs": "atbs_pc",
        "canceled": "canceled_pc",
        "clicks": "clicks_pc",
        "cr": "cr_pc",
        "ctr": "ctr_pc",
        "orders": "orders_pc",
        "shks": "shks_pc",
        "sum_price": "sum_price_pc",
        "views": "views_pc",
    },
    32: {
        "atbs": "atbs_android",
        "canceled": "canceled_android",
        "clicks": "clicks_android",
        "cr": "cr_android",
        "ctr": "ctr_android",
        "orders": "orders_android",
        "shks": "shks_android",
        "sum_price": "sum_price_android",
        "views": "views_android",
    },
    64: {
        "atbs": "atbs_ios",
        "canceled": "canceled_ios",
        "clicks": "clicks_ios",
        "cr": "cr_ios",
        "ctr": "ctr_ios",
        "orders": "orders_ios",
        "shks": "shks_ios",
        "sum_price": "sum_price_ios",
        "views": "views_ios",
    },
}
