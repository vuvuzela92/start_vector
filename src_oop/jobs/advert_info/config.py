from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, DateTime, String

ADVERTS_URL = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
CAMPAIGN_STATUSES: tuple[int, ...] = (9,)

TABLE_NAME = "advert_campaigns_info"
KEY_COLUMNS: tuple[str, ...] = ("campaign_id",)

DB_COLUMNS: tuple[str, ...] = (
    "campaign_id",
    "campaign_name",
    "bid_type",
    "nm_id",
    "search_bid",
    "recommendations_bid",
    "payment_type",
    "recommendations",
    "search",
    "created_at_campaign",
    "account",
)

INT_COLUMNS: tuple[str, ...] = (
    "campaign_id",
    "nm_id",
    "search_bid",
    "recommendations_bid",
)
BOOLEAN_COLUMNS: tuple[str, ...] = ("recommendations", "search")
DATETIME_COLUMNS: tuple[str, ...] = ("created_at_campaign",)
TEXT_COLUMNS: tuple[str, ...] = ("campaign_name", "bid_type", "payment_type", "account")

SCHEMA_DEFINITION = {
    "campaign_id": BigInteger,
    "campaign_name": String,
    "bid_type": String,
    "nm_id": BigInteger,
    "search_bid": BigInteger,
    "recommendations_bid": BigInteger,
    "payment_type": String,
    "recommendations": Boolean,
    "search": Boolean,
    "created_at_campaign": DateTime,
    "account": String,
}

REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 60
MAX_CONCURRENT_ACCOUNTS = 4
