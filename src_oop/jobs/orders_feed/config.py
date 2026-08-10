"""Настройки загрузки и хранения отчёта WB «Лента заказов»."""

from __future__ import annotations

ORDER_FEED_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v1/order-feed"
TABLE_NAME = "wb_order_feed"

# Кабинет входит в ключ, чтобы данные разных продавцов никогда не перезаписали друг друга.
KEY_COLUMNS: tuple[str, ...] = ("account", "srid")

MAX_PERIOD_DAYS = 31
PAGE_LIMIT = 1000
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 180

# Персональные и сервисные токены допускают один запрос в минуту на кабинет.
REQUEST_INTERVAL_SECONDS = 60
MAX_CONCURRENT_ACCOUNTS = 3

SOURCE_COLUMNS: tuple[str, ...] = (
    "nmId",
    "chrtId",
    "srid",
    "createdAt",
    "updatedAt",
    "status",
    "cancelType",
    "warehouseName",
    "warehouseRegion",
    "isMp",
    "destinationCity",
    "destinationDistrict",
    "sellerPrice",
    "isB2b",
)

DB_COLUMNS: tuple[str, ...] = (
    "account",
    "srid",
    "nm_id",
    "chrt_id",
    "created_at",
    "updated_at",
    "status",
    "cancel_type",
    "warehouse_name",
    "warehouse_region",
    "warehouse_type",
    "destination_city",
    "destination_district",
    "seller_price",
    "currency",
    "sale_type",
    "data_source",
    "snapshot_time",
    "loaded_at",
)

DATETIME_COLUMNS: tuple[str, ...] = (
    "created_at",
    "updated_at",
    "snapshot_time",
    "loaded_at",
)
INTEGER_COLUMNS: tuple[str, ...] = ("nm_id", "chrt_id")
NUMERIC_COLUMNS: tuple[str, ...] = ("seller_price",)
