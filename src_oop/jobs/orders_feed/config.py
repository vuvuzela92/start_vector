"""Настройки загрузки и хранения отчёта WB «Лента заказов»."""

from __future__ import annotations

ORDER_FEED_URL = (
    "https://seller-analytics-api.wildberries.ru/api/analytics/v1/order-feed"
)
TABLE_NAME = "wb_order_feed"

# Кабинет входит в ключ, чтобы данные разных продавцов никогда не перезаписали друг друга.
KEY_COLUMNS: tuple[str, ...] = ("account", "srid")

# Только изменяемые атрибуты заказа обновляются при конфликте; идентичность строки неизменна.
UPSERT_UPDATE_COLUMNS: tuple[str, ...] = (
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
    "snapshot_time",
    "loaded_at",
)

MAX_PERIOD_DAYS = 31
# Запас защищает границу 31 суток от сетевой задержки и повторов одного запроса.
HISTORY_BOUNDARY_SAFETY_MINUTES = 5
PAGE_LIMIT = 9000
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 180

# Персональные и сервисные токены допускают один запрос в минуту на кабинет.
REQUEST_INTERVAL_SECONDS = 60
MAX_CONCURRENT_ACCOUNTS = 3

# Страница WB делится на короткие транзакции, чтобы не перегружать соединение PostgreSQL.
DB_WRITE_CHUNK_SIZE = 2000
DB_WRITE_MAX_RETRIES = 4
DB_WRITE_RETRY_BASE_SECONDS = 2
DB_WRITE_RETRY_MAX_SECONDS = 30
