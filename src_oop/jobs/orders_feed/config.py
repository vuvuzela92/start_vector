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
