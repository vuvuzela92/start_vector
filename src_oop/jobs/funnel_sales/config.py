from __future__ import annotations

from sqlalchemy import BigInteger, Date, Numeric, String

FUNNEL_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"

# Целевая таблица ежедневной воронки продаж и ключ обновления одной строки.
TABLE_NAME = "funnel_daily"
KEY_COLUMNS: tuple[str, ...] = ("nm_id", "date")

# Таймауты и retry защищают регулярную выгрузку от временных ошибок WB API.
REQUEST_TIMEOUT_SECONDS = 40
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 10
RETRY_MAX_SLEEP_SECONDS = 120

# Ограничивает параллельность по кабинетам и дням, чтобы реже получать HTTP 429.
MAX_CONCURRENT_REQUESTS = 3

# Небольшой разнос старта запросов снижает риск коротких burst-пиков в WB API.
REQUEST_STAGGER_SECONDS = 0.35

# Legacy-сценарий поднимает последние 28 завершённых дней.
DEFAULT_DAYS_BACK = 28
PAGE_LIMIT = 1000

# Историческое исключение из legacy: эти строки считались некорректными и удалялись вручную.
EXCLUDED_NM_IDS_BY_DATE: tuple[tuple[int, tuple[str, ...]], ...] = (
    (165564235, ("2026-05-18", "2026-05-19")),
)

# Поля сырого ответа WB, которые участвуют в витрине funnel_daily.
DB_COLUMNS: tuple[str, ...] = (
    "account",
    "nm_id",
    "vendor_code",
    "title",
    "subject_id",
    "subject_name",
    "brand_name",
    "product_rating",
    "feedback_rating",
    "stocks_wb",
    "stocks_mp",
    "balance_sum",
    "open_count",
    "cart_count",
    "order_count",
    "orders_sum",
    "buyout_count",
    "buyout_sum",
    "cancel_count",
    "cancel_sum",
    "avg_price",
    "avg_orders_count_per_day",
    "share_order_percent",
    "add_to_wish_list",
    "time_to_ready",
    "localization_percent",
    "date",
    "month",
    "wild",
)

# Группы колонок для приведения к ожидаемым типам перед upsert в PostgreSQL.
INTEGER_COLUMNS: tuple[str, ...] = (
    "nm_id",
    "subject_id",
    "stocks_wb",
    "stocks_mp",
    "balance_sum",
    "open_count",
    "cart_count",
    "order_count",
    "orders_sum",
    "buyout_count",
    "buyout_sum",
    "cancel_count",
    "cancel_sum",
    "add_to_wish_list",
    "time_to_ready",
)
NUMERIC_COLUMNS: tuple[str, ...] = (
    "product_rating",
    "feedback_rating",
    "avg_price",
    "avg_orders_count_per_day",
    "share_order_percent",
    "localization_percent",
)
DATE_COLUMNS: tuple[str, ...] = ("date",)

# SQLAlchemy-схема таблицы funnel_daily для общего upsert через Database.sync_data_to_postgres().
SCHEMA_DEFINITION = {
    "account": String(255),
    "nm_id": BigInteger,
    "vendor_code": String(255),
    "title": String(255),
    "subject_id": BigInteger,
    "subject_name": String(255),
    "brand_name": String(255),
    "product_rating": Numeric(5, 2),
    "feedback_rating": Numeric(5, 2),
    "stocks_wb": BigInteger,
    "stocks_mp": BigInteger,
    "balance_sum": BigInteger,
    "open_count": BigInteger,
    "cart_count": BigInteger,
    "order_count": BigInteger,
    "orders_sum": BigInteger,
    "buyout_count": BigInteger,
    "buyout_sum": BigInteger,
    "cancel_count": BigInteger,
    "cancel_sum": BigInteger,
    "avg_price": Numeric(12, 2),
    "avg_orders_count_per_day": Numeric(10, 2),
    "share_order_percent": Numeric(10, 2),
    "add_to_wish_list": BigInteger,
    "time_to_ready": BigInteger,
    "localization_percent": Numeric(10, 2),
    "date": Date,
    "month": String(7),
    "wild": String(255),
}
