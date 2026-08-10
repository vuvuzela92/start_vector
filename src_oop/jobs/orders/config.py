from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Integer, Numeric, String

ORDERS_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"

# Целевая таблица витрины заказов WB и ключ, по которому повторный запуск обновляет заказ.
TABLE_NAME = "orders"
KEY_COLUMNS: tuple[str, ...] = ("date", "srid")

# Таймауты и повторы защищают регулярную загрузку от кратковременных сбоев WB API.
REQUEST_TIMEOUT_SECONDS = 40
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 120

# Ограничение параллельных кабинетов снижает риск HTTP 429 от WB Statistics API.
MAX_CONCURRENT_ACCOUNTS = 3

# Забираем четыре прошедших дня, чтобы догружать поздние изменения заказов.
DEFAULT_DAYS_BACK = 4

# Поля, которые ожидаются в сыром ответе WB и переносятся в витрину orders.
SOURCE_COLUMNS: tuple[str, ...] = (
    "date",
    "lastChangeDate",
    "warehouseName",
    "warehouseType",
    "countryName",
    "oblastOkrugName",
    "regionName",
    "supplierArticle",
    "nmId",
    "barcode",
    "category",
    "subject",
    "brand",
    "techSize",
    "incomeID",
    "isSupply",
    "isRealization",
    "totalPrice",
    "discountPercent",
    "spp",
    "finishedPrice",
    "priceWithDisc",
    "isCancel",
    "cancelDate",
    "orderType",
    "sticker",
    "gNumber",
    "srid",
)

# Сопоставляет поля WB API с колонками витрины orders в PostgreSQL.
COLUMN_RENAME_MAP: dict[str, str] = {
    "lastChangeDate": "last_change_date",
    "warehouseName": "warehouse_name",
    "warehouseType": "warehouse_type",
    "countryName": "country_name",
    "oblastOkrugName": "oblast_okrug_name",
    "regionName": "region_name",
    "supplierArticle": "supplier_article",
    "nmId": "article_id",
    "techSize": "tech_size",
    "incomeID": "income_id",
    "isSupply": "is_supply",
    "isRealization": "is_realization",
    "totalPrice": "total_price",
    "discountPercent": "discount_percent",
    "finishedPrice": "finished_price",
    "priceWithDisc": "price_with_disc",
    "isCancel": "is_cancel",
    "cancelDate": "cancel_date",
    "orderType": "order_type",
    "gNumber": "g_number",
}

# Финальный порядок колонок для записи в PostgreSQL через общий upsert.
DB_COLUMNS: tuple[str, ...] = (
    "date",
    "date_from",
    "last_change_date",
    "warehouse_name",
    "warehouse_type",
    "country_name",
    "oblast_okrug_name",
    "region_name",
    "supplier_article",
    "article_id",
    "barcode",
    "category",
    "subject",
    "brand",
    "tech_size",
    "income_id",
    "is_supply",
    "is_realization",
    "total_price",
    "discount_percent",
    "spp",
    "finished_price",
    "price_with_disc",
    "is_cancel",
    "cancel_date",
    "order_type",
    "sticker",
    "g_number",
    "srid",
)

# Группы колонок для приведения типов перед записью в PostgreSQL.
DATETIME_COLUMNS: tuple[str, ...] = (
    "date_from",
    "last_change_date",
    "cancel_date",
)
DATE_COLUMNS: tuple[str, ...] = ("date",)
INTEGER_COLUMNS: tuple[str, ...] = (
    "article_id",
    "income_id",
    "discount_percent",
    "spp",
)
NUMERIC_COLUMNS: tuple[str, ...] = (
    "total_price",
    "finished_price",
    "price_with_disc",
)
BOOLEAN_COLUMNS: tuple[str, ...] = (
    "is_supply",
    "is_realization",
    "is_cancel",
)

# SQLAlchemy-схема таблицы orders; используется общим Database.sync_data_to_postgres().
SCHEMA_DEFINITION = {
    "date": Date,
    "date_from": DateTime,
    "last_change_date": DateTime,
    "warehouse_name": String(255),
    "warehouse_type": String(255),
    "country_name": String(255),
    "oblast_okrug_name": String(255),
    "region_name": String(255),
    "supplier_article": String(255),
    "article_id": BigInteger,
    "barcode": String(255),
    "category": String(255),
    "subject": String(255),
    "brand": String(255),
    "tech_size": String(255),
    "income_id": BigInteger,
    "is_supply": Boolean,
    "is_realization": Boolean,
    "total_price": Numeric(10, 2),
    "discount_percent": Integer,
    "spp": Integer,
    "finished_price": Numeric(10, 2),
    "price_with_disc": Numeric(10, 2),
    "is_cancel": Boolean,
    "cancel_date": DateTime,
    "order_type": String(255),
    "sticker": String(255),
    "g_number": String(255),
    "srid": String(255),
}
