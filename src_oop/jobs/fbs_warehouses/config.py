from __future__ import annotations

from sqlalchemy import BigInteger, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB

OFFICES_URL = "https://marketplace-api.wildberries.ru/api/v3/offices"
WAREHOUSES_URL = "https://marketplace-api.wildberries.ru/api/v3/warehouses"

TABLE_NAME = "warehouses_fbs"

# Ключ синхронизации: один наш склад в одном WB-аккаунте должен быть записан один раз.
KEY_COLUMNS: tuple[str, ...] = ("warehouse_id", "account")

# Схема справочника FBS-складов: warehouse_id общий для бизнеса, wb_warehouse_id свой для аккаунта.
SCHEMA_DEFINITION = {
    "warehouse_id": BigInteger,
    "warehouse_name": String(255),
    "account": String(255),
    "wb_warehouse_id": BigInteger,
    "wb_office_id": BigInteger,
    "status": String(50),
    "create_payload": JSONB,
    "created_at": DateTime,
    "updated_at": DateTime,
    "deleted_at": DateTime,
}

# Таймауты и повторы защищают ручные операции со складами от кратковременных сбоев WB API.
REQUEST_TIMEOUT_SECONDS = 40
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 120

# Короткие повторы для PostgreSQL нужны ручным сценариям, где разовый обрыв соединения
# не должен сразу останавливать создание или удаление FBS-склада.
DB_MAX_RETRIES = 3
DB_RETRY_BASE_SLEEP_SECONDS = 2
DB_RETRY_MAX_SLEEP_SECONDS = 15

# Env-параметры позволяют запускать операции через текущий task registry без расширения CLI.
ACCOUNT_ENV = "WB_FBS_ACCOUNT"
ACCOUNTS_ENV = "WB_FBS_ACCOUNTS"
OFFICE_ID_ENV = "WB_FBS_OFFICE_ID"
WAREHOUSE_NAME_ENV = "WB_FBS_WAREHOUSE_NAME"
WAREHOUSE_ID_ENV = "WB_FBS_WAREHOUSE_ID"
OUR_WAREHOUSE_ID_ENV = "WB_FBS_OUR_WAREHOUSE_ID"
OUTPUT_PATH_ENV = "WB_FBS_OUTPUT_PATH"
IMPORT_SOURCE_PATH_ENV = "WB_FBS_IMPORT_SOURCE_PATH"
