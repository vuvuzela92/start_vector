from __future__ import annotations

from dataclasses import dataclass

STOCKS_URL_TEMPLATE = "https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}"

REQUEST_TIMEOUT_SECONDS = 40
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 120
CHRT_IDS_CHUNK_SIZE = 1000
REFRESH_VERIFY_ATTEMPTS = 5
REFRESH_VERIFY_SLEEP_SECONDS = (5, 10, 20, 30, 60)

UNIT_TABLE_TITLE = "UNIT 2.0 (tested) управление остатками"
UNIT_SHEET_TITLE = "MAIN (tested)"
SOPOST_SHEET_TITLE = "Сопост"
HEADER_ROW_INDEX = 1
DATA_START_ROW = 2

ARTICLE_COLUMN = "Артикул"
ACCOUNT_COLUMN = "ЛК"
WILD_COLUMN = "wild"
INSERT_AFTER_COLUMN = "Новый остаток"

TOTAL_STOCK_COLUMN = "ФБС общий остаток"
MIN_STOCK_COLUMN = "Минимальный остаток"
SOPOST_ADD_COLUMN = "Добавляем"
NEW_STOCK_ALL_WAREHOUSES_COLUMN = "Новый остаток для всех складов"
NEW_STOCK_VESHKI_COLUMN = "Новый остаток Вешки"
VESHKI_WAREHOUSE_ID = 2

STOCK_MANAGEMENT_COLUMNS: tuple[str, ...] = (
    TOTAL_STOCK_COLUMN,
    NEW_STOCK_ALL_WAREHOUSES_COLUMN,
    NEW_STOCK_VESHKI_COLUMN,
)

CREATE_MISSING_COLUMNS_ENV = "WB_FBS_CREATE_MISSING_COLUMNS"


@dataclass(frozen=True, slots=True)
class FBSWarehouseColumnConfig:
    """Описывает внутренний FBS-склад, который участвует в общем остатке и распределении."""

    warehouse_id: int
    warehouse_alias: str


# warehouse_id берется из нашей таблицы warehouses_fbs, а не из WB.
# Активность склада определяется в БД: сценарии используют только status='active'.
TARGET_WAREHOUSES: tuple[FBSWarehouseColumnConfig, ...] = (
    FBSWarehouseColumnConfig(
        warehouse_id=2,
        warehouse_alias="Вешки",
    ),
    FBSWarehouseColumnConfig(
        warehouse_id=1,
        warehouse_alias="Казань",
    ),
    FBSWarehouseColumnConfig(
        warehouse_id=3,
        warehouse_alias="Волгоград",
    ),
    FBSWarehouseColumnConfig(
        warehouse_id=4,
        warehouse_alias="Шушары",
    ),
    FBSWarehouseColumnConfig(
        warehouse_id=5,
        warehouse_alias="Екатеринбург",
    ),
    FBSWarehouseColumnConfig(
        warehouse_id=6,
        warehouse_alias="Владивосток",
    ),
)

AUTO_REFILL_APPLY_ENV = "WB_FBS_AUTO_REFILL_APPLY"
AUTO_REFILL_VESHKI_ONLY_ENV = "WB_FBS_AUTO_REFILL_VESHKI_ONLY"
