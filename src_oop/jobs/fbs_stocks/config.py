from __future__ import annotations

from dataclasses import dataclass

STOCKS_URL_TEMPLATE = "https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}"

REQUEST_TIMEOUT_SECONDS = 40
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 5
RETRY_MAX_SLEEP_SECONDS = 120
CHRT_IDS_CHUNK_SIZE = 1000

UNIT_TABLE_TITLE = "UNIT 2.0 (tested) управление остатками"
UNIT_SHEET_TITLE = "MAIN (tested)"
HEADER_ROW_INDEX = 1
DATA_START_ROW = 2

ARTICLE_COLUMN = "Артикул"
ACCOUNT_COLUMN = "ЛК"
INSERT_AFTER_COLUMN = "Новый остаток"

STOCK_MANAGEMENT_COLUMNS: tuple[str, ...] = (
    "Остаток Вешки",
    "ФБС Вешки",
    "Новый остаток Вешки",
    "Остаток Казань",
    "ФБС Казань",
    "Новый остаток Казань",
)

CREATE_MISSING_COLUMNS_ENV = "WB_FBS_CREATE_MISSING_COLUMNS"


@dataclass(frozen=True, slots=True)
class FBSWarehouseColumnConfig:
    """Связывает наш склад из warehouses_fbs с колонкой UNIT для записи FBS-остатка."""

    warehouse_id: int
    warehouse_alias: str
    target_column: str
    new_stock_column: str


# warehouse_id берется из нашей таблицы warehouses_fbs, а не из WB.
TARGET_WAREHOUSES: tuple[FBSWarehouseColumnConfig, ...] = (
    FBSWarehouseColumnConfig(
        warehouse_id=2,
        warehouse_alias="Вешки",
        target_column="ФБС Вешки",
        new_stock_column="Новый остаток Вешки",
    ),
    FBSWarehouseColumnConfig(
        warehouse_id=1,
        warehouse_alias="Казань",
        target_column="ФБС Казань",
        new_stock_column="Новый остаток Казань",
    ),
)

APPLY_STOCKS_ENV = "WB_FBS_APPLY_STOCKS"
