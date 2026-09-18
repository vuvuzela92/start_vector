"""Настройки job контроля FBS-остатков WB."""

from __future__ import annotations

import os
from dataclasses import dataclass


STOCKS_URL_TEMPLATE = "https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}"
CHRT_IDS_CHUNK_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = 40
MAX_RETRIES = 5
RETRY_DELAYS_SECONDS = (5, 10, 20, 40, 80)
CLICKHOUSE_HISTORY_TABLE = "wb_fbs_stock_checks"
CLICKHOUSE_CURRENT_TABLE = "wb_fbs_stock_current"


@dataclass(frozen=True, slots=True)
class StockControlSettings:
    """Хранит параметры запуска контроля и адрес чата коммерческого отдела."""

    bitrix_dialog_id: str
    bitrix_enabled: bool = False
    concurrency: int = 2

    @classmethod
    def from_env(cls) -> "StockControlSettings":
        """Читает безопасные настройки job из окружения без хранения секретов в коде."""
        return cls(
            bitrix_dialog_id=os.getenv("WB_STOCK_CONTROL_BITRIX_DIALOG_ID", "").strip(),
            bitrix_enabled=os.getenv(
                "WB_STOCK_CONTROL_BITRIX_ENABLED",
                "true",
            ).strip().lower() in {"1", "true", "yes"},
            concurrency=max(1, int(os.getenv("WB_STOCK_CONTROL_CONCURRENCY", "2"))),
        )
