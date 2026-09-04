"""Конфигурация новой выгрузки дневных WMS-остатков в PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import Date, DateTime, Integer, String

WMS_STOCK_TABLE_NAME = "wms_stock"
WMS_STOCK_LOOKBACK_DAYS = 1
WMS_STOCK_BACKFILL_START_DATE = date(2026, 7, 29)


@dataclass(frozen=True, slots=True)
class WMSStockZone:
    """Описывает отдельную WMS-зону для загрузки в витрину `public.wms_stock`.

    Бизнес-сценарий:
    одной витрине нужен общий остаток по складу и разрез по нескольким зонам,
    чтобы аналитика могла видеть, где именно физически лежит товар в рамках
    одного календарного дня и `product_id`.
    """

    location_id: int
    column_name: str
    zone_name: str
    include_subtree: bool = True


WMS_STOCK_ZONES: tuple[WMSStockZone, ...] = (
    WMSStockZone(location_id=2, column_name="receiving", zone_name="Приёмка"),
    WMSStockZone(location_id=6, column_name="packing", zone_name="Упаковка"),
    WMSStockZone(location_id=8, column_name="shortage", zone_name="Недостача"),
    WMSStockZone(location_id=35, column_name="fbs", zone_name="ФБС"),
    WMSStockZone(location_id=36, column_name="fbo", zone_name="ФБО"),
    WMSStockZone(location_id=37, column_name="defects", zone_name="Брак"),
    WMSStockZone(location_id=75, column_name="storage", zone_name="Хранение"),
)

# Целевая схема upsert для агрегированных дневных остатков WMS.
WMS_STOCK_SCHEMA_DEFINITION = {
    "balance_date": Date,
    "product_id": String,
    "stock_qty": Integer,
    "receiving": Integer,
    "packing": Integer,
    "shortage": Integer,
    "fbs": Integer,
    "fbo": Integer,
    "defects": Integer,
    "storage": Integer,
    "loaded_at": DateTime,
}

# Соответствие старых технических имен полей понятным англоязычным именам.
# Нужна для сохранения уже загруженных данных при обновлении схемы таблицы.
WMS_STOCK_LEGACY_COLUMN_RENAMES: dict[str, str] = {
    "acceptance": "receiving",
    "priemka": "receiving",
    "upakovka": "packing",
    "nedostacha": "shortage",
    "brak": "defects",
    "khranenie": "storage",
}

# Повторный запуск должен обновлять строку товара за конкретный календарный день.
WMS_STOCK_KEY_COLUMNS = ("balance_date", "product_id")
