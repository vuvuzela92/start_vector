"""Конфигурация новой выгрузки дневных WMS-остатков в PostgreSQL."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, DateTime, Integer, String

WMS_STOCK_TABLE_NAME = "wms_stock"
WMS_STOCK_LOOKBACK_DAYS = 7
WMS_STOCK_BACKFILL_START_DATE = date(2026, 7, 29)

# Целевая схема upsert для агрегированных дневных остатков WMS.
WMS_STOCK_SCHEMA_DEFINITION = {
    "balance_date": Date,
    "product_id": String,
    "stock_qty": Integer,
    "loaded_at": DateTime,
}

# Повторный запуск должен обновлять строку товара за конкретный календарный день.
WMS_STOCK_KEY_COLUMNS = ("balance_date", "product_id")
