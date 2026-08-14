"""Pydantic-схема преобразования legacy orders/sales в WB Order Feed."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from src_oop.jobs.orders_feed.schemas.enums import (
    DataSource,
    SaleType,
    WarehouseType,
)


class LegacyOrderFeedRow(BaseModel):
    """Валидирует готовую legacy-строку перед batch-upsert в общую витрину."""

    model_config = ConfigDict(extra="forbid")

    account: None = None
    srid: str = Field(min_length=1)
    nm_id: int = Field(gt=0)
    chrt_id: None = None
    created_at: datetime
    updated_at: datetime
    status: str = Field(min_length=1, max_length=64)
    cancel_type: None = None
    warehouse_name: str
    warehouse_region: str
    warehouse_type: WarehouseType
    destination_city: str = "Не указано"
    destination_district: str
    seller_price: Decimal = Field(ge=0)
    currency: str = "RUB"
    sale_type: SaleType
    data_source: DataSource
    snapshot_time: datetime
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))

    @field_validator(
        "warehouse_name",
        "warehouse_region",
        "destination_city",
        "destination_district",
        mode="before",
    )
    @classmethod
    def normalize_empty_text(cls, value: object) -> object:
        """Не допускает пустые аналитические измерения из старых таблиц."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return "Не указано"
        return value.strip() if isinstance(value, str) else value

    @classmethod
    def from_source(
        cls,
        row: Mapping[str, Any],
        source: DataSource,
    ) -> LegacyOrderFeedRow:
        """Применяет согласованные правила переноса orders или sales в Python."""
        if source not in {DataSource.ORDERS, DataSource.SALES}:
            raise ValueError(
                f"Источник {source} не поддерживается для legacy-backfill."
            )
        status = cls._status(row, source)
        updated_at = row.get("last_change_date") or row["date_from"]
        return cls(
            srid=str(row["srid"]).strip(),
            nm_id=int(row["article_id"]),
            created_at=row["date_from"],
            updated_at=updated_at,
            status=status,
            warehouse_name=row.get("warehouse_name"),
            warehouse_region=row.get("region_name"),
            warehouse_type=cls._warehouse_type(row.get("warehouse_type")),
            destination_district=cls._destination_district(row),
            seller_price=cls._seller_price(row),
            sale_type=(
                SaleType.B2C if row.get("order_type") == "Клиентский" else SaleType.B2B
            ),
            data_source=source,
            snapshot_time=updated_at,
        )

    @staticmethod
    def _status(row: Mapping[str, Any], source: DataSource) -> str:
        """Восстанавливает текущий статус из разных признаков legacy-источников."""
        if source is DataSource.ORDERS:
            return "cancel" if row.get("is_cancel") is True else "created"
        sale_id = str(row.get("sale_id") or "")
        return "return" if sale_id.startswith("R") else "buyout"

    @staticmethod
    def _warehouse_type(value: object) -> WarehouseType:
        """Приводит русские и технические названия legacy-склада к внутреннему enum."""
        normalized = str(value or "").strip().casefold()
        if normalized in {"склад продавца", "seller"}:
            return WarehouseType.SELLER
        return WarehouseType.WB

    @staticmethod
    def _destination_district(row: Mapping[str, Any]) -> str:
        """Подставляет страну для зарубежной доставки без российского округа."""
        district = str(row.get("oblast_okrug_name") or "").strip()
        country = str(row.get("country_name") or "").strip()
        if not district and country and country != "Россия":
            return country
        return district or "Не указано"

    @staticmethod
    def _seller_price(row: Mapping[str, Any]) -> Decimal:
        """Считает цену продавца по legacy total_price и проценту скидки."""
        total_price = Decimal(str(row.get("total_price") or 0))
        discount_percent = Decimal(str(row.get("discount_percent") or 0))
        result = total_price * (Decimal(1) - discount_percent / Decimal(100))
        return max(result, Decimal(0)).quantize(
            Decimal(1),
            rounding=ROUND_HALF_UP,
        )
