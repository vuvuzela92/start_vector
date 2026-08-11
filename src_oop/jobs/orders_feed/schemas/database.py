"""Pydantic-схема нормализованной строки PostgreSQL WB Order Feed."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src_oop.jobs.orders_feed.schemas.api import OrderFeedOrderResponse
from src_oop.jobs.orders_feed.schemas.enums import (
    DataSource,
    SaleType,
    WarehouseType,
)


class OrderFeedDatabaseRow(BaseModel):
    """Валидирует и формирует готовую к upsert snake_case-строку витрины."""

    model_config = ConfigDict(extra="forbid")

    account: str = Field(min_length=1)
    srid: str = Field(min_length=1)
    nm_id: int
    chrt_id: int
    created_at: datetime
    updated_at: datetime
    status: str = Field(min_length=1, max_length=64)
    cancel_type: str | None = Field(default=None, max_length=64)
    warehouse_name: str
    warehouse_region: str
    warehouse_type: WarehouseType
    destination_city: str
    destination_district: str
    seller_price: Decimal
    currency: str = Field(min_length=1)
    sale_type: SaleType
    data_source: DataSource = DataSource.ORDER_FEED
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
    def normalize_empty_location(cls, value: object) -> object:
        """Заменяет пустые поля местоположения WB значением для аналитики."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return "Не указано"
        return value.strip() if isinstance(value, str) else value

    @field_validator("created_at", "updated_at", "snapshot_time", "loaded_at")
    @classmethod
    def normalize_datetime_to_utc(cls, value: datetime) -> datetime:
        """Приводит все даты витрины к UTC и запрещает неоднозначное время без timezone."""
        if value.tzinfo is None:
            raise ValueError("Дата Order Feed должна содержать часовой пояс.")
        return value.astimezone(UTC)

    @classmethod
    def from_api(
        cls,
        order: OrderFeedOrderResponse,
        account: str,
        currency: str,
        snapshot_time: str | datetime,
    ) -> OrderFeedDatabaseRow:
        """Преобразует заказ WB, сохраняя внешние справочники без потери значений."""
        return cls(
            account=account,
            srid=order.srid,
            nm_id=order.nm_id,
            chrt_id=order.chrt_id,
            created_at=order.created_at,
            updated_at=order.updated_at,
            status=order.status,
            cancel_type=order.cancel_type,
            warehouse_name=order.warehouse_name,
            warehouse_region=order.warehouse_region,
            warehouse_type=(WarehouseType.SELLER if order.is_mp else WarehouseType.WB),
            destination_city=order.destination_city,
            destination_district=order.destination_district,
            seller_price=Decimal(str(order.seller_price)),
            currency=currency,
            sale_type=SaleType.B2B if order.is_b2b else SaleType.B2C,
            snapshot_time=snapshot_time,
        )
