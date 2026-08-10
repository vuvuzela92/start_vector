"""Pydantic-схемы camelCase-ответа WB Order Feed."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

from src_oop.jobs.orders_feed.schemas.enums import CancelType, OrderStatus


class OrderFeedOrderResponse(BaseModel):
    """Валидирует строку WB и выполняет явный CamelCase → snake_case маппинг."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    nm_id: StrictInt = Field(alias="nmId")
    chrt_id: StrictInt = Field(alias="chrtId")
    srid: str = Field(min_length=1)
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")
    status: OrderStatus
    cancel_type: CancelType | None = Field(default=None, alias="cancelType")
    warehouse_name: str = Field(alias="warehouseName", min_length=1)
    warehouse_region: str = Field(alias="warehouseRegion")
    is_mp: StrictBool = Field(alias="isMp")
    destination_city: str = Field(alias="destinationCity")
    destination_district: str = Field(alias="destinationDistrict")
    seller_price: float = Field(alias="sellerPrice")
    is_b2b: StrictBool = Field(alias="isB2b")


class OrderFeedDataResponse(BaseModel):
    """Валидирует страницу заказов и обязательную метку снимка пагинации."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    snapshot_time: datetime = Field(alias="snapshotTime")
    currency: str = Field(min_length=1)
    orders: list[OrderFeedOrderResponse]


class OrderFeedResponse(BaseModel):
    """Валидирует верхнеуровневую оболочку успешного ответа WB Order Feed."""

    model_config = ConfigDict(extra="ignore")

    data: OrderFeedDataResponse
