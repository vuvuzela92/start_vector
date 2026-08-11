"""Pydantic-схемы camelCase-ответа WB Order Feed."""

from datetime import datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
)
from src_oop.jobs.orders_feed.schemas.enums import CancelType, OrderStatus


class OrderFeedOrderResponse(BaseModel):
    """Валидирует строку WB и выполняет явный CamelCase → snake_case маппинг."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    nm_id: StrictInt = Field(alias="nmId", gt=0)
    chrt_id: StrictInt = Field(alias="chrtId", gt=0)
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
    seller_price: float = Field(alias="sellerPrice", ge=0, allow_inf_nan=False)
    is_b2b: StrictBool = Field(alias="isB2b")


class OrderFeedDataResponse(BaseModel):
    """Валидирует страницу заказов и обязательную метку снимка пагинации."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    snapshot_time: datetime | None = Field(default=None, alias="snapshotTime")
    currency: str = Field(min_length=1)
    orders: list[OrderFeedOrderResponse]

    @field_validator("snapshot_time", mode="before")
    @classmethod
    def empty_snapshot_time_is_missing(cls, value: object) -> object:
        """WB иногда присылает пустую метку, если результат помещается в одну страницу."""
        return None if value == "" else value


class OrderFeedResponse(BaseModel):
    """Валидирует верхнеуровневую оболочку успешного ответа WB Order Feed."""

    model_config = ConfigDict(extra="ignore")

    data: OrderFeedDataResponse


class OrderFeedAPIErrorResponse(BaseModel):
    """Общие диагностические поля ошибок WB разных API-шлюзов."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    title: str | None = None
    detail: str | None = None
    code: str | None = None
    request_id: str | None = Field(default=None, alias="requestId")
    origin: str | None = None
    status: int | None = None
    status_text: str | None = Field(default=None, alias="statusText")
    timestamp: datetime | None = None
