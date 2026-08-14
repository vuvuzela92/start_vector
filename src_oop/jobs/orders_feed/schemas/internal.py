"""Валидируемые модели обмена данными между компонентами Order Feed."""

from __future__ import annotations

from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field, model_validator
from src_oop.jobs.orders_feed.config import MAX_PERIOD_DAYS
from src_oop.jobs.orders_feed.schemas.api import OrderFeedOrderResponse


class OrderFeedPeriod(BaseModel):
    """Проверенный период одного запроса отчёта по текущему статусу заказа."""

    model_config = ConfigDict(frozen=True)

    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_period(self) -> OrderFeedPeriod:
        """Запрещает неоднозначные даты, обратный и слишком длинный период."""
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError(
                "Границы периода Order Feed должны содержать часовой пояс."
            )
        if self.start > self.end:
            raise ValueError("Начало периода Order Feed не может быть позже конца.")
        if self.end - self.start > timedelta(days=MAX_PERIOD_DAYS):
            raise ValueError(
                f"Период Order Feed не может превышать {MAX_PERIOD_DAYS} сутки."
            )
        return self


class OrderFeedPage(BaseModel):
    """Проверенная страница WB с внутренним контекстом загрузки."""

    model_config = ConfigDict(arbitrary_types_allowed=False)

    account: str = Field(min_length=1)
    snapshot_time: str | None
    currency: str = Field(min_length=1)
    orders: list[OrderFeedOrderResponse]
    offset: int = Field(ge=0)
    limit: int = Field(gt=0)
    retries_used: int = Field(default=0, ge=0)

    @property
    def has_next_page(self) -> bool:
        """Продолжает пагинацию только при наличии метки стабильного снимка."""
        return self.snapshot_time is not None and len(self.orders) >= self.limit
