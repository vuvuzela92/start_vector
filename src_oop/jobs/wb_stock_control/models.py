"""Доменные модели контроля опубликованных остатков WB."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class StockTarget:
    """Описывает товар, который нужно проверить на складе конкретного ЛК."""

    account: str
    warehouse_id: int
    wb_warehouse_id: int
    article_id: int
    wild: str
    chrt_id: int
    is_in_unit: bool


@dataclass(frozen=True, slots=True)
class StockObservation:
    """Фиксирует результат проверки одной карточки на одном складе."""

    target: StockTarget
    amount: int | None
    check_status: str
    error_type: str | None
    checked_at: datetime
