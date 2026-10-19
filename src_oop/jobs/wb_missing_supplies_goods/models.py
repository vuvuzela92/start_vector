"""Модели данных для загрузки товаров поставок WB."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SupplyInfo:
    """Описывает поставку, которую API вернуло в списке поставок аккаунта."""

    supply_id: int
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AccountResult:
    """Содержит итог обработки одного ЛК для итогового логирования job."""

    account: str
    supplies_checked: int
    supplies_saved: int
    goods_inserted: int
