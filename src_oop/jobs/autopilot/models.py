from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

MetricValues = dict[int, float | int | str | None]
ProfitCalculationInputs = dict[int, dict[str, float]]


@dataclass(frozen=True, slots=True)
class WBCardSnapshot:
    article_id: int
    promo_status: int | None = None
    rating: float | None = None
    full_price: float | None = None
    spp: float | None = None
    discounted_price: float | None = None


@dataclass(slots=True)
class AutopilotHourlySummary:
    """Разделяет результат обновления ПУ, сохранения среза и доставки мониторинга."""

    articles_total: int = 0
    metrics_attempted: int = 0
    metrics_written: int = 0
    metrics_failed: list[str] = field(default_factory=list)
    funnel_rows: int = 0
    cometa_rows: int = 0
    wb_card_rows: int = 0
    spp_history_rows: int = 0
    monitoring_capture_status: str = "not_started"
    monitoring_synced: int = 0
    monitoring_conflicts: int = 0
    monitoring_delivery_failed: bool = False
    monitoring_delivery_locked: bool = False
    monitoring_pu_written: bool = False
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None
