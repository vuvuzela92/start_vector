from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


MetricValues = dict[int, float | int | str | None]


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
    articles_total: int = 0
    metrics_attempted: int = 0
    metrics_written: int = 0
    metrics_failed: list[str] = field(default_factory=list)
    funnel_rows: int = 0
    cometa_rows: int = 0
    wb_card_rows: int = 0
    spp_history_rows: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None
