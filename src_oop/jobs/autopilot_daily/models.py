from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class AutopilotDailySummary:
    """Итог выполнения дневного сценария автопилота.

    Бизнес-логика:
    собирает счетчики по основным этапам обновления ПУ и связанных UNIT-листов,
    чтобы в логах было видно, какие части ежедневной актуализации прошли, а
    какие были пропущены или завершились частичной ошибкой.
    """

    articles_total: int = 0
    current_rows: int = 0
    history_rows: int = 0
    metrics_attempted: int = 0
    metrics_written: int = 0
    metrics_failed: list[str] = field(default_factory=list)
    avg_position_current_rows: int = 0
    avg_position_history_rows: int = 0
    goods_info_rows: int = 0
    unit_adv_rows: int = 0
    sopost_rows: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None
