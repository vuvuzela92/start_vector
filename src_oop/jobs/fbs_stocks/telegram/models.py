from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class FBSNotificationEvent:
    """Описывает единичную проблему FBS-сценария для последующей агрегации в Telegram.

    Бизнес-сценарий: ошибки могут возникать по задаче целиком, по группе `wild` или по конкретному
    артикулу/складу. Единая модель события позволяет сначала собрать причины сбоев во время
    выполнения сценария, а затем одним местом сформировать понятные оператору уведомления.
    """

    job_name: str
    severity: str
    reason_code: str
    reason: str
    detail: str | None = None
    account: str | None = None
    wild: str | None = None
    article_id: int | None = None
    warehouse_name: str | None = None
    wb_warehouse_id: int | None = None
    wb_office_id: int | None = None
    happened_at: datetime | None = None


@dataclass(slots=True)
class FBSJobFailureContext:
    """Описывает фатальную ошибку всего FBS-сценария для Telegram-уведомления.

    Бизнес-сценарий: когда задача оборвалась до завершения, пользователю нужно получить одно
    краткое сообщение с названием job, типом ошибки и безопасным контекстом без трассировки и
    секретов, чтобы быстро понять, что именно не сработало.
    """

    job_name: str
    reason: str
    error_type: str
    account_scope: str
    detail: str | None = None
    happened_at: datetime | None = None
