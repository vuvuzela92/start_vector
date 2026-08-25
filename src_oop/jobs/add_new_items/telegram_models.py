"""Модели состояния Telegram-бота для запуска add_new_items."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TelegramLaunchActor:
    """Описывает человека, который запустил job через Telegram.

    Модель нужна, чтобы бот мог показать в статусе и итоговом сообщении, кто
    именно инициировал перенос новых товаров из рабочей группы.
    """

    user_id: int | None
    display_name: str


@dataclass(frozen=True, slots=True)
class TaskRunResult:
    """Хранит итог одного запуска add_new_items из Telegram.

    Объект обслуживает операционный сценарий поддержки: в чате нужно быстро
    понять, кто запускал перенос, когда он завершился и чем именно закончился.
    """

    status: str
    started_at: datetime
    finished_at: datetime
    requested_by: str
    exit_code: int | None
    duration_seconds: int
    summary: str
    details_text: str
    log_excerpt: str


@dataclass(frozen=True, slots=True)
class ActiveRunInfo:
    """Описывает текущий активный запуск job, если он еще не завершился.

    Модель защищает бизнес-правило одиночного запуска: пока перенос новых
    товаров выполняется, бот должен уметь показать, кто его уже запустил.
    """

    started_at: datetime
    requested_by: str
    chat_id: int
