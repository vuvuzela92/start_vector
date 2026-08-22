"""Проверки доступа к корпоративным Telegram-сценариям."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TelegramActor:
    """Описывает пользователя и чат, из которых пришёл Telegram-запрос.

    Модель обслуживает единое бизнес-правило безопасности: внутренние отчёты
    нельзя показывать случайным пользователям и посторонним чатам, поэтому
    проверка прав должна работать одинаково для всех Telegram-модулей проекта.
    """

    user_id: int | None
    chat_id: int | None


def is_actor_allowed(
    actor: TelegramActor,
    *,
    allowed_user_ids: frozenset[int],
    allowed_chat_ids: frozenset[int],
) -> bool:
    """Возвращает `True`, если actor проходит allow-list Telegram.

    Бизнес-правило: если указан хотя бы один allow-list, actor должен пройти
    все активные ограничения. Это защищает корпоративные саммари от выдачи
    наружу и делает поведение предсказуемым для всех будущих модулей.
    """
    if allowed_user_ids and actor.user_id not in allowed_user_ids:
        return False
    if allowed_chat_ids and actor.chat_id not in allowed_chat_ids:
        return False
    return True
