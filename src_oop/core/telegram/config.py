"""Общие настройки Telegram-инфраструктуры проекта."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _parse_int_set(raw_value: str | None) -> frozenset[int]:
    """Преобразует строку из env в множество int для allow-list Telegram.

    Этот helper обслуживает единое бизнес-правило безопасности: доступ к
    корпоративным ботам и саммари должен выдаваться только явно разрешённым
    пользователям и чатам, даже если оператор записал значения через запятую,
    пробелы или JSON-массив.
    """
    if not raw_value:
        return frozenset()

    prepared = raw_value.strip()
    if not prepared:
        return frozenset()

    if prepared.startswith("["):
        parsed = json.loads(prepared)
        return frozenset(int(value) for value in parsed)

    parts = [chunk.strip() for chunk in prepared.replace(";", ",").split(",")]
    return frozenset(int(part) for part in parts if part)


def _parse_chat_ids(raw_value: str | None) -> tuple[str, ...]:
    """Нормализует список chat id для сервисных Telegram-рассылок.

    Функция нужна для автоматических daily/weekly-отчётов. Она принимает как
    одну строку, так и список через запятую, чтобы оператор мог настроить
    получателей без правки кода.
    """
    if not raw_value:
        return ()
    return tuple(
        item.strip()
        for item in raw_value.replace(";", ",").split(",")
        if item.strip()
    )


@dataclass(frozen=True, slots=True)
class TelegramCoreSettings:
    """Хранит общие Telegram-настройки для всех job-модулей проекта.

    Бизнес-смысл этого объекта в том, чтобы разные контуры проекта использовали
    одинаковые правила доступа, одни и те же env-переменные и один источник
    правды для сервисной отправки сообщений.
    """

    bot_token: str
    allowed_user_ids: frozenset[int]
    allowed_chat_ids: frozenset[int]
    service_chat_ids: tuple[str, ...]
    request_timeout_seconds: int

    @classmethod
    def from_env(cls) -> "TelegramCoreSettings":
        """Собирает Telegram-настройки из env без раскрытия секретов в логах.

        Метод обслуживает запуск всех внутренних Telegram-ботов и уведомителей.
        Если токен не задан, это не считается ошибкой загрузки конфигурации:
        конкретный сценарий сам решит, можно ли продолжать без Telegram.
        """
        bot_token = (
            os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
            or os.getenv("WB_FBS_TG_BOT_TOKEN", "").strip()
        )
        allowed_chat_ids_raw = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS")
        service_chat_ids_raw = os.getenv("TELEGRAM_SERVICE_CHAT_IDS") or allowed_chat_ids_raw
        return cls(
            bot_token=bot_token,
            allowed_user_ids=_parse_int_set(os.getenv("TELEGRAM_ALLOWED_USER_IDS")),
            allowed_chat_ids=_parse_int_set(allowed_chat_ids_raw),
            service_chat_ids=_parse_chat_ids(service_chat_ids_raw),
            request_timeout_seconds=int(
                os.getenv("TELEGRAM_REQUEST_TIMEOUT_SECONDS", "20")
            ),
        )
