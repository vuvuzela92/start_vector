"""Конфигурация Telegram-бота для запуска add_new_items."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

DEFAULT_ALLOWED_CHAT_IDS = frozenset({-5448345087})
DEFAULT_SUBPROCESS_TIMEOUT_SECONDS = 30 * 60
DEFAULT_LOG_TAIL_LINES = 20
DEFAULT_TELEGRAM_LOG_MAX_LENGTH = 3000


def parse_int_set(raw_value: str | None) -> frozenset[int]:
    """Преобразует строку env в множество целых значений для Telegram-настроек.

    Helper нужен для модульных allow-list и списков групп. Он поддерживает
    строку через запятую, точку с запятой и JSON-массив, чтобы оператор мог
    настраивать доступ без правки кода и без отдельного формата для каждого
    бота.
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


@dataclass(frozen=True, slots=True)
class AddNewItemsTelegramSettings:
    """Хранит настройки Telegram-бота как части job add_new_items.

    Бизнес-смысл этого объекта в том, чтобы Telegram-запуск жил рядом с
    основным сценарием переноса новых товаров, но сохранял свой токен, свою
    группу и собственные правила доступа независимо от других ботов проекта.
    """

    bot_token: str
    allowed_chat_ids: frozenset[int]
    allowed_user_ids: frozenset[int]
    subprocess_timeout_seconds: int
    log_tail_lines: int
    telegram_log_max_length: int

    @classmethod
    def from_env(cls) -> "AddNewItemsTelegramSettings":
        """Собирает настройки Telegram-бота add_new_items из env.

        Метод обслуживает рабочий сценарий запуска переноса новых товаров из
        группы Telegram. Если оператор не задал список чатов явно, бот
        ограничивается согласованной группой по умолчанию, а таймаут защищает
        от зависшего серверного процесса.
        """
        allowed_chat_ids_raw = os.getenv("ADD_NEW_ITEMS_TELEGRAM_ALLOWED_CHAT_IDS")
        return cls(
            bot_token=os.getenv("ADD_NEW_ITEMS_TELEGRAM_BOT_TOKEN", "").strip(),
            allowed_chat_ids=(
                parse_int_set(allowed_chat_ids_raw)
                if allowed_chat_ids_raw
                else DEFAULT_ALLOWED_CHAT_IDS
            ),
            allowed_user_ids=parse_int_set(
                os.getenv("ADD_NEW_ITEMS_TELEGRAM_ALLOWED_USER_IDS")
            ),
            subprocess_timeout_seconds=int(
                os.getenv(
                    "ADD_NEW_ITEMS_TELEGRAM_SUBPROCESS_TIMEOUT_SECONDS",
                    str(DEFAULT_SUBPROCESS_TIMEOUT_SECONDS),
                )
            ),
            log_tail_lines=int(
                os.getenv(
                    "ADD_NEW_ITEMS_TELEGRAM_LOG_TAIL_LINES",
                    str(DEFAULT_LOG_TAIL_LINES),
                )
            ),
            telegram_log_max_length=int(
                os.getenv(
                    "ADD_NEW_ITEMS_TELEGRAM_LOG_MAX_LENGTH",
                    str(DEFAULT_TELEGRAM_LOG_MAX_LENGTH),
                )
            ),
        )
