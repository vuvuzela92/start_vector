"""Настройки Telegram-бота управления доступами."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class DatabaseAccessTelegramSettings:
    """Хранит allow-list управляющих Telegram-групп для операций с правами БД."""

    bot_token: str
    manager_chat_ids: frozenset[int]

    @classmethod
    def from_env(cls) -> DatabaseAccessTelegramSettings:
        """Загружает Telegram ID управляющих групп без раскрытия иных настроек.

        Пустой список намеренно не разрешает доступ никому: это защищает
        управляющий бот при неполной конфигурации окружения. ID группы не
        является секретом и может храниться в обычной конфигурации.
        """

        load_dotenv()
        raw_value = os.getenv("DATABASE_ACCESS_MANAGER_TELEGRAM_CHAT_IDS", "")
        return cls(
            bot_token=os.getenv("DB_MANAGER_SV_TOKEN", "").strip(),
            manager_chat_ids=frozenset(
                int(item.strip())
                for item in raw_value.replace(";", ",").split(",")
                if item.strip()
            )
        )
