"""Запуск Telegram-бота управления доступами PostgreSQL."""

from __future__ import annotations

import asyncio

from src_oop.jobs.database_access_management.telegram_config import (
    DatabaseAccessTelegramSettings,
)
from src_oop.jobs.database_access_management.telegram_router import (
    create_database_access_router,
)


async def run_telegram_bot() -> None:
    """Запускает закрытый Telegram-бот для руководителей управления доступами.

    Entrypoint обслуживает интерактивный MVP: проверяет отдельный токен и
    allow-list до подключения к Telegram, затем передаёт сообщения router-у
    выдачи и отзыва PostgreSQL-доступов.
    """

    from aiogram import Bot, Dispatcher
    from aiogram.types import BotCommand

    settings = DatabaseAccessTelegramSettings.from_env()
    if not settings.bot_token:
        raise ValueError("Не задана переменная DB_MANAGER_SV_TOKEN для Telegram-бота.")
    if not settings.manager_chat_ids:
        raise ValueError("Не задана переменная DATABASE_ACCESS_MANAGER_TELEGRAM_CHAT_IDS.")
    bot = Bot(token=settings.bot_token)
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Открыть меню"),
            BotCommand(command="grant", description="Выдать PostgreSQL-доступ"),
            BotCommand(command="accesses", description="Показать активные доступы"),
            BotCommand(command="revoke", description="Отозвать доступ по номеру"),
            BotCommand(command="delete_user", description="Удалить пользователя по логину"),
        ]
    )
    dispatcher = Dispatcher()
    dispatcher.include_router(create_database_access_router())
    await dispatcher.start_polling(bot)


def main() -> None:
    """Синхронно запускает Telegram-бота из командной строки проекта."""

    asyncio.run(run_telegram_bot())


if __name__ == "__main__":
    main()
