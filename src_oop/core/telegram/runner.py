"""Общий запуск Telegram-ботов на базе aiogram 3."""

from __future__ import annotations

from src_oop.core.telegram.config import TelegramCoreSettings


async def run_bot(router, *, bot_token: str | None = None, on_startup=None) -> None:
    """Запускает aiogram-бота с переданным router и токеном конкретного модуля.

    Этот entrypoint обслуживает все Telegram-модули проекта и сохраняет
    совместимость со старыми ботами. Новый модуль может передать собственный
    `bot_token`, а старые сценарии продолжают работать через общий
    `TelegramCoreSettings`, пока не будут отдельно переведены на модульную
    конфигурацию. Необязательный `on_startup` позволяет модулю настроить
    меню команд или другой Telegram-UI до начала polling без дублирования
    общей механики запуска.
    """
    from aiogram import Bot, Dispatcher

    resolved_bot_token = bot_token
    if not resolved_bot_token:
        settings = TelegramCoreSettings.from_env()
        resolved_bot_token = settings.bot_token

    if not resolved_bot_token:
        raise RuntimeError(
            "Невозможно запустить Telegram-бот: не задан TELEGRAM_BOT_TOKEN."
        )

    bot = Bot(token=resolved_bot_token)
    if on_startup is not None:
        await on_startup(bot)
    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    await dispatcher.start_polling(bot)
