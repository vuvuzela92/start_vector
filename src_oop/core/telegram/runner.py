"""Общий запуск Telegram-ботов на базе aiogram 3."""

from __future__ import annotations

from src_oop.core.telegram.config import TelegramCoreSettings


async def run_bot(router) -> None:
    """Запускает aiogram-бота с переданным router и общими настройками проекта.

    Этот entrypoint обслуживает все будущие Telegram-модули проекта. Он держит
    единое правило запуска и проверки обязательного токена, а бизнес-роуты
    передаются снаружи, чтобы `core` не зависел от конкретных jobs.
    """
    from aiogram import Bot, Dispatcher

    settings = TelegramCoreSettings.from_env()
    if not settings.bot_token:
        raise RuntimeError(
            "Невозможно запустить Telegram-бот: не задан TELEGRAM_BOT_TOKEN."
        )

    bot = Bot(token=settings.bot_token)
    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    await dispatcher.start_polling(bot)
