from __future__ import annotations

import asyncio
import logging

from src_oop.core.telegram.runner import run_bot
from src_oop.jobs.add_new_items.repository import AddNewItemsRepository
from src_oop.jobs.add_new_items.service import AddNewItemsService
from src_oop.jobs.add_new_items.telegram_config import AddNewItemsTelegramSettings
from src_oop.jobs.add_new_items.telegram_router import build_add_new_items_router

logger = logging.getLogger(__name__)


def add_new_items_run() -> None:
    """Запускает основной перенос новых товаров в рабочие таблицы.

    Это главный бизнес-сценарий job: строки со статусом добавления читаются из
    исходной таблицы, затем данные переносятся в UNIT, Автопилот, products и
    служебные флаги в исходнике обновляются по итогам выполнения.
    """

    logger.info("Инициализируем job add_new_items.")
    service = AddNewItemsService(repository=AddNewItemsRepository())
    service.run()


async def _configure_bot_menu(bot, settings: AddNewItemsTelegramSettings) -> None:
    """Настраивает меню команд Telegram для группы add_new_items.

    Этот шаг улучшает пользовательский сценарий Telegram-запуска: сотрудникам
    не нужно помнить текст команд вручную, потому что Telegram показывает
    доступные действия бота как пункты меню прямо в интерфейсе группы.
    """
    from aiogram.types import BotCommand, BotCommandScopeChat

    commands = [
        BotCommand(command="start", description="Показать справку по боту"),
        BotCommand(command="help", description="Показать список доступных команд"),
        BotCommand(command="status", description="Показать статус текущего запуска"),
        BotCommand(command="last_result", description="Показать итог последнего запуска"),
        BotCommand(command="run_add_new_items", description="Запустить перенос новых товаров"),
    ]

    for chat_id in settings.allowed_chat_ids:
        await bot.set_my_commands(
            commands=commands,
            scope=BotCommandScopeChat(chat_id=chat_id),
        )
        logger.info(
            "Меню команд Telegram настроено для add_new_items | chat_id=%s | commands=%s",
            chat_id,
            len(commands),
        )


async def add_new_items_telegram_bot_async() -> None:
    """Запускает Telegram-бота как дополнительный канал работы job add_new_items.

    Это пользовательский сценарий для рабочей группы: бот принимает команду,
    запускает серверный перенос новых товаров и сообщает в чат итог выполнения
    без участия оператора в консоли сервера.
    """
    settings = AddNewItemsTelegramSettings.from_env()
    if not settings.bot_token:
        raise RuntimeError(
            "Невозможно запустить Telegram-бот add_new_items: не задан ADD_NEW_ITEMS_TELEGRAM_BOT_TOKEN."
        )

    await run_bot(
        build_add_new_items_router(),
        bot_token=settings.bot_token,
        on_startup=lambda bot: _configure_bot_menu(bot, settings),
    )


def add_new_items_telegram_bot() -> None:
    """Синхронно запускает Telegram-бота как часть job add_new_items.

    Entrypoint нужен для штатного серверного запуска через `main.py`, чтобы
    Telegram-бот жил рядом с основным сценарием add_new_items и не создавал
    отдельный верхнеуровневый job-пакет в проекте.
    """
    asyncio.run(add_new_items_telegram_bot_async())
