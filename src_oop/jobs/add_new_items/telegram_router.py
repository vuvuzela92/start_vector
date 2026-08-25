"""Telegram-router для запуска job add_new_items из рабочей группы."""

from __future__ import annotations

import logging

from src_oop.core.telegram.access import TelegramActor, is_actor_allowed
from src_oop.core.telegram.keyboards import build_commands_reply_keyboard
from src_oop.jobs.add_new_items.telegram_config import AddNewItemsTelegramSettings
from src_oop.jobs.add_new_items.telegram_models import TelegramLaunchActor
from src_oop.jobs.add_new_items.telegram_service import AddNewItemsTelegramService

logger = logging.getLogger(__name__)

BUTTON_RUN_ADD_NEW_ITEMS = "Запустить добавление"
BUTTON_STATUS = "Показать статус"
BUTTON_LAST_RESULT = "Последний результат"
BUTTON_HELP = "Справка"


def build_add_new_items_router():
    """Создаёт router Telegram-бота как часть job add_new_items.

    Router обслуживает дополнительный пользовательский канал того же
    бизнес-сценария: безопасный запуск `add_new_items_run` из разрешённой
    рабочей группы с понятным статусом и итоговым сообщением в Telegram.
    """
    from aiogram import Bot, F, Router
    from aiogram.types import Message

    router = Router(name="add_new_items_telegram")
    settings = AddNewItemsTelegramSettings.from_env()
    service = AddNewItemsTelegramService(settings=settings)
    commands_keyboard = build_commands_reply_keyboard(
        (BUTTON_RUN_ADD_NEW_ITEMS, BUTTON_STATUS),
        (BUTTON_LAST_RESULT, BUTTON_HELP),
    )

    async def _is_message_allowed(message: Message, bot: Bot) -> bool:
        """Проверяет доступ отправителя по группе и опциональному списку пользователей.

        Бизнес-правило для этого бота такое: если список пользователей не задан,
        доступ получают все сообщения из разрешённой группы. Если список задан,
        он дополнительно сужает круг доступа для запуска переноса товаров.
        """
        actor = TelegramActor(
            user_id=getattr(message.from_user, "id", None),
            chat_id=getattr(message.chat, "id", None),
        )
        if not is_actor_allowed(
            actor,
            allowed_user_ids=settings.allowed_user_ids,
            allowed_chat_ids=settings.allowed_chat_ids,
        ):
            return False

        if actor.user_id is None or actor.chat_id is None:
            return False

        if not settings.allowed_user_ids:
            logger.info(
                "Доступ к add_new_items подтвержден по chat_id без дополнительного запроса в Telegram API | chat_id=%s | user_id=%s",
                actor.chat_id,
                actor.user_id,
            )
            return True

        try:
            member = await bot.get_chat_member(chat_id=actor.chat_id, user_id=actor.user_id)
        except Exception as error:
            logger.error(
                "Не удалось проверить членство пользователя в группе add_new_items | chat_id=%s | user_id=%s | error_type=%s",
                actor.chat_id,
                actor.user_id,
                type(error).__name__,
            )
            return False

        return getattr(member, "status", "") not in {"left", "kicked"}

    def _build_actor(message: Message) -> TelegramLaunchActor:
        """Собирает человекочитаемые данные автора команды для статусов и логов.

        Боту важно показывать в группе не только `user_id`, но и понятное имя,
        чтобы коллеги сразу видели, кто именно запустил перенос новых товаров.
        """
        full_name = (
            message.from_user.full_name if message.from_user else "Неизвестный пользователь"
        )
        username = getattr(message.from_user, "username", None)
        display_name = f"{full_name} (@{username})" if username else full_name
        return TelegramLaunchActor(
            user_id=getattr(message.from_user, "id", None),
            display_name=display_name,
        )

    def _help_text() -> str:
        """Возвращает краткую справку по доступным командам Telegram-бота.

        Справка нужна для быстрого старта в группе: участник должен сразу понять,
        как запустить add_new_items и где посмотреть результат предыдущего запуска.
        """
        return "\n".join(
            [
                "Бот запуска add_new_items готов к работе.",
                "",
                "Кнопки внизу экрана:",
                f"{BUTTON_RUN_ADD_NEW_ITEMS} - запустить перенос новых товаров",
                f"{BUTTON_STATUS} - показать текущий статус запуска",
                f"{BUTTON_LAST_RESULT} - показать итог последнего завершенного запуска",
                f"{BUTTON_HELP} - показать эту справку",
                "",
                "Доступные команды:",
                "/run_add_new_items - запустить перенос новых товаров",
                "/status - показать текущий статус запуска",
                "/last_result - показать итог последнего завершенного запуска",
                "/help - показать эту справку",
            ]
        )

    def _extract_command_name(message: Message) -> str | None:
        """Извлекает имя slash-команды из текста сообщения Telegram.

        Telegram в группах часто присылает команды в форме `/status@botname`.
        Helper нормализует такой формат и возвращает только имя команды без
        слеша и без упоминания, чтобы вся маршрутизация шла по одному правилу.
        """
        text = (message.text or "").strip()
        button_mapping = {
            BUTTON_RUN_ADD_NEW_ITEMS: "run_add_new_items",
            BUTTON_STATUS: "status",
            BUTTON_LAST_RESULT: "last_result",
            BUTTON_HELP: "help",
        }
        mapped_command = button_mapping.get(text)
        if mapped_command is not None:
            return mapped_command

        if not text.startswith("/"):
            return None

        first_token = text.split(maxsplit=1)[0]
        command_token = first_token[1:]
        return command_token.split("@", maxsplit=1)[0]

    async def _log_incoming_message(message: Message, *, handler_name: str) -> None:
        """Логирует входящее сообщение для диагностики поведения бота в группе.

        Этот helper нужен для операционного этапа: пока Telegram-канал только
        вводится в эксплуатацию, важно видеть, какую команду Telegram реально
        передал в handler и из какого чата она пришла.
        """
        logger.info(
            "Получено Telegram-сообщение для add_new_items | handler=%s | chat_id=%s | user_id=%s | text=%s",
            handler_name,
            getattr(message.chat, "id", None),
            getattr(getattr(message, "from_user", None), "id", None),
            getattr(message, "text", None),
        )

    @router.message(F.text)
    async def command_entry(message: Message, bot: Bot) -> None:
        """Маршрутизирует все текстовые команды бота по единому правилу.

        Этот единый handler защищает от коллизий старого поведения бота: каждая
        команда разбирается в одном месте, поэтому `/status`, `/help`, кнопки
        и запуск job в группе работают одинаково предсказуемо.
        """
        command_name = _extract_command_name(message)
        if command_name is None:
            return

        await _log_incoming_message(message, handler_name=f"command_entry:{command_name}")
        if not await _is_message_allowed(message, bot):
            await message.answer(
                "Доступ к запуску add_new_items из этого чата запрещён.",
                reply_markup=commands_keyboard,
            )
            return

        if command_name in {"start", "help"}:
            await message.answer(
                _help_text(),
                disable_web_page_preview=True,
                reply_markup=commands_keyboard,
            )
            return

        if command_name == "status":
            await message.answer(
                await service.get_status_text(),
                disable_web_page_preview=True,
                reply_markup=commands_keyboard,
            )
            return

        if command_name == "last_result":
            await message.answer(
                await service.get_last_result_text(),
                disable_web_page_preview=True,
                reply_markup=commands_keyboard,
            )
            return

        if command_name == "run_add_new_items":
            actor = _build_actor(message)
            text = await service.start_run(
                bot=bot,
                actor=actor,
                chat_id=message.chat.id,
            )
            await message.answer(
                text,
                disable_web_page_preview=True,
                reply_markup=commands_keyboard,
            )
            return

        await message.answer(
            "Команда не распознана.\n\n"
            "Используйте одну из команд:\n"
            "/start\n"
            "/help\n"
            "/run_add_new_items\n"
            "/status\n"
            "/last_result",
            disable_web_page_preview=True,
            reply_markup=commands_keyboard,
        )

    return router
