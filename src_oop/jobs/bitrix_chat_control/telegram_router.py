"""Telegram-router сервиса контроля рабочих чатов Bitrix24."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src_oop.core.telegram.access import TelegramActor, is_actor_allowed
from src_oop.core.telegram.config import TelegramCoreSettings
from src_oop.core.telegram.keyboards import build_back_keyboard
from src_oop.jobs.bitrix_chat_control.report_service import BitrixChatReportService
from src_oop.jobs.bitrix_chat_control.repository import BitrixChatControlRepository

MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def build_bitrix_chat_control_router():
    """Создаёт aiogram-router для `/summary`, `/chats` и служебных команд.

    Router держит только интерактивный UX конкретного Bitrix-модуля. Общие
    правила Telegram-доступа и базовый запуск бота вынесены в `src_oop/core`,
    чтобы их могли переиспользовать и другие будущие jobs.
    """
    from aiogram import F, Router
    from aiogram.filters import Command
    from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

    router = Router(name="bitrix_chat_control")
    repository = BitrixChatControlRepository()
    report_service = BitrixChatReportService()
    settings = TelegramCoreSettings.from_env()

    def _allowed(message_or_callback) -> bool:
        actor = TelegramActor(
            user_id=getattr(getattr(message_or_callback, "from_user", None), "id", None),
            chat_id=getattr(getattr(message_or_callback, "message", message_or_callback).chat, "id", None),
        )
        return is_actor_allowed(
            actor,
            allowed_user_ids=settings.allowed_user_ids,
            allowed_chat_ids=settings.allowed_chat_ids,
        )

    def _chat_keyboard(prefix: str) -> InlineKeyboardMarkup:
        """Строит inline-кнопки выбора чата для команд отчётности.

        Кнопки позволяют не вводить идентификаторы вручную и уменьшают риск
        ошибки в группе, где пользователю важно быстро выбрать нужный чат по
        человекочитаемому имени.
        """
        chats = repository.list_active_chats()
        buttons = [
            [InlineKeyboardButton(text=chat.name, callback_data=f"{prefix}:{chat.id}")]
            for chat in chats
        ]
        return InlineKeyboardMarkup(inline_keyboard=buttons)

    def _period_keyboard(prefix: str, chat_id: int) -> InlineKeyboardMarkup:
        """Строит inline-кнопки выбора периода для команды `/summary`.

        Этот helper поддерживает основной пользовательский сценарий: после
        выбора чата бот сразу предлагает понятные готовые периоды без ручного
        ввода.
        """
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="1 день", callback_data=f"{prefix}:{chat_id}:1"),
                    InlineKeyboardButton(text="7 дней", callback_data=f"{prefix}:{chat_id}:7"),
                ],
                [
                    InlineKeyboardButton(text="14 дней", callback_data=f"{prefix}:{chat_id}:14"),
                    InlineKeyboardButton(text="30 дней", callback_data=f"{prefix}:{chat_id}:30"),
                ],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:chats")],
            ]
        )

    def _command_catalog_text() -> str:
        """Возвращает компактную справку по основным командам бота.

        Справка нужна для первого касания в группе: пользователь должен сразу
        понять, как получить summary и как проверить список отслеживаемых чатов.
        """
        return "\n".join(
            [
                "Бот саммари Bitrix24 готов к работе.",
                "",
                "Основные команды:",
                "/chats - список отслеживаемых чатов",
                "/summary - итог по чату за период",
                "/refresh_chats - обновить список чатов из Bitrix24",
                "",
                "Для работы в группе можно использовать команды с @start_wb_bot.",
            ]
        )

    def _render_chats_text() -> str:
        """Форматирует список monitored_chats для команды `/chats`.

        Этот экран выполняет роль справочника и должен быть коротким, но
        информативным: пользователь сразу видит количество чатов и их названия.
        """
        chats = repository.list_active_chats()
        if not chats:
            return "Активные чаты для мониторинга пока не настроены."
        lines = [f"Отслеживаемые чаты: {len(chats)}", ""]
        lines.extend(f"{index}. {chat.name}" for index, chat in enumerate(chats, start=1))
        lines.extend(
            [
                "",
                "Подсказка:",
                "Используйте /summary, чтобы выбрать чат и получить отчёт.",
            ]
        )
        return "\n".join(lines)

    def _refresh_result_text(*, refreshed_count: int) -> str:
        """Собирает человекочитаемый итог ручного обновления списка чатов.

        После `/refresh_chats` пользователю важно не только знать, что запрос
        завершён, но и увидеть, сколько чатов подтверждено и сколько их всего
        доступно в локальном мониторинге.
        """
        chats = repository.list_active_chats()
        lines = [
            "Список чатов обновлён.",
            "",
            f"Подтверждено чатов из Bitrix24: {refreshed_count}",
            f"Активных monitored_chats сейчас: {len(chats)}",
        ]
        if chats:
            preview = ", ".join(chat.name for chat in chats[:5])
            suffix = "..." if len(chats) > 5 else ""
            lines.extend(["", f"Примеры чатов: {preview}{suffix}"])
        return "\n".join(lines)

    @router.message(Command("summary"))
    async def summary_entry(message: Message) -> None:
        """Запускает выбор чата для ручного получения compact summary.

        Бизнес-сценарий: пользователь не вводит chat id руками, а выбирает нужный
        чат кнопкой, чтобы сократить ошибки и снизить порог использования бота.
        """
        if not _allowed(message):
            await message.answer("Доступ к корпоративным саммари запрещён.")
            return
        await message.answer(
            "Выберите чат для summary:",
            reply_markup=_chat_keyboard("summary_chat"),
        )

    @router.message(Command("start"))
    @router.message(Command("help"))
    async def start_entry(message: Message) -> None:
        """Показывает список доступных команд при первом обращении к боту.

        Этот сценарий нужен для запуска из группы: пользователь часто начинает с
        `/start@botname`, и бот должен сразу объяснить, какие команды доступны
        для просмотра саммари и списка отслеживаемых чатов.
        """
        if not _allowed(message):
            await message.answer("Доступ к корпоративным саммари запрещён.")
            return
        await message.answer(_command_catalog_text())

    @router.message(Command("chats"))
    async def chats_entry(message: Message) -> None:
        """Показывает список активных monitored_chats без дополнительных действий.

        Команда нужна как быстрый справочник для оператора: видно, какие чаты уже
        поставлены на мониторинг и какие названия сейчас отображаются в боте.
        """
        if not _allowed(message):
            await message.answer("Доступ к корпоративным саммари запрещён.")
            return
        await message.answer(_render_chats_text())

    @router.message(Command("refresh_chats"))
    async def refresh_chats_entry(message: Message) -> None:
        """Принудительно обновляет monitored_chats из Bitrix прямо из Telegram.

        Команда нужна для операционного сценария: техпользователь добавлен в
        новый чат, и руководителю не хочется ждать следующего планового sync.
        Бот сразу перечитывает доступные чаты из Bitrix и обновляет локальный
        список, после чего новые чаты становятся доступны в `/chats` и
        `/summary`.
        """
        if not _allowed(message):
            await message.answer("Доступ к корпоративным саммари запрещён.")
            return
        from src_oop.jobs.bitrix_chat_control.run import refresh_monitored_chats_async

        await message.answer("Обновляю список чатов из Bitrix24. Это может занять несколько секунд...")
        refreshed_count = await refresh_monitored_chats_async(repository=repository)
        await message.answer(_refresh_result_text(refreshed_count=refreshed_count))

    @router.callback_query(F.data == "back:chats")
    async def back_to_chats(callback: CallbackQuery) -> None:
        """Возвращает пользователя к выбору чата из inline-навигации.

        Это поддерживает основной UX-путь бота: всегда можно вернуться назад без
        повторного ввода команды и без потери контекста навигации.
        """
        if not _allowed(callback):
            await callback.answer("Доступ запрещён.", show_alert=True)
            return
        await callback.message.edit_text(
            "Выберите чат для summary:",
            reply_markup=_chat_keyboard("summary_chat"),
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("summary_chat:"))
    async def summary_choose_period(callback: CallbackQuery) -> None:
        """Показывает выбор периода после выбора чата для `/summary`.

        Шаг отделён от формирования отчёта, потому что пользователь выбирает не
        только чат, но и глубину истории: 1, 7, 14 или 30 дней.
        """
        if not _allowed(callback):
            await callback.answer("Доступ запрещён.", show_alert=True)
            return
        chat_id = int(callback.data.split(":")[1])
        await callback.message.edit_text(
            "Выберите период для summary:",
            reply_markup=_period_keyboard("summary_period", chat_id),
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("summary_period:"))
    async def summary_report(callback: CallbackQuery) -> None:
        """Формирует compact summary по выбранному чату и периоду.

        Финальный шаг ручного сценария использует уже reconciled-состояние БД,
        чтобы пользователь получил управленческий итог, а не сырую переписку.
        """
        if not _allowed(callback):
            await callback.answer("Доступ запрещён.", show_alert=True)
            return
        _, chat_id_raw, days_raw = callback.data.split(":")
        await _render_period_summary(
            callback=callback,
            chat_id=int(chat_id_raw),
            days=int(days_raw),
            report_service=report_service,
            repository=repository,
        )

    async def _render_period_summary(
        *,
        callback: CallbackQuery,
        chat_id: int,
        days: int,
        report_service: BitrixChatReportService,
        repository: BitrixChatControlRepository,
    ) -> None:
        """Строит итоговое summary по выбранному чату и периоду.

        Helper держит единое правило расчёта периода и чтения проблем из БД,
        чтобы ручной Telegram-сценарий выдавал тот же управленческий итог, что и
        плановые отчёты модуля.
        """
        chat = repository.get_chat_by_id(chat_id)
        if chat is None:
            await callback.answer("Чат не найден.", show_alert=True)
            return
        period_end = datetime.now(tz=MOSCOW_TZ)
        period_start = period_end - timedelta(days=days)
        problems = repository.get_problems_for_period(
            chat_id=chat_id,
            period_start=period_start,
            period_end=period_end,
        )
        summary = report_service.build_chat_summary(
            chat_name=chat.name,
            period_start=period_start,
            period_end=period_end,
            problems=problems,
        )
        text = report_service.render_summary_text(summary)
        await callback.message.edit_text(
            text,
            reply_markup=build_back_keyboard("back:chats"),
        )
        await callback.answer()

    return router
