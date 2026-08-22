"""CLI entrypoint'ы Bitrix chat control: sync, daily, weekly и Telegram-бот."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src_oop.core.telegram.config import TelegramCoreSettings
from src_oop.core.telegram.notifier import TelegramNotifier
from src_oop.core.telegram.runner import run_bot
from src_oop.jobs.bitrix_chat_control.analysis_service import BitrixChatAnalysisService
from src_oop.jobs.bitrix_chat_control.config import BitrixChatControlSettings
from src_oop.jobs.bitrix_chat_control.llm_client import BitrixChatLLMClient
from src_oop.jobs.bitrix_chat_control.mcp_client import ReadonlyBitrixMCPClient
from src_oop.jobs.bitrix_chat_control.models import (
    AnalysisRunStatus,
    BitrixMessageInput,
)
from src_oop.jobs.bitrix_chat_control.rest_client import ReadonlyBitrixRESTClient
from src_oop.jobs.bitrix_chat_control.report_service import BitrixChatReportService
from src_oop.jobs.bitrix_chat_control.repository import BitrixChatControlRepository
from src_oop.jobs.bitrix_chat_control.telegram_router import build_bitrix_chat_control_router

logger = logging.getLogger(__name__)
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def bitrix_chat_control_create_tables() -> None:
    """Создаёт таблицы и bootstrap monitored_chats для Bitrix chat control MVP.

    Этот entrypoint запускает первый бизнес-сценарий сервиса: подготовку схемы БД
    и создание начального списка monitored_chats по env без ручной работы в SQL.
    """
    repository = BitrixChatControlRepository()
    settings = BitrixChatControlSettings.from_env()
    repository.create_tables()
    created_count = asyncio.run(refresh_monitored_chats_async())
    logger.info(
        "Инициализация Bitrix chat control завершена | bootstrap_monitored_chats=%s",
        created_count,
    )


async def sync_bitrix_chats_async() -> None:
    """Синхронизирует новые сообщения Bitrix24 и обновляет состояние проблем.

    Полный бизнес-сценарий: bootstrap monitored_chats, получить сообщения только
    из разрешённых read-only Bitrix tools, сохранить их в PostgreSQL без дублей и
    обновить open/resolved-состояние проблем по новым сигналам.
    """
    repository = BitrixChatControlRepository()
    discovered_count = await refresh_monitored_chats_async(repository=repository)
    settings = BitrixChatControlSettings.from_env()
    active_chats = repository.list_active_chats()
    if not active_chats:
        logger.warning(
            "Синхронизация Bitrix-чата пропущена: нет активных monitored_chats. "
            "Проверьте доступ техаккаунта к чатам Bitrix24 или таблицу monitored_chats."
        )
        return
    logger.info(
        "Подготовка monitored_chats завершена | discovered_or_filtered_chats=%s | active_chats=%s",
        discovered_count,
        len(active_chats),
    )
    bitrix_client = _build_runtime_bitrix_client(settings)
    analysis_service = BitrixChatAnalysisService(
        repository=repository,
        llm_client=BitrixChatLLMClient(settings=settings),
    )
    notifier = TelegramNotifier(TelegramCoreSettings.from_env())

    for chat in active_chats:
        run_id = repository.create_analysis_run(chat_id=chat.id, run_type="sync_bitrix_chats")
        try:
            dialog_payload = await bitrix_client.get_dialog(chat.bitrix_dialog_id)
            resolved_name = _extract_dialog_name(dialog_payload) or chat.name
            raw_messages = await bitrix_client.get_dialog_messages(
                chat.bitrix_dialog_id,
                limit=settings.messages_limit,
                last_synced_message_id=chat.last_synced_message_id,
            )
            prepared_messages = _prepare_messages(
                dialog_id=chat.bitrix_dialog_id,
                raw_messages=raw_messages,
            )
            new_messages = repository.save_messages(prepared_messages)
            if new_messages:
                last_message = max(new_messages, key=lambda item: item.message_datetime)
                repository.update_chat_metadata(
                    chat.id,
                    name=resolved_name,
                    last_synced_message_id=last_message.bitrix_message_id,
                    last_synced_at=last_message.message_datetime,
                )
            else:
                repository.update_chat_metadata(chat.id, name=resolved_name)

            outcome = analysis_service.analyze_new_messages(
                chat_id=chat.id,
                new_messages=new_messages,
            )
            repository.finish_analysis_run(
                run_id,
                status=AnalysisRunStatus.SUCCESS,
                messages_scanned=outcome.counters.messages_scanned,
                new_messages_count=outcome.counters.new_messages_count,
                new_problems_count=outcome.counters.new_problems_count,
                updated_problems_count=outcome.counters.updated_problems_count,
                resolved_problems_count=outcome.counters.resolved_problems_count,
            )
            logger.info(
                "Синхронизация Bitrix-чата завершена | chat=%s | new_messages=%s | new_problems=%s | updated_problems=%s | resolved=%s",
                resolved_name,
                outcome.counters.new_messages_count,
                outcome.counters.new_problems_count,
                outcome.counters.updated_problems_count,
                outcome.counters.resolved_problems_count,
            )
        except Exception as error:
            repository.finish_analysis_run(
                run_id,
                status=AnalysisRunStatus.FAILED,
                error_message=f"{type(error).__name__}: {error}",
            )
            logger.exception(
                "Ошибка синхронизации Bitrix-чата | chat_id=%s | error_type=%s",
                chat.bitrix_dialog_id,
                type(error).__name__,
            )
            if settings.admin_notify_enabled:
                await notifier.send_to_service_chats(
                    "Техническая ошибка Bitrix chat control: не удалось синхронизировать "
                    f"чат {chat.name} ({chat.bitrix_dialog_id})."
                )


def sync_bitrix_chats() -> None:
    """Синхронно запускает sync Bitrix-чатов для CLI и будущего планировщика."""
    asyncio.run(sync_bitrix_chats_async())


async def daily_report_async() -> None:
    """Формирует ежедневный краткий отчёт по активным чатам и отправляет в Telegram.

    Бизнес-сценарий daily: показать только новые проблемы за сутки, закрытые
    кейсы и хвост ранее открытых проблем, не повторяя целиком длинную недельную
    историю по каждому чату.
    """
    repository = BitrixChatControlRepository()
    report_service = BitrixChatReportService()
    notifier = TelegramNotifier(TelegramCoreSettings.from_env())
    now = datetime.now(tz=MOSCOW_TZ)
    period_start = now - timedelta(days=1)
    messages: list[str] = []

    for chat in repository.list_active_chats():
        problems = repository.get_problems_for_period(
            chat_id=chat.id,
            period_start=period_start,
            period_end=now,
        )
        summary = report_service.build_chat_summary(
            chat_name=chat.name,
            period_start=period_start,
            period_end=now,
            problems=problems,
        )
        messages.append(report_service.render_summary_text(summary))

    if messages:
        await notifier.send_to_service_chats("\n\n".join(messages))


def daily_report() -> None:
    """Синхронно запускает ежедневный отчёт для CLI и cron-подобных запусков."""
    asyncio.run(daily_report_async())


async def weekly_report_async() -> None:
    """Формирует недельное compact summary по всем активным monitored_chats.

    Бизнес-сценарий weekly: руководитель получает по каждому чату компактный
    блок с проблемами, решёнными кейсами, хвостом незакрытых вопросов и
    коротким итогом без чтения всей переписки за неделю.
    """
    repository = BitrixChatControlRepository()
    report_service = BitrixChatReportService()
    notifier = TelegramNotifier(TelegramCoreSettings.from_env())
    now = datetime.now(tz=MOSCOW_TZ)
    period_start = now - timedelta(days=7)
    messages: list[str] = []

    for chat in repository.list_active_chats():
        problems = repository.get_problems_for_period(
            chat_id=chat.id,
            period_start=period_start,
            period_end=now,
        )
        summary = report_service.build_chat_summary(
            chat_name=chat.name,
            period_start=period_start,
            period_end=now,
            problems=problems,
        )
        messages.append(report_service.render_summary_text(summary))

    if messages:
        await notifier.send_to_service_chats("\n\n".join(messages))


def weekly_report() -> None:
    """Синхронно запускает weekly-отчёт для CLI и будущего планировщика."""
    asyncio.run(weekly_report_async())


async def telegram_bot_async() -> None:
    """Запускает интерактивный Telegram-бот Bitrix chat control на aiogram 3.

    Это основной пользовательский UX-сценарий MVP: руководитель или оператор
    выбирает чат и период кнопками, а бот возвращает компактное управленческое
    саммари без ручного ввода параметров.
    """
    await run_bot(build_bitrix_chat_control_router())


def telegram_bot() -> None:
    """Синхронно запускает Telegram-бота Bitrix chat control из CLI."""
    asyncio.run(telegram_bot_async())


async def refresh_monitored_chats_async(
    repository: BitrixChatControlRepository | None = None,
    settings: BitrixChatControlSettings | None = None,
) -> int:
    """Обновляет monitored_chats из Bitrix discovery и env-фильтра по требованию.

    Бизнес-смысл helper'а в том, чтобы сервис по запросу или перед плановым
    sync автоматически подхватывал все рабочие групповые чаты техаккаунта.
    `BITRIX_CHAT_IDS` при этом остаётся опциональным фильтром, если нужно
    ограничить мониторинг конкретным поднабором чатов.
    """
    repository = repository or BitrixChatControlRepository()
    settings = settings or BitrixChatControlSettings.from_env()
    repository.create_tables()
    if not settings.chat_discovery_enabled:
        return repository.ensure_monitored_chats(settings.monitored_dialog_ids)

    bitrix_client = _build_runtime_bitrix_client(settings)
    dialog_filter = (
        frozenset(settings.monitored_dialog_ids) if settings.monitored_dialog_ids else None
    )
    discovered_chats = await bitrix_client.list_work_chats(
        page_limit=settings.chat_discovery_page_limit,
        dialog_ids_filter=dialog_filter,
    )
    if not discovered_chats and settings.monitored_dialog_ids:
        logger.warning(
            "Bitrix discovery не вернул чаты по заданному фильтру BITRIX_CHAT_IDS. "
            "Будет использован fallback по env-списку без проверки названий."
        )
        return repository.ensure_monitored_chats(settings.monitored_dialog_ids)
    return repository.sync_monitored_chats(discovered_chats)


def _extract_dialog_name(dialog_payload) -> str | None:
    """Пытается безопасно извлечь человекочитаемое имя чата из ответа Bitrix.

    Форматы payload у разных MCP-обвязок могут немного отличаться, поэтому
    helper бережно обходит несколько ожидаемых схем и не падает на частично
    заполненных ответах.
    """
    if isinstance(dialog_payload, dict):
        for key in ("name", "title", "dialog_name"):
            value = dialog_payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for nested_key in ("result", "dialog", "data"):
            nested = dialog_payload.get(nested_key)
            if isinstance(nested, dict):
                nested_name = _extract_dialog_name(nested)
                if nested_name:
                    return nested_name
    return None


def _prepare_messages(dialog_id: str, raw_messages) -> list[BitrixMessageInput]:
    """Нормализует ответ Bitrix messages tool к списку BitrixMessageInput.

    Этот helper защищает остальной код от разницы форматов MCP-ответов. Он
    старается извлечь список сообщений из типичных полей `messages`, `items`,
    `result`, `data` и привести их к единой структуре перед upsert в БД.
    """
    payload_items = _extract_message_items(raw_messages)
    prepared: list[BitrixMessageInput] = []
    for item in payload_items:
        if not isinstance(item, dict):
            continue
        message_id = _pick_first_str(item, "id", "message_id", "messageId")
        text = _pick_first_str(item, "text", "message", "body")
        if not message_id or not text:
            continue
        author_id = _pick_first_str(item, "author_id", "authorId", "user_id", "userId")
        author_name = _pick_first_str(item, "author_name", "authorName", "user_name", "userName") or (
            author_id or "Неизвестный автор"
        )
        date_value = item.get("date") or item.get("created_at") or item.get("datetime")
        message_datetime = _coerce_datetime(date_value)
        if message_datetime is None:
            continue
        prepared.append(
            BitrixMessageInput(
                bitrix_message_id=message_id,
                dialog_id=dialog_id,
                author_id=author_id,
                author_name=author_name,
                message_text=text,
                message_datetime=message_datetime,
                raw_payload_json=item,
            )
        )
    return prepared


def _extract_message_items(payload) -> list[dict]:
    """Извлекает список сообщений из разных форматов ответа Bitrix/MCP.

    Это нужно для MVP-совместимости с разными обёртками MCP: одни возвращают
    сообщения в `messages`, другие в `items`, а третьи заворачивают ответ в
    `result` или `data`.
    """
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("messages", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    for key in ("result", "data"):
        nested = payload.get(key)
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        if isinstance(nested, dict):
            extracted = _extract_message_items(nested)
            if extracted:
                return extracted
    return []


def _pick_first_str(item: dict, *keys: str) -> str | None:
    """Возвращает первое непустое строковое значение из набора альтернативных ключей.

    Helper нужен для безопасной нормализации внешнего payload, где один и тот же
    смысловой атрибут может называться по-разному в зависимости от обвязки API.
    """
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        prepared = str(value).strip()
        if prepared:
            return prepared
    return None


def _coerce_datetime(value) -> datetime | None:
    """Преобразует строку или datetime из Bitrix payload в timezone-aware дату.

    Это защищает хронологию проблем от неоднозначных дат: если Bitrix прислал
    строку без timezone, для MVP мы трактуем её как московское время.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=MOSCOW_TZ)
        return value
    if not value:
        return None
    raw_value = str(value).strip()
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=MOSCOW_TZ)
    return parsed


def _build_runtime_bitrix_client(settings: BitrixChatControlSettings):
    """Выбирает транспорт Bitrix для боевого рантайма по настройкам окружения.

    По умолчанию сервис использует официальный Bitrix REST, потому что он
    пригоден для CLI и планировщика. MCP сохраняется как вспомогательный режим
    для ручной диагностики и локальных экспериментов внутри Codex.
    """
    if settings.runtime_transport == "rest":
        return ReadonlyBitrixRESTClient(settings=settings)
    if settings.runtime_transport == "mcp":
        return ReadonlyBitrixMCPClient(settings=settings)
    raise ValueError(
        "BITRIX_RUNTIME_TRANSPORT должен быть `rest` или `mcp`."
    )
