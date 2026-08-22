"""Конфигурация сервиса контроля рабочих чатов Bitrix24."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

BITRIX_ALLOWED_MCP_TOOLS: frozenset[str] = frozenset(
    {
        "chat_recent_list",
        "chat_recent_get",
        "chat_dialog_get",
        "chat_dialog_users_list",
        "chat_dialog_messages_get",
        "chat_user_list",
    }
)


def _parse_dialog_ids(raw_value: str | None) -> tuple[str, ...]:
    """Разбирает список Bitrix dialog id из env для bootstrap monitored_chats.

    Этот helper обслуживает первичную настройку MVP: оператор может быстро
    включить наблюдение за нужными чатами через env, а приложение создаст или
    актуализирует записи monitored_chats без ручных SQL-операций.
    """
    if not raw_value:
        return ()
    return tuple(
        item.strip()
        for item in raw_value.replace(";", ",").split(",")
        if item.strip()
    )


def _parse_headers(raw_value: str | None) -> dict[str, str]:
    """Читает дополнительные HTTP-заголовки для MCP без захардкоженных секретов.

    Бизнес-сценарий: разные установки mcp-router могут требовать `Authorization`
    или другие заголовки. Они должны задаваться только через env и не попадать в
    кодовую базу.
    """
    if not raw_value:
        return {}
    payload = json.loads(raw_value)
    if not isinstance(payload, dict):
        raise ValueError("BITRIX_MCP_HEADERS_JSON должен содержать JSON-объект.")
    return {str(key): str(value) for key, value in payload.items()}


@dataclass(frozen=True, slots=True)
class BitrixChatControlSettings:
    """Хранит настройки Bitrix/MCP, анализа и расписаний для chat-control сервиса.

    Этот объект связывает бизнес-сценарий Bitrix -> PostgreSQL -> Telegram в
    одном месте, чтобы jobs, бот и аналитика использовали одинаковую
    конфигурацию и одинаковые ограничения безопасности.
    """

    runtime_transport: str
    mcp_url: str
    mcp_protocol_version: str
    mcp_headers: dict[str, str]
    mcp_timeout_seconds: int
    rest_portal_url: str
    rest_auth_mode: str
    rest_webhook_user_id: str
    rest_webhook_token: str
    rest_access_token: str
    rest_request_timeout_seconds: int
    rest_max_retries: int
    monitored_dialog_ids: tuple[str, ...]
    chat_discovery_enabled: bool
    chat_discovery_page_limit: int
    messages_limit: int
    analysis_model: str
    daily_report_hour_moscow: int
    weekly_report_weekday: int
    admin_notify_enabled: bool

    @classmethod
    def from_env(cls) -> "BitrixChatControlSettings":
        """Собирает настройки сервиса Bitrix chat control из env.

        Метод обслуживает все entrypoint'ы нового контура. Если часть настроек не
        задана, используются безопасные значения по умолчанию, не нарушающие
        read-only режим работы с Bitrix24.
        """
        mcp_headers = _parse_headers(os.getenv("BITRIX_MCP_HEADERS_JSON"))
        auth_token = os.getenv("BITRIX_MCP_AUTH_TOKEN", "").strip()
        if auth_token and "Authorization" not in mcp_headers:
            mcp_headers["Authorization"] = f"Bearer {auth_token}"
        return cls(
            runtime_transport=os.getenv("BITRIX_RUNTIME_TRANSPORT", "rest").strip().lower(),
            mcp_url=os.getenv("BITRIX_MCP_URL", "https://b24.mcp-router.ru/mcp").strip(),
            mcp_protocol_version=os.getenv(
                "BITRIX_MCP_PROTOCOL_VERSION",
                "2026-07-28",
            ).strip(),
            mcp_headers=mcp_headers,
            mcp_timeout_seconds=int(os.getenv("BITRIX_MCP_TIMEOUT_SECONDS", "30")),
            rest_portal_url=os.getenv("BITRIX_REST_PORTAL_URL", "").strip(),
            rest_auth_mode=os.getenv("BITRIX_REST_AUTH_MODE", "webhook").strip().lower(),
            rest_webhook_user_id=os.getenv("BITRIX_REST_WEBHOOK_USER_ID", "").strip(),
            rest_webhook_token=os.getenv("BITRIX_REST_WEBHOOK_TOKEN", "").strip(),
            rest_access_token=os.getenv("BITRIX_REST_ACCESS_TOKEN", "").strip(),
            rest_request_timeout_seconds=int(
                os.getenv("BITRIX_REST_REQUEST_TIMEOUT_SECONDS", "30")
            ),
            rest_max_retries=int(os.getenv("BITRIX_REST_MAX_RETRIES", "3")),
            monitored_dialog_ids=_parse_dialog_ids(os.getenv("BITRIX_CHAT_IDS")),
            chat_discovery_enabled=os.getenv(
                "BITRIX_CHAT_DISCOVERY_ENABLED",
                "true",
            ).strip().lower()
            in {"1", "true", "yes"},
            chat_discovery_page_limit=int(
                os.getenv("BITRIX_CHAT_DISCOVERY_PAGE_LIMIT", "200")
            ),
            messages_limit=int(os.getenv("BITRIX_MESSAGES_LIMIT", "200")),
            analysis_model=os.getenv("BITRIX_ANALYSIS_MODEL", "gpt-4.1-mini").strip(),
            daily_report_hour_moscow=int(
                os.getenv("BITRIX_DAILY_REPORT_HOUR_MOSCOW", "9")
            ),
            weekly_report_weekday=int(os.getenv("BITRIX_WEEKLY_REPORT_WEEKDAY", "0")),
            admin_notify_enabled=os.getenv(
                "BITRIX_ADMIN_NOTIFY_ENABLED",
                "true",
            ).strip().lower()
            in {"1", "true", "yes"},
        )
