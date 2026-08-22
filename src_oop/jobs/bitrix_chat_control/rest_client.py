"""Боевой read-only клиент Bitrix24 REST для мониторинга рабочих чатов."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import aiohttp
from src_oop.jobs.bitrix_chat_control.config import BitrixChatControlSettings

logger = logging.getLogger(__name__)


class BitrixRESTReadonlyError(RuntimeError):
    """Сигнализирует об ошибке read-only интеграции с Bitrix24 REST."""


@dataclass(frozen=True, slots=True)
class BitrixRESTRequestContext:
    """Описывает подготовленный способ авторизации к Bitrix24 REST.

    Структура нужна, чтобы боевой клиент единообразно строил URL и тело запроса
    как для входящего вебхука, так и для OAuth token-а, не размазывая правила
    авторизации по каждому отдельному методу.
    """

    endpoint_url: str
    auth_mode: str


class ReadonlyBitrixRESTClient:
    """Читает рабочие чаты Bitrix24 через официальный REST API.

    Этот клиент предназначен для боевого рантайма сервиса. В отличие от MCP, он
    не зависит от интерактивной сессии Codex и может стабильно работать в CLI,
    планировщике и серверном окружении.
    """

    def __init__(self, settings: BitrixChatControlSettings | None = None) -> None:
        """Подготавливает REST-настройки и проверяет базовую конфигурацию."""
        self.settings = settings or BitrixChatControlSettings.from_env()
        self._request_context = self._build_request_context()

    async def list_work_chats(
        self,
        *,
        page_limit: int,
        dialog_ids_filter: frozenset[str] | None = None,
    ) -> list[tuple[str, str]]:
        """Возвращает групповые чаты, доступные техническому пользователю.

        Бизнес-правило такое же, как в discovery-слое для MCP: в мониторинг
        попадают только рабочие чаты `chat...` или `sg...`, а личные диалоги
        сотрудников сознательно исключаются.
        """
        offset = 0
        discovered: dict[str, str] = {}

        while True:
            payload = await self._call_method(
                "im.recent.list",
                {
                    "SKIP_OPENLINES": "Y",
                    "SKIP_DIALOG": "Y",
                    "SKIP_CHAT": "N",
                    "PARSE_TEXT": "N",
                    "GET_ORIGINAL_TEXT": "N",
                    "OFFSET": offset,
                    "LIMIT": page_limit,
                },
            )
            items = self._extract_list_items(payload)
            for item in items:
                dialog_id = self._extract_recent_dialog_id(item)
                if not dialog_id:
                    continue
                if dialog_ids_filter and dialog_id not in dialog_ids_filter:
                    continue
                discovered[dialog_id] = self._extract_recent_dialog_name(item, dialog_id)

            if len(items) < page_limit:
                break
            offset += page_limit

        return sorted(discovered.items(), key=lambda item: item[1].lower())

    async def get_dialog(self, dialog_id: str) -> Any:
        """Получает карточку чата для имени и технических атрибутов."""
        return await self._call_method("im.dialog.get", {"DIALOG_ID": dialog_id})

    async def get_dialog_users(self, dialog_id: str) -> Any:
        """Получает участников чата для будущих сценариев explainability."""
        return await self._call_method(
            "im.dialog.users.list",
            {
                "DIALOG_ID": dialog_id,
                "SKIP_EXTERNAL": "Y",
                "LIMIT": 200,
                "OFFSET": 0,
            },
        )

    async def get_dialog_messages(
        self,
        dialog_id: str,
        *,
        limit: int,
        last_synced_message_id: str | None = None,
    ) -> Any:
        """Получает сообщения диалога, предпочитая инкрементальную загрузку.

        Для повторных запусков сервис запрашивает сообщения новее
        `last_synced_message_id` через `FIRST_ID`. Если такой границы ещё нет,
        берётся только последнее окно сообщений размером `LIMIT`.
        """
        payload: dict[str, Any] = {
            "DIALOG_ID": dialog_id,
            "LIMIT": min(limit, 50),
        }
        if last_synced_message_id:
            payload["FIRST_ID"] = int(last_synced_message_id)
        return await self._call_method("im.dialog.messages.get", payload)

    def _build_request_context(self) -> BitrixRESTRequestContext:
        """Проверяет настройки авторизации и строит базовый endpoint REST.

        Метод защищает оператора от тихих конфигурационных ошибок: если не
        задан домен портала, user id вебхука, webhook token или OAuth token,
        сервис падает сразу с понятным сообщением ещё до боевого запуска sync.
        """
        portal_url = self.settings.rest_portal_url.strip().rstrip("/")
        if not portal_url:
            raise BitrixRESTReadonlyError(
                "Не задан BITRIX_REST_PORTAL_URL для боевого REST-рантайма."
            )

        parsed = urlparse(portal_url if "://" in portal_url else f"https://{portal_url}")
        if not parsed.scheme or not parsed.netloc:
            raise BitrixRESTReadonlyError(
                "BITRIX_REST_PORTAL_URL должен содержать домен Bitrix24, например "
                "`https://company.bitrix24.ru`."
            )
        normalized_portal_url = f"{parsed.scheme}://{parsed.netloc}"

        if self.settings.rest_auth_mode == "webhook":
            if not self.settings.rest_webhook_user_id or not self.settings.rest_webhook_token:
                raise BitrixRESTReadonlyError(
                    "Для режима webhook нужно заполнить BITRIX_REST_WEBHOOK_USER_ID "
                    "и BITRIX_REST_WEBHOOK_TOKEN."
                )
            return BitrixRESTRequestContext(
                endpoint_url=(
                    f"{normalized_portal_url}/rest/"
                    f"{self.settings.rest_webhook_user_id}/"
                    f"{self.settings.rest_webhook_token}"
                ),
                auth_mode="webhook",
            )

        if self.settings.rest_auth_mode == "oauth":
            if not self.settings.rest_access_token:
                raise BitrixRESTReadonlyError(
                    "Для режима oauth нужно заполнить BITRIX_REST_ACCESS_TOKEN."
                )
            return BitrixRESTRequestContext(
                endpoint_url=f"{normalized_portal_url}/rest",
                auth_mode="oauth",
            )

        raise BitrixRESTReadonlyError(
            "BITRIX_REST_AUTH_MODE должен быть `webhook` или `oauth`."
        )

    async def _call_method(self, method_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Вызывает один REST-метод Bitrix24 с retry и безопасной диагностикой.

        Клиент следует внутренним правилам проекта для внешних HTTP-запросов:
        ограничивает timeout, обрабатывает `429`, делает несколько повторов и не
        пишет в логи секреты, URL с токенами или полный ответ сервера.
        """
        request_payload = dict(payload)
        if self._request_context.auth_mode == "oauth":
            request_payload["auth"] = self.settings.rest_access_token

        timeout = aiohttp.ClientTimeout(total=self.settings.rest_request_timeout_seconds)
        last_error: Exception | None = None
        request_urls = self._build_candidate_urls(method_name)

        for attempt in range(1, self.settings.rest_max_retries + 1):
            try:
                data = await self._post_with_compatible_urls(
                    request_urls=request_urls,
                    request_payload=request_payload,
                    method_name=method_name,
                    timeout=timeout,
                    attempt=attempt,
                )
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as error:
                last_error = error
                if attempt >= self.settings.rest_max_retries:
                    break
                logger.warning(
                    "Повтор запроса к Bitrix24 REST после сетевой ошибки | method=%s | attempt=%s | error_type=%s",
                    method_name,
                    attempt,
                    type(error).__name__,
                )
                await asyncio.sleep(min(attempt, 3))
                continue

            if "error" in data:
                error_code = data.get("error")
                error_description = str(data.get("error_description", ""))[:300]
                raise BitrixRESTReadonlyError(
                    "Bitrix24 REST вернул бизнес-ошибку "
                    f"| method={method_name} | error={error_code} | description={error_description}"
                )
            if not isinstance(data, dict) or "result" not in data:
                raise BitrixRESTReadonlyError(
                    "Bitrix24 REST вернул неожиданный формат ответа "
                    f"| method={method_name}"
                )
            return data["result"]

        raise BitrixRESTReadonlyError(
            "Не удалось выполнить запрос к Bitrix24 REST после повторов "
            f"| method={method_name} | error_type={type(last_error).__name__ if last_error else 'Unknown'}"
        )

    def _build_candidate_urls(self, method_name: str) -> tuple[str, str]:
        """Строит оба совместимых варианта URL вызова REST-метода Bitrix24.

        На разных контурах Bitrix и в генераторе webhook встречаются оба формата:
        с суффиксом `.json` и без него. Клиент пробует сначала канонический
        вариант без суффикса, а затем fallback с `.json`, чтобы не зависеть от
        особенностей конкретного портала.
        """
        base_url = f"{self._request_context.endpoint_url}/{method_name}"
        return base_url, f"{base_url}.json"

    async def _post_with_compatible_urls(
        self,
        *,
        request_urls: tuple[str, str],
        request_payload: dict[str, Any],
        method_name: str,
        timeout: aiohttp.ClientTimeout,
        attempt: int,
    ) -> dict[str, Any]:
        """Пробует совместимые варианты REST-вызова без утечки секретов в логи.

        Метод нужен, чтобы пережить несовпадение форматов webhook URL на разных
        порталах Bitrix24. При этом в логи попадает только имя метода и статус,
        без токенов и полного URL.
        """
        last_not_found_body: str | None = None

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for request_url in request_urls:
                async with session.post(
                    request_url,
                    data=request_payload,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "application/json",
                    },
                ) as response:
                    if response.status == 429:
                        retry_after = self._extract_retry_after_seconds(response)
                        logger.warning(
                            "Bitrix24 временно ограничил запросы | method=%s | attempt=%s | retry_after=%s",
                            method_name,
                            attempt,
                            retry_after,
                        )
                        await asyncio.sleep(retry_after)
                        raise aiohttp.ClientError("Bitrix24 вернул 429 Too Many Requests.")
                    if response.status in {401, 403}:
                        raise BitrixRESTReadonlyError(
                            "Bitrix24 REST отклонил авторизацию. Проверьте webhook/OAuth "
                            f"| method={method_name} | status={response.status}"
                        )
                    if response.status >= 500:
                        raise BitrixRESTReadonlyError(
                            "Bitrix24 REST временно недоступен "
                            f"| method={method_name} | status={response.status}"
                        )
                    if response.status == 404:
                        last_not_found_body = (await response.text())[:300]
                        continue
                    if response.status >= 400:
                        body_preview = (await response.text())[:300]
                        raise BitrixRESTReadonlyError(
                            "Bitrix24 REST вернул ошибку запроса "
                            f"| method={method_name} | status={response.status} "
                            f"| body={body_preview}"
                        )
                    return await response.json()

        raise BitrixRESTReadonlyError(
            "Bitrix24 REST вернул ошибку запроса "
            f"| method={method_name} | status=404 | body={last_not_found_body or 'Method not found'}"
        )

    @staticmethod
    def _extract_retry_after_seconds(response: aiohttp.ClientResponse) -> int:
        """Возвращает безопасное время ожидания после `429 Too Many Requests`.

        Если сервер не прислал `Retry-After`, используется короткий консервативный
        backoff, чтобы не заблокировать сервис надолго и при этом не спамить
        повторными запросами.
        """
        retry_after = response.headers.get("Retry-After", "").strip()
        if retry_after.isdigit():
            return max(1, min(int(retry_after), 30))
        return 3

    @staticmethod
    def _extract_list_items(payload: Any) -> list[dict[str, Any]]:
        """Извлекает массив элементов recent-списка из REST-ответа Bitrix24.

        Для REST `im.recent.list` массив чатов находится непосредственно в
        `result`, поэтому helper остаётся простым и отделяет transport-логику от
        остальной бизнес-обработки.
        """
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("items"), list):
            return [item for item in payload["items"] if isinstance(item, dict)]
        return []

    @staticmethod
    def _extract_recent_dialog_id(item: dict[str, Any]) -> str | None:
        """Возвращает dialog id рабочего чата или `None` для лишних сущностей."""
        raw_id = item.get("id")
        if isinstance(raw_id, str) and raw_id.startswith(("chat", "sg")):
            return raw_id
        return None

    @staticmethod
    def _extract_recent_dialog_name(item: dict[str, Any], fallback: str) -> str:
        """Возвращает название чата из recent-ответа REST."""
        for key in ("title", "name"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        nested_chat = item.get("chat")
        if isinstance(nested_chat, dict):
            for key in ("name", "title"):
                value = nested_chat.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return fallback
