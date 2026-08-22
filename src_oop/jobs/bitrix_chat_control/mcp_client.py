"""Read-only клиент Bitrix24 через удалённый MCP Streamable HTTP."""

from __future__ import annotations

import json
import logging
from itertools import count
from typing import Any

import aiohttp
from src_oop.jobs.bitrix_chat_control.config import (
    BITRIX_ALLOWED_MCP_TOOLS,
    BitrixChatControlSettings,
)

logger = logging.getLogger(__name__)


class BitrixMCPReadonlyError(RuntimeError):
    """Сигнализирует о нарушении read-only режима или ошибке MCP Bitrix24."""


class ReadonlyBitrixMCPClient:
    """Работает только с разрешёнными read-only tool Bitrix24 через MCP.

    Клиент обслуживает ключевое бизнес-правило сервиса: Bitrix24 используется
    только как источник данных. Любая попытка выйти за allow-list должна
    завершаться ошибкой ещё до HTTP-запроса к MCP, чтобы код приложения
    физически не мог вызвать write-методы чатов.
    """

    def __init__(
        self,
        settings: BitrixChatControlSettings | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        """Подключает конфигурацию MCP и готовит read-only клиент."""
        self.settings = settings or BitrixChatControlSettings.from_env()
        self.timeout_seconds = timeout_seconds or self.settings.mcp_timeout_seconds
        self._request_ids = count(1)
        self._initialized = False
        self._tool_schemas: dict[str, dict[str, Any]] = {}

    async def list_recent_dialogs(self) -> Any:
        """Получает список доступных диалогов Bitrix24 для выбора monitored_chats."""
        return await self.call_tool("chat_recent_list", {})

    async def list_work_chats(
        self,
        *,
        page_limit: int,
        dialog_ids_filter: frozenset[str] | None = None,
    ) -> list[tuple[str, str]]:
        """Возвращает все групповые чаты, доступные техаккаунту, без личных диалогов.

        Этот метод обслуживает боевой сценарий масштабирования: при росте числа
        рабочих чатов оператор не должен вручную поддерживать список dialog id.
        Если `BITRIX_CHAT_IDS` задан, он используется как безопасный фильтр, а не
        как единственный источник чатов.
        """
        offset = 0
        discovered: dict[str, str] = {}

        while True:
            payload = await self.call_tool(
                "chat_recent_list",
                {
                    "limit": page_limit,
                    "offset": offset,
                    "skip_dialog": True,
                    "skip_openlines": True,
                    "parse_text": False,
                    "get_original_text": False,
                },
            )
            items, has_more = self._extract_recent_items(payload)
            for item in items:
                dialog_id = self._extract_recent_dialog_id(item)
                if not dialog_id:
                    continue
                if dialog_ids_filter and dialog_id not in dialog_ids_filter:
                    continue
                chat_name = self._extract_recent_dialog_name(item, dialog_id)
                discovered[dialog_id] = chat_name

            if not has_more or not items:
                break
            offset += page_limit

        return sorted(discovered.items(), key=lambda item: item[1].lower())

    async def get_recent_dialog(self, dialog_id: str) -> Any:
        """Получает сводку по недавнему диалогу для уточнения его метаданных."""
        return await self.call_tool(
            "chat_recent_get",
            self._build_arguments(
                "chat_recent_get",
                dialog_id=dialog_id,
            ),
        )

    async def get_dialog(self, dialog_id: str) -> Any:
        """Получает карточку чата Bitrix24 для названия и идентификаторов."""
        return await self.call_tool(
            "chat_dialog_get",
            self._build_arguments("chat_dialog_get", dialog_id=dialog_id),
        )

    async def get_dialog_users(self, dialog_id: str) -> Any:
        """Получает участников чата для последующих Telegram- и AI-сценариев."""
        return await self.call_tool(
            "chat_dialog_users_list",
            self._build_arguments("chat_dialog_users_list", dialog_id=dialog_id),
        )

    async def get_dialog_messages(
        self,
        dialog_id: str,
        *,
        limit: int,
        last_synced_message_id: str | None = None,
    ) -> Any:
        """Получает историю сообщений чата, предпочитая инкрементальную загрузку.

        Бизнес-правило: сервис не должен перечитывать бесконечно всю историю.
        Поэтому клиент сначала пытается использовать параметр последнего
        обработанного сообщения, если он поддерживается схемой инструмента.
        Если такого параметра нет, клиент работает в деградированном режиме и
        читает только ограниченное окно последних сообщений.
        """
        return await self.call_tool(
            "chat_dialog_messages_get",
            self._build_arguments(
                "chat_dialog_messages_get",
                dialog_id=dialog_id,
                limit=limit,
                last_synced_message_id=last_synced_message_id,
            ),
        )

    async def list_users(self) -> Any:
        """Получает справочник пользователей Bitrix24 в read-only режиме."""
        return await self.call_tool("chat_user_list", {})

    async def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        """Вызывает только разрешённый tool Bitrix24 и возвращает его payload.

        Этот метод реализует главное техническое ограничение проекта: если tool
        не входит в allow-list read-only-методов, запрос даже не уйдёт в сеть.
        """
        if tool_name not in BITRIX_ALLOWED_MCP_TOOLS:
            raise BitrixMCPReadonlyError(
                f"Запрещён вызов write или неизвестного MCP tool: {tool_name}"
            )

        await self._initialize_if_needed()
        payload = {
            "jsonrpc": "2.0",
            "id": next(self._request_ids),
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": self.settings.mcp_protocol_version,
                    "io.modelcontextprotocol/clientInfo": {
                        "name": "start_vector_bitrix_chat_control",
                        "version": "1.0.0",
                    },
                    "io.modelcontextprotocol/clientCapabilities": {},
                },
            },
        }
        response = await self._post_jsonrpc(payload)
        result = response.get("result")
        if not isinstance(result, dict):
            raise BitrixMCPReadonlyError(
                f"MCP Bitrix24 вернул неожиданный ответ tools/call для {tool_name}."
            )
        if result.get("isError") is True:
            raise BitrixMCPReadonlyError(
                f"MCP Bitrix24 вернул ошибку инструмента {tool_name}."
            )
        return self._unwrap_tool_result(result)

    async def _initialize_if_needed(self) -> None:
        """Инициализирует MCP-сессию и кеширует схемы доступных read-only tools.

        Инициализация нужна не только по требованиям MCP, но и для безопасной
        адаптации к inputSchema удалённого сервера: дальше клиент подбирает имена
        аргументов не наугад, а на основе реально опубликованных схем tools/list.
        """
        if self._initialized:
            return

        initialize_payload = {
            "jsonrpc": "2.0",
            "id": next(self._request_ids),
            "method": "initialize",
            "params": {
                "protocolVersion": self.settings.mcp_protocol_version,
                "clientInfo": {
                    "name": "start_vector_bitrix_chat_control",
                    "version": "1.0.0",
                },
                "capabilities": {},
            },
        }
        await self._post_jsonrpc(initialize_payload)
        tools_list_payload = {
            "jsonrpc": "2.0",
            "id": next(self._request_ids),
            "method": "tools/list",
            "params": {},
        }
        tools_response = await self._post_jsonrpc(tools_list_payload)
        tools = tools_response.get("result", {}).get("tools", [])
        if isinstance(tools, list):
            for tool in tools:
                if isinstance(tool, dict) and isinstance(tool.get("name"), str):
                    self._tool_schemas[tool["name"]] = tool
        self._initialized = True

    def _build_arguments(
        self,
        tool_name: str,
        *,
        dialog_id: str | None = None,
        limit: int | None = None,
        last_synced_message_id: str | None = None,
    ) -> dict[str, Any]:
        """Подбирает аргументы под реальную inputSchema удалённого Bitrix tool.

        Метод защищает проект от хрупкой привязки к одному варианту имён
        параметров. Если сервер использует `dialog_id`, `dialogId` или другое
        совместимое имя, клиент выберет его автоматически после `tools/list`.
        """
        properties = (
            self._tool_schemas.get(tool_name, {})
            .get("inputSchema", {})
            .get("properties", {})
        )
        arguments: dict[str, Any] = {}

        if dialog_id is not None:
            for field_name in ("dialog_id", "dialogId", "chat_id", "chatId", "id"):
                if field_name in properties:
                    arguments[field_name] = dialog_id
                    break

        if limit is not None and "limit" in properties:
            arguments["limit"] = limit

        if last_synced_message_id is not None:
            for field_name in (
                "last_id",
                "lastId",
                "from_id",
                "fromId",
                "message_id",
                "messageId",
            ):
                if field_name in properties:
                    arguments[field_name] = last_synced_message_id
                    break
        return arguments

    async def _post_jsonrpc(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Отправляет один JSON-RPC запрос в MCP endpoint и возвращает JSON-ответ.

        Метод обслуживает Streamable HTTP transport. Он поддерживает как обычный
        JSON-ответ, так и SSE-ответ с финальным JSON-RPC payload, чтобы не
        зависеть от того, какой режим ответа выберет удалённый MCP-сервер.
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": self.settings.mcp_protocol_version,
            **self.settings.mcp_headers,
        }
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self.settings.mcp_url,
                json=payload,
                headers=headers,
            ) as response:
                if response.status >= 400:
                    body = await response.text()
                    if response.status in {401, 403}:
                        raise BitrixMCPReadonlyError(
                            "Ошибка авторизации MCP Bitrix24. Проверьте OAuth/headers "
                            f"| status={response.status}"
                        )
                    raise BitrixMCPReadonlyError(
                        "MCP Bitrix24 вернул HTTP-ошибку "
                        f"| status={response.status} | body={body[:500]}"
                    )
                content_type = response.headers.get("Content-Type", "")
                if "text/event-stream" in content_type:
                    return await self._read_sse_jsonrpc(response)
                return await response.json()

    async def _read_sse_jsonrpc(self, response: aiohttp.ClientResponse) -> dict[str, Any]:
        """Извлекает финальный JSON-RPC ответ из SSE-потока Streamable HTTP.

        Это нужно для совместимости с MCP-серверами, которые стримят прогресс и
        закрывают запрос финальным `data:`-событием с итоговым JSON-RPC ответом.
        """
        last_payload: dict[str, Any] | None = None
        async for raw_chunk in response.content:
            chunk = raw_chunk.decode("utf-8", errors="ignore")
            for line in chunk.splitlines():
                if not line.startswith("data:"):
                    continue
                raw_json = line[len("data:") :].strip()
                if not raw_json:
                    continue
                try:
                    candidate = json.loads(raw_json)
                except json.JSONDecodeError:
                    continue
                if isinstance(candidate, dict) and "jsonrpc" in candidate:
                    last_payload = candidate
        if last_payload is None:
            raise BitrixMCPReadonlyError(
                "MCP Bitrix24 вернул SSE без финального JSON-RPC результата."
            )
        return last_payload

    def _unwrap_tool_result(self, result: dict[str, Any]) -> Any:
        """Преобразует MCP result в полезный payload Bitrix tool.

        Сервера MCP часто возвращают `content` как список текстовых блоков или
        JSON-объектов. Метод старается аккуратно распаковать результат без жёсткой
        привязки к одному формату ответа.
        """
        structured = result.get("structuredContent")
        if structured is not None:
            return structured

        content = result.get("content")
        if not isinstance(content, list):
            return result
        if len(content) == 1 and isinstance(content[0], dict):
            item = content[0]
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                text_payload = item["text"].strip()
                try:
                    return json.loads(text_payload)
                except json.JSONDecodeError:
                    return text_payload
            return item
        return content

    @staticmethod
    def _extract_recent_items(payload: Any) -> tuple[list[dict[str, Any]], bool]:
        """Нормализует список recent-диалогов и признак продолжения пагинации.

        Helper защищает discovery-слой от различий форматов MCP-обвязки: данные
        могут лежать в `items`, `result.items` или приходить уже распакованным
        словарём верхнего уровня.
        """
        if not isinstance(payload, dict):
            return [], False
        if isinstance(payload.get("items"), list):
            return (
                [item for item in payload["items"] if isinstance(item, dict)],
                bool(payload.get("has_more")),
            )
        result = payload.get("result")
        if isinstance(result, dict) and isinstance(result.get("items"), list):
            return (
                [item for item in result["items"] if isinstance(item, dict)],
                bool(result.get("has_more")),
            )
        return [], False

    @staticmethod
    def _extract_recent_dialog_id(item: dict[str, Any]) -> str | None:
        """Извлекает dialog id группового чата из одного элемента recent-списка.

        Это бизнес-правило discovery: в мониторинг должны попадать только чаты,
        у которых есть стабильный dialog id вида `chat123` или `sg123`.
        Личные диалоги и неполные записи здесь отбрасываются.
        """
        raw_id = item.get("id")
        if not isinstance(raw_id, str):
            return None
        if raw_id.startswith(("chat", "sg")):
            return raw_id
        return None

    @staticmethod
    def _extract_recent_dialog_name(item: dict[str, Any], fallback: str) -> str:
        """Возвращает человекочитаемое имя чата из recent-пейлоада Bitrix24.

        Название нужно не только для удобства оператора, но и для Telegram-бота:
        пользователь должен видеть в списке реальные имена рабочих чатов, а не
        внутренние dialog id.
        """
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
