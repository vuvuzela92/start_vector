"""Structured-output LLM клиент для extraction событий из рабочих чатов Bitrix24."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict
from src_oop.jobs.bitrix_chat_control.config import BitrixChatControlSettings
from src_oop.jobs.bitrix_chat_control.models import ExtractionResult
from src_oop.jobs.bitrix_chat_control.prompts import EXTRACTION_SYSTEM_PROMPT

load_dotenv()


class _ExtractionEnvelope(BaseModel):
    """Описывает structured-output extraction-ответ LLM для Bitrix chat control.

    Конверт нужен как строгая граница между внешним LLM-ответом и внутренней
    моделью `ExtractionResult`, чтобы дальше в reconciliation и БД попадал
    только валидный структурированный результат без свободного текста.
    """

    model_config = ConfigDict(extra="forbid")

    result: ExtractionResult


@dataclass(frozen=True, slots=True)
class ChatMessageForLLM:
    """Описывает минимальный набор полей сообщения для extraction-подсказки.

    Эта структура нужна, чтобы модель видела только полезный контекст:
    идентификатор сообщения, время, автора и текст, без raw payload и других
    технических деталей внешней интеграции.
    """

    id: int
    dt: str
    author: str
    text: str


class BitrixChatLLMClient:
    """Выделяет проблемы и решения через OpenAI structured output с fallback."""

    def __init__(self, settings: BitrixChatControlSettings | None = None) -> None:
        """Подключает настройки production-модели для Bitrix analysis."""
        self.settings = settings or BitrixChatControlSettings.from_env()

    def is_available(self) -> bool:
        """Проверяет, можно ли использовать OpenAI extraction в этом окружении.

        Бизнес-правило: отсутствие ключа или Python-пакета не должно ломать
        синхронизацию чатов. Если production LLM недоступен, сервис обязан
        безопасно переключиться на локальные эвристики.
        """
        if not os.getenv("OPENAI_API_KEY", "").strip():
            return False
        try:
            __import__("openai")
        except ImportError:
            return False
        return True

    def extract(self, messages: list[ChatMessageForLLM]) -> ExtractionResult:
        """Вызывает OpenAI и возвращает extraction-результат в строгой структуре.

        Метод обслуживает production extraction: новые сообщения передаются в
        модель компактным JSON, а на выходе ожидается только structured output,
        который можно напрямую передавать в reconciliation.
        """
        from openai import OpenAI

        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", "").strip())
        user_payload = {
            "messages": [asdict(message) for message in messages],
            "rules": {
                "ignore_as_resolution": [
                    "посмотрю",
                    "проверю",
                    "сделаем",
                    "поправим",
                    "занимаюсь",
                    "передал разработчику",
                    "создал задачу",
                ],
                "keep_message_ids": True,
                "timezone": "Europe/Moscow",
            },
        }
        response = client.responses.create(
            model=self.settings.analysis_model,
            input=[
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": EXTRACTION_SYSTEM_PROMPT}],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": json.dumps(user_payload, ensure_ascii=False),
                        }
                    ],
                },
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "bitrix_chat_extraction",
                    "schema": self._response_format_schema(),
                    "strict": True,
                }
            },
        )
        parsed = _ExtractionEnvelope.model_validate_json(self._read_output_text(response))
        return parsed.result

    @staticmethod
    def _response_format_schema() -> dict[str, Any]:
        """Возвращает JSON Schema в формате, совместимом со strict Responses API.

        OpenAI strict json_schema ожидает, что у каждого объекта будет явно
        перечислен `required` со всеми полями из `properties`, включая поля с
        default/default_factory. Pydantic-схема не всегда делает это в точности
        так, как требует API, поэтому здесь мы рекурсивно доводим схему до
        совместимого вида перед отправкой запроса.
        """
        schema = _ExtractionEnvelope.model_json_schema()

        def ensure_required(node: Any) -> None:
            if isinstance(node, dict):
                properties = node.get("properties")
                if isinstance(properties, dict) and properties:
                    node["required"] = list(properties.keys())
                for value in node.values():
                    ensure_required(value)
            elif isinstance(node, list):
                for item in node:
                    ensure_required(item)

        ensure_required(schema)
        return schema

    @staticmethod
    def _read_output_text(response: Any) -> str:
        """Извлекает JSON-текст structured output из ответа Responses API.

        Это нужно для устойчивости к вариантам SDK-объекта ответа: иногда текст
        уже собран в `output_text`, а иногда его приходится составлять из
        сегментов `output.content`.
        """
        output_text = getattr(response, "output_text", None)
        if isinstance(output_text, str) and output_text.strip():
            return output_text

        collected: list[str] = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                text_value = getattr(content, "text", None)
                if isinstance(text_value, str):
                    collected.append(text_value)
        if collected:
            return "".join(collected)
        raise RuntimeError("OpenAI Responses API не вернул structured output текст.")
