"""Проверки ключевых бизнес-правил сервиса контроля чатов Bitrix24."""

from __future__ import annotations

from datetime import UTC, datetime

from src_oop.core.telegram.access import TelegramActor, is_actor_allowed
from src_oop.jobs.bitrix_chat_control.analysis_service import BitrixChatAnalysisService
from src_oop.jobs.bitrix_chat_control.mcp_client import (
    BitrixMCPReadonlyError,
    ReadonlyBitrixMCPClient,
)
from src_oop.jobs.bitrix_chat_control.config import BitrixChatControlSettings
from src_oop.jobs.bitrix_chat_control.llm_client import ChatMessageForLLM
from src_oop.jobs.bitrix_chat_control.rest_client import (
    BitrixRESTReadonlyError,
    ReadonlyBitrixRESTClient,
)
from src_oop.jobs.bitrix_chat_control.models import BitrixMessage, ProblemStatus
from src_oop.jobs.bitrix_chat_control.report_service import BitrixChatReportService


class _FakeProblem:
    """Минимальная проблема для unit-тестов report service без PostgreSQL.

    Вспомогательная модель позволяет проверять формат и сортировку Telegram-отчёта
    без поднятия реальной БД, потому что на этом уровне важны именно
    бизнес-правила представления данных.
    """

    def __init__(
        self,
        *,
        title: str,
        first_seen_at: datetime,
        last_seen_at: datetime | None = None,
        resolved_at: datetime | None = None,
        status: str = ProblemStatus.OPEN.value,
        last_state_summary: str | None = None,
    ) -> None:
        self.title = title
        self.first_seen_at = first_seen_at
        self.last_seen_at = last_seen_at or first_seen_at
        self.resolved_at = resolved_at
        self.status = status
        self.last_state_summary = last_state_summary


def _message(
    message_id: int,
    text: str,
    *,
    hour: int = 9,
) -> BitrixMessage:
    """Создаёт минимальное сообщение Bitrix для unit-тестов extraction/reconciliation.

    Этот helper страхует тесты от лишнего ORM-шумa и оставляет в сценарии только
    важные поля: текст, идентификатор сообщения и момент, когда сигнал был
    получен в рабочем чате.
    """
    return BitrixMessage(
        id=message_id,
        bitrix_message_id=str(message_id),
        dialog_id="chat7249",
        author_id="1",
        author_name="Тест",
        message_text=text,
        message_datetime=datetime(2026, 8, 20, hour, tzinfo=UTC),
        raw_payload_json=None,
    )


def test_write_tool_is_forbidden_before_network_call() -> None:
    """Запрещает write MCP tools ещё до любой попытки HTTP-запроса.

    Это ключевая гарантия read-only режима: приложение физически не должно
    уметь вызвать `chat_message_add` или другой write-инструмент Bitrix24.
    """
    client = ReadonlyBitrixMCPClient()

    try:
        import asyncio

        asyncio.run(client.call_tool("chat_message_add", {}))
    except BitrixMCPReadonlyError as error:
        assert "Запрещён" in str(error)
    else:
        raise AssertionError("Ожидалась ошибка read-only режима.")


def test_actor_allow_list_accepts_only_configured_user_and_chat() -> None:
    """Разрешает Telegram-доступ только actor, который прошёл оба allow-list.

    Для корпоративного бота важно не только знать пользователя, но и проверять
    сам чат, чтобы внутренние саммари не уходили в посторонние каналы.
    """
    allowed = is_actor_allowed(
        TelegramActor(user_id=10, chat_id=20),
        allowed_user_ids=frozenset({10}),
        allowed_chat_ids=frozenset({20}),
    )
    denied = is_actor_allowed(
        TelegramActor(user_id=10, chat_id=99),
        allowed_user_ids=frozenset({10}),
        allowed_chat_ids=frozenset({20}),
    )

    assert allowed is True
    assert denied is False


def test_problem_message_with_same_entity_is_deduplicated() -> None:
    """Склеивает повторные сигналы по одной сущности в одну проблему.

    Это покрывает важный бизнес-кейс из ТЗ: несколько сообщений про `ФБС_1`
    не должны превращаться в независимые проблемы, если речь идёт об одном
    продолжающемся блокирующем кейсе.
    """
    service = BitrixChatAnalysisService()

    result = service.extract(
        [
            _message(1, "ФБС_1 блокирует склад."),
            _message(2, "По ФБС_1 больше 9000 СЗ."),
            _message(3, "Продолжаем разбираться с ФБС_1."),
        ]
    )

    assert len(result.problems) == 1
    assert result.problems[0].message_ids == [1, 2, 3]


def test_phrase_look_around_does_not_mark_resolution() -> None:
    """Не считает «посмотрю» подтверждением решения проблемы.

    Этот кейс специально зафиксирован в ТЗ: обещание действий не должно
    переводить проблему в `resolved`, иначе daily и weekly отчёты будут ложно
    занижать хвост открытых проблем.
    """
    service = BitrixChatAnalysisService()

    result = service.extract([_message(1, "Посмотрю проблему с ФБС_1 позже.")])

    assert result.resolutions == []


def test_explicit_confirmation_is_resolution_signal() -> None:
    """Считает явное подтверждение работы сигналом для закрытия проблемы.

    Это защищает бизнес-логику problem -> resolved: фраза «проверили, теперь
    работает» должна попадать в resolution extraction и идти в reconciliation.
    """
    service = BitrixChatAnalysisService()

    result = service.extract([_message(1, "Проверили, теперь работает.")])

    assert len(result.resolutions) == 1


def test_summary_keeps_problem_chronology() -> None:
    """Сохраняет сортировку проблем от старых к новым в weekly summary.

    В ТЗ явно указано, что блок «Проблемы» нельзя сортировать по важности. Он
    должен идти строго в хронологическом порядке первого упоминания.
    """
    service = BitrixChatReportService()
    period_start = datetime(2026, 8, 17, tzinfo=UTC)
    period_end = datetime(2026, 8, 21, tzinfo=UTC)
    problems = [
        _FakeProblem(title="Ошибка СМР мешает проводить возвраты.", first_seen_at=datetime(2026, 8, 19, tzinfo=UTC)),
        _FakeProblem(title="Проформы не подтянулись к машинам в пути.", first_seen_at=datetime(2026, 8, 17, tzinfo=UTC)),
    ]

    summary = service.build_chat_summary(
        chat_name="Руководители",
        period_start=period_start,
        period_end=period_end,
        problems=problems,
    )
    rendered = service.render_summary_text(summary)

    assert rendered.index("17.08: Проформы не подтянулись к машинам в пути.") < rendered.index(
        "19.08: Ошибка СМР мешает проводить возвраты."
    )


def test_recent_dialogs_keep_only_group_chats() -> None:
    """Оставляет в discovery только групповые чаты, исключая личные диалоги.

    Это покрывает боевой сценарий техаккаунта: саммари должно строиться по
    рабочим чатам, а не по личным перепискам сотрудников.
    """
    payload = {
        "items": [
            {"id": "chat19407", "title": "ФБС2"},
            {"id": "sg8849", "chat": {"name": "Проект Склад"}},
            {"id": 465, "title": "Ирина Борзенко"},
            {"id": "461", "title": "Личный диалог"},
        ],
        "has_more": False,
    }

    items, has_more = ReadonlyBitrixMCPClient._extract_recent_items(payload)
    discovered = [
        (
            ReadonlyBitrixMCPClient._extract_recent_dialog_id(item),
            ReadonlyBitrixMCPClient._extract_recent_dialog_name(item, "fallback"),
        )
        for item in items
    ]

    assert has_more is False
    assert [item for item in discovered if item[0] is not None] == [
        ("chat19407", "ФБС2"),
        ("sg8849", "Проект Склад"),
    ]


def test_recent_dialogs_support_nested_result_payload() -> None:
    """Понимает recent-ответ, если MCP оборачивает его в поле `result`.

    Это защищает discovery-слой от различий формата ответа между версиями
    Bitrix/MCP-router и не даёт silently потерять список чатов.
    """
    payload = {
        "result": {
            "items": [
                {"id": "chat1", "chat": {"name": "Общий чат"}},
            ],
            "has_more": True,
        }
    }

    items, has_more = ReadonlyBitrixMCPClient._extract_recent_items(payload)

    assert has_more is True
    assert items == [{"id": "chat1", "chat": {"name": "Общий чат"}}]


def test_slots_dataclass_messages_are_serialized_for_llm_payload() -> None:
    """Сериализует сообщения для LLM без опоры на `__dict__`.

    Это защищает production-контур после включения `slots=True`: OpenAI-вызов
    не должен падать ещё до сетевого запроса только из-за формы dataclass.
    """
    from dataclasses import asdict

    payload = asdict(
        ChatMessageForLLM(
            id=1,
            dt="2026-08-21T14:00:00+03:00",
            author="Тест",
            text="Проверили, теперь работает.",
        )
    )

    assert payload == {
        "id": 1,
        "dt": "2026-08-21T14:00:00+03:00",
        "author": "Тест",
        "text": "Проверили, теперь работает.",
    }


def test_numeric_short_problem_title_is_dropped_before_validation() -> None:
    """Не допускает падения sync на коротких числовых псевдо-заголовках.

    В рабочих чатах встречаются сообщения, где эвристика может вытащить только
    число или слишком короткий кусок текста. Такой шум нужно отбрасывать до
    создания `DetectedProblem`, иначе весь чат падает на валидации.
    """
    service = BitrixChatAnalysisService()

    result = service.extract([_message(1, "18. Проверить позже.")])

    assert result.problems == []


def test_bitrix_user_tags_are_cleaned_before_analysis() -> None:
    """Очищает bitrix user tags, чтобы проблема выделялась по смысловому тексту.

    Это повышает качество и LLM, и эвристик: если сообщение начинается с
    `[USER=..]`, extraction не должен воспринимать разметку как часть title.
    """
    service = BitrixChatAnalysisService()

    cleaned = service._clean_message_text(
        "[USER=159]Виктория[/USER] [USER=71]Константин[/USER] программа не дает "
        "на одну фабрику назначить несколько менеджеров"
    )

    assert cleaned == "Виктория Константин программа не дает на одну фабрику назначить несколько менеджеров"


def test_system_join_message_is_ignored() -> None:
    """Не считает системное приглашение в чат управленческой проблемой.

    Это защищает саммари от ложных кейсов после добавления техпользователя или
    других участников в рабочие чаты.
    """
    service = BitrixChatAnalysisService()
    message = _message(1, "Константин пригласил в чат Помощник Руководителя")
    message.raw_payload_json = {"params": {"CODE": ["CHAT_JOIN"]}}

    result = service.extract([message])

    assert result.problems == []
    assert result.resolutions == []


def test_short_generic_question_is_not_problem() -> None:
    """Не считает короткий общий вопрос признаком реальной проблемы.

    Это покрывает практический шум из чатов, где встречаются реплики вроде
    `Так, а что это?` или `Кто сканит?`, не содержащие явного сбоя.
    """
    service = BitrixChatAnalysisService()

    result = service.extract([_message(1, "Так, а что это?")])

    assert result.problems == []


def test_rest_client_requires_portal_url_for_runtime() -> None:
    """Не даёт запускать боевой REST-контур без домена Bitrix24.

    Это защищает первый production-запуск от неочевидной сетевой ошибки: при
    пустом домене сервис должен упасть сразу с конфигурационной подсказкой.
    """
    settings = BitrixChatControlSettings(
        runtime_transport="rest",
        mcp_url="https://b24.mcp-router.ru/mcp",
        mcp_protocol_version="2026-07-28",
        mcp_headers={},
        mcp_timeout_seconds=30,
        rest_portal_url="",
        rest_auth_mode="webhook",
        rest_webhook_user_id="1",
        rest_webhook_token="token",
        rest_access_token="",
        rest_request_timeout_seconds=30,
        rest_max_retries=3,
        monitored_dialog_ids=(),
        chat_discovery_enabled=True,
        chat_discovery_page_limit=200,
        messages_limit=200,
        analysis_model="gpt-4.1-mini",
        daily_report_hour_moscow=9,
        weekly_report_weekday=0,
        admin_notify_enabled=True,
    )

    try:
        ReadonlyBitrixRESTClient(settings=settings)
    except BitrixRESTReadonlyError as error:
        assert "BITRIX_REST_PORTAL_URL" in str(error)
    else:
        raise AssertionError("Ожидалась ошибка конфигурации REST.")


def test_rest_recent_dialogs_keep_only_group_chats() -> None:
    """Отфильтровывает личные диалоги и оставляет только рабочие чаты.

    Это страхует discovery-слой после перехода на REST: список monitored_chats
    не должен разрастаться за счёт личных бесед техаккаунта.
    """
    payload = [
        {"id": "chat19407", "title": "ФБС2"},
        {"id": "sg8849", "chat": {"name": "Проект Склад"}},
        {"id": 465, "title": "Ирина Борзенко"},
    ]

    items = ReadonlyBitrixRESTClient._extract_list_items(payload)
    discovered = [
        (
            ReadonlyBitrixRESTClient._extract_recent_dialog_id(item),
            ReadonlyBitrixRESTClient._extract_recent_dialog_name(item, "fallback"),
        )
        for item in items
    ]

    assert [item for item in discovered if item[0] is not None] == [
        ("chat19407", "ФБС2"),
        ("sg8849", "Проект Склад"),
    ]


def test_rest_extracts_items_from_dict_payload() -> None:
    """Понимает реальный REST-ответ im.recent.list с ключом `items`.

    Это страхует ручное обновление `/refresh_chats` и bootstrap monitored_chats:
    список чатов не должен теряться только из-за того, что Bitrix вернул словарь
    с массивом `items`, а не голый список верхнего уровня.
    """
    payload = {
        "items": [
            {"id": "chat1", "title": "Общий чат"},
            {"id": "chat2", "title": "IT Команда"},
        ]
    }

    items = ReadonlyBitrixRESTClient._extract_list_items(payload)

    assert items == [
        {"id": "chat1", "title": "Общий чат"},
        {"id": "chat2", "title": "IT Команда"},
    ]
