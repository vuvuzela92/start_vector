"""Доменные модели и ORM-схема сервиса контроля рабочих чатов Bitrix24."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    String,
    Text,
    UniqueConstraint,
    MetaData,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

BITRIX_CHAT_CONTROL_SCHEMA = "bitrix_chat_control"


class BitrixChatControlBase(DeclarativeBase):
    """Базовый класс метаданных ORM для сервиса контроля чатов Bitrix24."""

    metadata = MetaData(schema=BITRIX_CHAT_CONTROL_SCHEMA)


class ProblemStatus(StrEnum):
    """Описывает жизненный цикл управленческой проблемы в рабочем чате."""

    OPEN = "open"
    RESOLVED = "resolved"


class AnalysisRunStatus(StrEnum):
    """Описывает результат выполнения шага синхронизации/анализа/reporting."""

    STARTED = "started"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ProblemMessageRelationType(StrEnum):
    """Фиксирует, почему сообщение связано с проблемой в explainable-аналитике."""

    PROBLEM_SIGNAL = "problem_signal"
    RESOLUTION_SIGNAL = "resolution_signal"
    CONTEXT = "context"


class MonitoredChat(BitrixChatControlBase):
    """Хранит список рабочих чатов Bitrix24, включённых в мониторинг."""

    __tablename__ = "monitored_chats"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    bitrix_dialog_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_synced_message_id: Mapped[str | None] = mapped_column(String(255))
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
        onupdate=lambda: datetime.now(tz=UTC),
    )


class BitrixMessage(BitrixChatControlBase):
    """Хранит локальную idempotent-копию сообщений Bitrix24 для анализа."""

    __tablename__ = "bitrix_messages"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    bitrix_message_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    dialog_id: Mapped[str] = mapped_column(String(255), nullable=False)
    author_id: Mapped[str | None] = mapped_column(String(255))
    author_name: Mapped[str] = mapped_column(String(255), nullable=False)
    message_text: Mapped[str] = mapped_column(Text, nullable=False)
    message_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_payload_json: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )

    __table_args__ = (
        Index("ix_bitrix_messages_dialog_datetime", "dialog_id", "message_datetime"),
    )


class Problem(BitrixChatControlBase):
    """Хранит агрегированную проблему и её текущее состояние для Telegram-саммари."""

    __tablename__ = "problems"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    chat_id: Mapped[int] = mapped_column(
        ForeignKey("monitored_chats.id", ondelete="CASCADE"),
        nullable=False,
    )
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    normalized_title: Mapped[str] = mapped_column(String(500), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[ProblemStatus] = mapped_column(String(32), nullable=False)
    resolution_summary: Mapped[str | None] = mapped_column(Text)
    last_state_summary: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
        onupdate=lambda: datetime.now(tz=UTC),
    )

    __table_args__ = (
        Index("ix_problems_chat_status", "chat_id", "status"),
        Index("ix_problems_chat_first_seen", "chat_id", "first_seen_at"),
    )


class ProblemMessage(BitrixChatControlBase):
    """Связывает проблему с исходными сообщениями для explainable-аудита."""

    __tablename__ = "problem_messages"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    problem_id: Mapped[int] = mapped_column(
        ForeignKey("problems.id", ondelete="CASCADE"),
        nullable=False,
    )
    message_id: Mapped[int] = mapped_column(
        ForeignKey("bitrix_messages.id", ondelete="CASCADE"),
        nullable=False,
    )
    relation_type: Mapped[ProblemMessageRelationType] = mapped_column(
        String(64),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )

    __table_args__ = (
        UniqueConstraint(
            "problem_id",
            "message_id",
            "relation_type",
            name="uq_problem_messages_problem_message_relation",
        ),
    )


class AnalysisRun(BitrixChatControlBase):
    """Логирует шаги синхронизации, анализа и отправки отчётов по чатам."""

    __tablename__ = "analysis_runs"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    chat_id: Mapped[int | None] = mapped_column(
        ForeignKey("monitored_chats.id", ondelete="SET NULL")
    )
    run_type: Mapped[str] = mapped_column(String(64), nullable=False)
    period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[AnalysisRunStatus] = mapped_column(String(32), nullable=False)
    messages_scanned: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    new_messages_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    new_problems_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_problems_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    resolved_problems_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    report_hash: Mapped[str | None] = mapped_column(String(128))
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TelegramReportLog(BitrixChatControlBase):
    """Защищает daily/weekly-рассылки от повторной отправки одинакового отчёта."""

    __tablename__ = "telegram_report_log"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    chat_id: Mapped[int | None] = mapped_column(
        ForeignKey("monitored_chats.id", ondelete="SET NULL")
    )
    report_type: Mapped[str] = mapped_column(String(64), nullable=False)
    period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    recipient_chat_id: Mapped[str] = mapped_column(String(255), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(tz=UTC),
    )

    __table_args__ = (
        UniqueConstraint(
            "report_type",
            "chat_id",
            "period_start",
            "period_end",
            "recipient_chat_id",
            "content_hash",
            name="uq_telegram_report_log_dedup",
        ),
    )


class BitrixMessageInput(BaseModel):
    """Валидирует и нормализует сообщение Bitrix24 перед записью в PostgreSQL.

    Модель обслуживает границу внешней интеграции: мы сохраняем только безопасно
    нормализованные поля сообщения, чтобы синхронизация была идемпотентной и не
    зависела от случайных форматов raw payload.
    """

    model_config = ConfigDict(extra="allow")

    bitrix_message_id: str = Field(min_length=1)
    dialog_id: str = Field(min_length=1)
    author_id: str | None = None
    author_name: str = Field(default="Неизвестный автор", min_length=1)
    message_text: str = Field(min_length=1)
    message_datetime: datetime
    raw_payload_json: dict | None = None

    @field_validator("message_text")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        """Убирает пустые и шумные пробелы, сохраняя человекочитаемый текст.

        Это защищает аналитический слой от ложных дублей, которые возникают,
        когда одно и то же сообщение отличается только количеством пробелов и
        переносов строк.
        """
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("Текст сообщения Bitrix24 не должен быть пустым.")
        return normalized

    @field_validator("message_datetime")
    @classmethod
    def ensure_timezone(cls, value: datetime) -> datetime:
        """Требует timezone-aware дату сообщения для корректной хронологии проблем.

        Бизнес-правило: daily и weekly-отчёты нельзя строить по наивным датам, иначе
        одно и то же сообщение может попасть в разные сутки и исказить саммари.
        """
        if value.tzinfo is None:
            raise ValueError("Дата сообщения должна содержать часовой пояс.")
        return value.astimezone(UTC)


class DetectedProblem(BaseModel):
    """Описывает проблему, выделенную из новых сообщений рабочего чата.

    Эта структура обслуживает extraction-стадию и служит входом для
    reconciliation: дальше приложение решает, новая это проблема или обновление
    уже существующей записи в БД.
    """

    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=3, max_length=500)
    normalized_title: str = Field(min_length=2, max_length=500)
    message_ids: list[int]
    first_seen_at: datetime
    summary: str = Field(min_length=3)
    entity_tokens: list[str] = Field(default_factory=list)


class DetectedResolution(BaseModel):
    """Описывает сигнал явного решения проблемы из новых сообщений Bitrix24.

    Структура нужна для отдельной reconciliation-стадии, где приложение должно
    отличить реальное подтверждение результата от обещаний вроде «посмотрю» или
    «создал задачу».
    """

    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=3, max_length=500)
    normalized_title: str = Field(min_length=2, max_length=500)
    message_ids: list[int]
    resolved_at: datetime
    summary: str = Field(min_length=3)
    entity_tokens: list[str] = Field(default_factory=list)


class ExtractionResult(BaseModel):
    """Содержит структурированный результат extraction-стадии анализа чата."""

    model_config = ConfigDict(extra="forbid")

    problems: list[DetectedProblem] = Field(default_factory=list)
    resolutions: list[DetectedResolution] = Field(default_factory=list)


class ChatSummarySection(BaseModel):
    """Описывает блок итогового Telegram-саммари по одному чату."""

    model_config = ConfigDict(extra="forbid")

    title: str
    lines: list[str] = Field(default_factory=list)


class ChatSummary(BaseModel):
    """Описывает готовое компактное саммари чата для Telegram.

    Эта модель обслуживает финальный этап pipeline: после reconciliation и чтения
    состояния БД приложение формирует короткий управленческий текст, понятный
    руководителю без чтения всей переписки.
    """

    model_config = ConfigDict(extra="forbid")

    status_emoji: str
    chat_name: str
    period_label: str
    problems: ChatSummarySection
    resolved: ChatSummarySection
    unresolved: ChatSummarySection
    conclusion: str
    requires_attention: bool


@dataclass(slots=True)
class AnalysisCounters:
    """Хранит счётчики reconciliation-стадии для analysis_runs и логов."""

    messages_scanned: int = 0
    new_messages_count: int = 0
    new_problems_count: int = 0
    updated_problems_count: int = 0
    resolved_problems_count: int = 0


@dataclass(slots=True)
class ReconciliationOutcome:
    """Возвращает итог обработки пачки новых сообщений одного чата."""

    counters: AnalysisCounters = field(default_factory=AnalysisCounters)
    extraction_result: ExtractionResult = field(default_factory=ExtractionResult)
