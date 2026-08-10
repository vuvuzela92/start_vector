"""Модели обмена данными внутри сценария WB Order Feed."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import NotRequired, TypedDict

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
    Table,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src_oop.jobs.orders_feed.config import TABLE_NAME
from src_oop.jobs.orders_feed.schemas.api import OrderFeedOrderResponse
from src_oop.jobs.orders_feed.schemas.enums import (
    CancelType,
    DataSource,
    OrderStatus,
    SaleType,
    WarehouseType,
)


class OrderFeedPaginationRequest(TypedDict):
    """Типизирует offset-пагинацию запроса, где snapshotTime отсутствует на первой странице."""

    offset: int
    limit: int
    snapshotTime: NotRequired[str]


class OrderFeedSelectedPeriodRequest(TypedDict):
    """Типизирует границы периода по времени текущего статуса заказа."""

    start: str
    end: str


class OrderFeedRequest(TypedDict):
    """Описывает полное JSON-тело запроса отчёта без товарных фильтров."""

    selectedPeriod: OrderFeedSelectedPeriodRequest
    nmIds: list[int]
    subjectIds: list[int]
    brandNames: list[str]
    tagIds: list[int]
    pagination: OrderFeedPaginationRequest


def _enum_values(enum_class: type[StrEnum]) -> list[str]:
    """Передаёт SQLAlchemy значения enum, чтобы PostgreSQL хранил понятные строки, а не имена Python."""
    return [item.value for item in enum_class]


class OrderFeedBase(DeclarativeBase):
    """Базовый класс метаданных для управляемого создания таблицы Order Feed."""


# Ссылка нужна SQLAlchemy для построения FK, но repository намеренно не создаёт таблицу article.
ARTICLE_REFERENCE_TABLE = Table(
    "article",
    OrderFeedBase.metadata,
    Column("nm_id", BigInteger, primary_key=True),
)


class WBOrderFeedRecord(OrderFeedBase):
    """Декларативная модель строки PostgreSQL-таблицы WB Order Feed."""

    __tablename__ = TABLE_NAME

    account: Mapped[str] = mapped_column(String(255), primary_key=True)
    srid: Mapped[str] = mapped_column(String(255), primary_key=True)
    nm_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "article.nm_id",
            name="fk_wb_order_feed_nm_id_article",
            onupdate="CASCADE",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    chrt_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[OrderStatus] = mapped_column(
        Enum(OrderStatus, name="wb_order_feed_status", values_callable=_enum_values),
        nullable=False,
    )
    cancel_type: Mapped[CancelType | None] = mapped_column(
        Enum(CancelType, name="wb_order_feed_cancel_type", values_callable=_enum_values)
    )
    warehouse_name: Mapped[str] = mapped_column(String(255), nullable=False)
    warehouse_region: Mapped[str] = mapped_column(String(255), nullable=False)
    warehouse_type: Mapped[WarehouseType] = mapped_column(
        Enum(WarehouseType, name="wb_order_feed_warehouse_type", values_callable=_enum_values),
        nullable=False,
    )
    destination_city: Mapped[str] = mapped_column(String(255), nullable=False)
    destination_district: Mapped[str] = mapped_column(String(255), nullable=False)
    seller_price: Mapped[float] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    sale_type: Mapped[SaleType] = mapped_column(
        Enum(SaleType, name="wb_order_feed_sale_type", values_callable=_enum_values),
        nullable=False,
    )
    data_source: Mapped[DataSource] = mapped_column(
        Enum(DataSource, name="wb_data_source", values_callable=_enum_values),
        nullable=False,
    )
    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_wb_order_feed_account_updated_at", "account", "updated_at"),
        Index("ix_wb_order_feed_nm_id", "nm_id"),
        Index("ix_wb_order_feed_status", "status"),
    )


@dataclass(frozen=True, slots=True)
class OrderFeedPeriod:
    """Описывает допустимый период одного запроса отчёта по текущему статусу заказа."""

    start: datetime
    end: datetime


@dataclass(slots=True)
class OrderFeedPage:
    """Хранит одну страницу стабильного снимка WB и метрики выполненного запроса."""

    account: str
    snapshot_time: str
    currency: str
    orders: list[OrderFeedOrderResponse]
    offset: int
    limit: int
    retries_used: int = 0

    @property
    def has_next_page(self) -> bool:
        """Определяет продолжение пагинации по бизнес-правилу неполной последней страницы."""
        return len(self.orders) >= self.limit


@dataclass(slots=True)
class OrderFeedSaveResult:
    """Содержит результат upsert одной страницы отчёта в PostgreSQL."""

    input_rows: int
    written_rows: int
    dropped_missing_key_rows: int = 0
    collapsed_duplicate_rows: int = 0


@dataclass(slots=True)
class OrderFeedRunSummary:
    """Сводит метрики полной загрузки Order Feed по выбранным кабинетам."""

    accounts_total: int = 0
    pages_received: int = 0
    raw_rows: int = 0
    normalized_rows: int = 0
    written_rows: int = 0
    dropped_missing_key_rows: int = 0
    collapsed_duplicate_rows: int = 0
    total_retry_count: int = 0
    failed_accounts: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None
