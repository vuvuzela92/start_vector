"""Нормализация ответа WB Order Feed в табличную схему PostgreSQL."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from src_oop.jobs.orders_feed.schemas.database import OrderFeedDatabaseRow
from src_oop.jobs.orders_feed.schemas.internal import OrderFeedPage

logger = logging.getLogger(__name__)


class OrderFeedNormalizer:
    """Преобразует camelCase-поля WB в типизированные snake_case-колонки витрины."""

    def normalize(self, page: OrderFeedPage) -> list[OrderFeedDatabaseRow]:
        """Преобразует проверенные API-схемы в проверенные Pydantic-схемы PostgreSQL."""
        database_snapshot_time = page.snapshot_time or datetime.now(tz=UTC)
        for order in page.orders:
            if order.known_status is None:
                logger.warning(
                    "WB вернул неизвестный статус заказа; значение будет сохранено "
                    "без изменений | account=%s | srid=%s | status=%s",
                    page.account,
                    order.srid,
                    order.status,
                )
            if order.cancel_type is not None and order.known_cancel_type is None:
                logger.warning(
                    "WB вернул неизвестный тип отмены; значение будет сохранено "
                    "без изменений | account=%s | srid=%s | cancel_type=%s",
                    page.account,
                    order.srid,
                    order.cancel_type,
                )
        rows = [
            OrderFeedDatabaseRow.from_api(
                order=order,
                account=page.account,
                currency=page.currency,
                snapshot_time=database_snapshot_time,
            )
            for order in page.orders
        ]
        logger.info(
            "Нормализована страница Order Feed | account=%s | offset=%s | rows=%s",
            page.account,
            page.offset,
            len(rows),
        )
        return rows
