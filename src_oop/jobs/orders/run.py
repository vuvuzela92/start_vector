from __future__ import annotations

import asyncio
import logging
from datetime import date

from src_oop.jobs.orders.service import OrdersService

logger = logging.getLogger(__name__)


def _coerce_date(value: date | str | None, parameter_name: str) -> date | None:
    """Приводит параметр даты entrypoint к `date` для запуска загрузки заказов WB."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ValueError(
                f"Параметр {parameter_name} должен быть датой в формате YYYY-MM-DD."
            ) from error
    raise TypeError(
        f"Параметр {parameter_name} должен иметь тип date, str или None."
    )


async def orders_async(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    """Запускает async-entrypoint загрузки заказов WB в таблицу orders.

    Бизнес-сценарий: получить заказы WB за выбранный период по всем или одному
    кабинету, нормализовать поля под витрину PostgreSQL и выполнить upsert по
    ключу `(date, srid)`.
    """
    resolved_date_from = _coerce_date(date_from, "date_from")
    resolved_date_to = _coerce_date(date_to, "date_to")

    logger.info(
        "Старт entrypoint orders_async | date_from=%s | date_to=%s | account=%s",
        resolved_date_from.isoformat() if resolved_date_from else None,
        resolved_date_to.isoformat() if resolved_date_to else None,
        account,
    )
    service = OrdersService()
    summary = await service.run(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        account=account,
    )
    logger.info(
        "Завершен entrypoint orders_async | accounts_total=%s | requests_total=%s | requests_succeeded=%s | requests_failed=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s",
        summary.accounts_total,
        summary.requests_total,
        summary.requests_succeeded,
        summary.requests_failed,
        summary.raw_rows,
        summary.normalized_rows,
        summary.written_rows,
    )


def orders(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    """Синхронный entrypoint для реестра задач, запускающий загрузку заказов WB."""
    asyncio.run(orders_async(date_from=date_from, date_to=date_to, account=account))
