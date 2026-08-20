from __future__ import annotations

import asyncio
import logging
from datetime import date

from src_oop.jobs.funnel_sales.service import FunnelSalesService

logger = logging.getLogger(__name__)


def _coerce_date(value: date | str | None, parameter_name: str) -> date | None:
    """Приводит параметр даты entrypoint к `date` для запуска выгрузки daily funnel."""
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
    raise TypeError(f"Параметр {parameter_name} должен иметь тип date, str или None.")


async def funnel_sales_async(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    """Запускает async-entrypoint выгрузки ежедневной воронки продаж WB в таблицу funnel_daily.

    Бизнес-сценарий: получить по всем или одному кабинету ежедневную воронку WB
    за выбранный период, нормализовать метрики карточки и продаж, затем выполнить
    upsert в PostgreSQL по ключу `(nm_id, date)`.
    """
    resolved_date_from = _coerce_date(date_from, "date_from")
    resolved_date_to = _coerce_date(date_to, "date_to")

    logger.info(
        "Старт entrypoint funnel_sales_async | date_from=%s | date_to=%s | account=%s",
        resolved_date_from.isoformat() if resolved_date_from else None,
        resolved_date_to.isoformat() if resolved_date_to else None,
        account,
    )
    summary = await FunnelSalesService().run(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        account=account,
    )
    logger.info(
        "Завершён entrypoint funnel_sales_async | accounts_total=%s | report_dates_total=%s | requests_total=%s | requests_succeeded=%s | requests_failed=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | removed_exact_duplicates=%s | collapsed_business_duplicates=%s | excluded_rows=%s",
        summary.accounts_total,
        summary.report_dates_total,
        summary.requests_total,
        summary.requests_succeeded,
        summary.requests_failed,
        summary.raw_rows,
        summary.normalized_rows,
        summary.written_rows,
        summary.removed_exact_duplicates,
        summary.collapsed_business_duplicates,
        summary.excluded_rows,
    )


def funnel_sales(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    """Синхронный entrypoint для реестра задач, запускающий выгрузку daily funnel WB."""
    asyncio.run(funnel_sales_async(date_from=date_from, date_to=date_to, account=account))
