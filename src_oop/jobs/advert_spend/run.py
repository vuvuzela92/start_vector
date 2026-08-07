from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from src_oop.jobs.advert_spend.config import DAYS_TO_CLEAN
from src_oop.jobs.advert_spend.service import AdvertSpendService

logger = logging.getLogger(__name__)


def _coerce_date(value: date | str | None, parameter_name: str) -> date | None:
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


def _resolve_period(
    date_from: date | str | None,
    date_to: date | str | None,
) -> tuple[date, date]:
    resolved_date_from = _coerce_date(date_from, "date_from")
    resolved_date_to = _coerce_date(date_to, "date_to")

    if resolved_date_from is None and resolved_date_to is None:
        today = date.today()
        return today - timedelta(days=DAYS_TO_CLEAN), today

    if resolved_date_to is None:
        resolved_date_to = resolved_date_from
    if resolved_date_from is None:
        resolved_date_from = resolved_date_to

    if resolved_date_from > resolved_date_to:
        raise ValueError("Параметр date_from не может быть позже date_to.")
    return resolved_date_from, resolved_date_to


async def advert_spend_async(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    resolved_date_from, resolved_date_to = _resolve_period(date_from, date_to)
    logger.info(
        "Старт entrypoint advert_spend_async | date_from=%s | date_to=%s | account=%s",
        resolved_date_from.isoformat(),
        resolved_date_to.isoformat(),
        account,
    )

    service = AdvertSpendService()
    summary = await service.run(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        account=account,
    )
    logger.info(
        "Завершён entrypoint advert_spend_async | accounts_total=%s | accounts_processed=%s | accounts_without_rows=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | dropped_missing_key_rows=%s | collapsed_duplicate_rows=%s",
        summary.accounts_total,
        summary.accounts_processed,
        summary.accounts_without_rows,
        summary.raw_rows,
        summary.normalized_rows,
        summary.written_rows,
        summary.dropped_missing_key_rows,
        summary.collapsed_duplicate_rows,
    )


def advert_spend(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    asyncio.run(advert_spend_async(date_from=date_from, date_to=date_to, account=account))
