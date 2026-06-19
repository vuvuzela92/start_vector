from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from src_oop.jobs.advert.service import AdvertStatsService

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
        yesterday = date.today() - timedelta(days=1)
        return yesterday, yesterday

    if resolved_date_to is None:
        resolved_date_to = resolved_date_from
    if resolved_date_from is None:
        resolved_date_from = resolved_date_to

    if resolved_date_from > resolved_date_to:
        raise ValueError("Параметр date_from не может быть позже date_to.")
    return resolved_date_from, resolved_date_to


async def advert_stat_async(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    resolved_date_from, resolved_date_to = _resolve_period(date_from, date_to)
    logger.info(
        "Старт entrypoint advert_stat_async | date_from=%s | date_to=%s | account=%s",
        resolved_date_from.isoformat(),
        resolved_date_to.isoformat(),
        account,
    )

    service = AdvertStatsService()
    summary = await service.run(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        account=account,
    )
    logger.info(
        "Завершён entrypoint advert_stat_async | accounts_total=%s | accounts_processed=%s | campaign_count_total=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | dropped_missing_key_rows=%s | collapsed_duplicate_rows=%s",
        summary.accounts_total,
        summary.accounts_processed,
        summary.campaign_count_total,
        summary.raw_rows,
        summary.normalized_rows,
        summary.written_rows,
        summary.dropped_missing_key_rows,
        summary.collapsed_duplicate_rows,
    )


def advert_stat(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    asyncio.run(advert_stat_async(date_from=date_from, date_to=date_to, account=account))
