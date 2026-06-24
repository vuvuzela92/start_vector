from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from datetime import date

from src_oop.jobs.bukh_docs.models import JobRunResult
from src_oop.jobs.bukh_docs.service import BukhDocsService

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


async def get_bukh_docs_async(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    tokens_by_account: Mapping[str, str] | None = None,
) -> JobRunResult:
    resolved_date_from = _coerce_date(date_from, "date_from")
    resolved_date_to = _coerce_date(date_to, "date_to")

    service = BukhDocsService()
    result = await service.run(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        tokens_by_account=tokens_by_account,
    )
    logger.info(
        "Завершен запуск bukh_docs: accounts=%s documents_found=%s written_rows=%s errors=%s",
        result.accounts_total,
        result.documents_found,
        result.written_rows,
        len(result.errors),
    )
    return result


def get_bukh_docs(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    tokens_by_account: Mapping[str, str] | None = None,
) -> JobRunResult:
    return asyncio.run(
        get_bukh_docs_async(
            date_from=date_from,
            date_to=date_to,
            tokens_by_account=tokens_by_account,
        )
    )
