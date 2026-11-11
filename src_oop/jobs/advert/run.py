from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from src_oop.jobs.advert.service import AdvertStatsRunSummary, AdvertStatsService

logger = logging.getLogger(__name__)


class AdvertStatsRunError(RuntimeError):
    """Сообщает CLI о неполном результате после завершения обработки всех доступных аккаунтов."""

    def __init__(self, summary: AdvertStatsRunSummary) -> None:
        """Сохраняет сводку для вызывающего кода, не скрывая уже записанные данные."""
        self.summary = summary
        super().__init__(
            "Сбор рекламной статистики WB завершён с ошибками: "
            f"аккаунтов с ошибками={len(summary.failed_accounts)}, "
            f"ошибочных batch={len(summary.failed_batches)}, "
            f"записано строк={summary.written_rows}."
        )


def _coerce_date(value: date | str | None, parameter_name: str) -> date | None:
    """Проверяет дату запуска, чтобы статистика запрашивалась только за заданный период."""
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
    """Разрешает даты по умолчанию во вчера и проверяет порядок бизнес-периода отчёта."""
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
    """Запускает полный сбор WB за период и сигнализирует CLI о частичном результате."""
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
        "Завершён entrypoint advert_stat_async | accounts_total=%s | accounts_processed=%s | campaign_count_total=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | planned_batches=%s | http_attempts=%s | responses_429=%s | api_errors=%s | normalization_errors=%s | database_errors=%s | duration_seconds=%.2f | dropped_missing_key_rows=%s | collapsed_duplicate_rows=%s",
        summary.accounts_total,
        summary.accounts_processed,
        summary.campaign_count_total,
        summary.raw_rows,
        summary.normalized_rows,
        summary.written_rows,
        summary.planned_batches,
        summary.http_attempts,
        summary.responses_429,
        summary.api_errors,
        summary.normalization_errors,
        summary.database_errors,
        summary.duration_seconds,
        summary.dropped_missing_key_rows,
        summary.collapsed_duplicate_rows,
    )
    if summary.failed_accounts or summary.failed_batches:
        raise AdvertStatsRunError(summary)


def advert_stat(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    account: str | None = None,
) -> None:
    """Сохраняет CLI-контракт python main.py advert_stat с ненулевым кодом при ошибках."""
    asyncio.run(advert_stat_async(date_from=date_from, date_to=date_to, account=account))
