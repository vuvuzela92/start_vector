"""Точки входа для выгрузки возвратов покупателей WB в Google Sheets."""

from __future__ import annotations

import asyncio
import logging

from src_oop.jobs.returns_to_customers.service import BuyersReturnsService

logger = logging.getLogger(__name__)


async def returns_to_customers_async(
    account: str | None = None,
    write_to_google: bool = True,
) -> None:
    """Запускает основной сценарий: запись в БД обязательна, Google Sheets включаются флагом."""
    await returns_to_customers_async_with_options(
        account=account,
        dry_run=False,
        write_to_google=write_to_google,
    )


async def returns_to_customers_async_with_options(
    account: str | None = None,
    dry_run: bool = False,
    write_to_google: bool = True,
) -> None:
    """Запускает сценарий возвратов в боевом или dry-run режиме с опциональной выгрузкой в Google Sheets."""
    logger.info(
        "Старт entrypoint returns_to_customers_async | account=%s | dry_run=%s | write_to_google=%s",
        account,
        dry_run,
        write_to_google,
    )
    summary = await BuyersReturnsService().run_with_options(
        account=account,
        dry_run=dry_run,
        write_to_google=write_to_google,
    )
    logger.info(
        "Завершён entrypoint returns_to_customers_async | accounts_total=%s | accounts_processed=%s | pages_received=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | db_written_rows=%s | failed_accounts=%s | dry_run=%s | write_to_google=%s",
        summary.accounts_total,
        summary.accounts_processed,
        summary.pages_received,
        summary.raw_rows,
        summary.normalized_rows,
        summary.written_rows,
        summary.db_written_rows,
        summary.failed_accounts,
        dry_run,
        write_to_google,
    )


def returns_to_customers(
    account: str | None = None,
    write_to_google: bool = True,
) -> None:
    """Запускает основной синхронный сценарий: запись в БД всегда, Google Sheets только по флагу."""
    asyncio.run(
        returns_to_customers_async(
            account=account,
            write_to_google=write_to_google,
        )
    )
