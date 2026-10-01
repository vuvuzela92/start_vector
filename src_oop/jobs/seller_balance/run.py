"""Точки входа для выгрузки баланса продавцов WB в Google Sheets."""

from __future__ import annotations

import asyncio
import logging

from src_oop.jobs.seller_balance.service import SellerBalanceService

logger = logging.getLogger(__name__)


async def seller_balance_async(account: str | None = None) -> None:
    """Запускает полный бизнес-сценарий обновления баланса и начислений в ДДС.

    Бизнес-сценарий:
    entrypoint получает начисления из PostgreSQL, собирает актуальный баланс
    по одному или всем кабинетам и публикует оба блока во вкладку `Переменные`.
    """
    summary = await SellerBalanceService().run(account=account)
    logger.info(
        "Завершён entrypoint seller_balance_async | accounts_total=%s | accounts_processed=%s | balance_rows=%s | financial_rows=%s | failed_accounts=%s",
        summary.accounts_total,
        summary.accounts_processed,
        summary.written_rows,
        summary.financial_rows,
        summary.failed_accounts,
    )


def seller_balance_run(account: str | None = None) -> None:
    """Запускает синхронную оболочку для выгрузки баланса и начислений.

    Бизнес-сценарий:
    функция нужна для интеграции с существующим реестром задач, который
    ожидает обычный callable и не управляет event loop самостоятельно.
    """
    asyncio.run(seller_balance_async(account=account))
