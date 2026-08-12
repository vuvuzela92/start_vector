from __future__ import annotations

import asyncio
import logging

from src_oop.jobs.fbs_stocks.service import FBSStocksService

logger = logging.getLogger(__name__)


async def update_fbs_stocks_in_unit_async() -> None:
    """Запускает обновление текущих FBS-остатков WB в тестовой UNIT-таблице."""
    logger.info("Старт обновления FBS-остатков в UNIT.")
    summary = await FBSStocksService().update_current_fbs_stocks()
    logger.info(
        "FBS-остатки в UNIT обновлены | unit_rows=%s | articles_with_chrt_id=%s | wb_requests=%s | updated_columns=%s",
        summary.unit_rows,
        summary.articles_with_chrt_id,
        summary.wb_requests,
        summary.updated_columns,
    )


async def apply_new_fbs_stocks_from_unit_async() -> None:
    """Запускает подготовку или отправку новых FBS-остатков из UNIT в WB."""
    logger.info("Старт отправки новых FBS-остатков из UNIT в WB.")
    summary = await FBSStocksService().apply_new_fbs_stocks()
    logger.info(
        "Сценарий новых FBS-остатков завершен | requested_rows=%s | prepared_rows=%s | skipped_rows=%s | wb_requests=%s | applied=%s",
        summary.requested_rows,
        summary.prepared_rows,
        summary.skipped_rows,
        summary.wb_requests,
        summary.applied,
    )


def update_fbs_stocks_in_unit() -> None:
    """Синхронный entrypoint для реестра задач, обновляющий FBS-остатки в UNIT."""
    asyncio.run(update_fbs_stocks_in_unit_async())


def apply_new_fbs_stocks_from_unit() -> None:
    """Синхронный entrypoint для отправки новых FBS-остатков из UNIT в WB."""
    asyncio.run(apply_new_fbs_stocks_from_unit_async())
