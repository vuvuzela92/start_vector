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
    """Запускает единый сценарий отправки и автопополнения FBS-остатков из UNIT.

    Бизнес-сценарий: сначала обрабатываются ручные управляющие поля UNIT, затем выполняется проверка
    автопополнения по `Минимальный остаток` и `Сопост -> Добавляем`. Оба шага используют один режим
    применения: при `WB_FBS_APPLY_STOCKS=true` отправляют данные в WB, иначе работают как dry-run.
    """
    service = FBSStocksService()
    logger.info("Старт отправки новых FBS-остатков из UNIT в WB.")
    summary = await service.apply_new_fbs_stocks()
    logger.info(
        "Сценарий новых FBS-остатков завершен | requested_rows=%s | prepared_rows=%s | skipped_rows=%s | wb_requests=%s | cleared_cells=%s | excluded_rows=%s | refreshed_columns=%s | applied=%s",
        summary.requested_rows,
        summary.prepared_rows,
        summary.skipped_rows,
        summary.wb_requests,
        summary.cleared_cells,
        summary.excluded_rows,
        summary.refreshed_columns,
        summary.applied,
    )
    logger.info("Старт автопополнения FBS-остатков после ручной отправки UNIT.")
    auto_refill_summary = await service.auto_refill_fbs_stocks(
        apply=summary.applied,
        excluded_row_numbers=set(summary.auto_refill_excluded_row_numbers),
    )
    logger.info(
        "Сценарий автопополнения FBS-остатков после ручной отправки завершен | checked_rows=%s | triggered_rows=%s | prepared_rows=%s | skipped_rows=%s | wb_requests=%s | excluded_rows=%s | refreshed_columns=%s | applied=%s",
        auto_refill_summary.checked_rows,
        auto_refill_summary.triggered_rows,
        auto_refill_summary.prepared_rows,
        auto_refill_summary.skipped_rows,
        auto_refill_summary.wb_requests,
        auto_refill_summary.excluded_rows,
        auto_refill_summary.refreshed_columns,
        auto_refill_summary.applied,
    )


async def auto_refill_fbs_stocks_from_unit_async() -> None:
    """Запускает cron-сценарий автопополнения FBS-остатков из UNIT.

    Бизнес-сценарий: задача проверяет средний остаток на внутренний склад относительно колонки
    `Минимальный остаток` и при необходимости устанавливает значение `Добавляем` из листа `Сопост`
    на каждом активном FBS-складе.
    """
    logger.info("Старт автопополнения FBS-остатков из UNIT в WB.")
    summary = await FBSStocksService().auto_refill_fbs_stocks()
    logger.info(
        "Сценарий автопополнения FBS-остатков завершен | checked_rows=%s | triggered_rows=%s | prepared_rows=%s | skipped_rows=%s | wb_requests=%s | excluded_rows=%s | refreshed_columns=%s | applied=%s",
        summary.checked_rows,
        summary.triggered_rows,
        summary.prepared_rows,
        summary.skipped_rows,
        summary.wb_requests,
        summary.excluded_rows,
        summary.refreshed_columns,
        summary.applied,
    )


def update_fbs_stocks_in_unit() -> None:
    """Синхронный entrypoint для реестра задач, обновляющий FBS-остатки в UNIT."""
    asyncio.run(update_fbs_stocks_in_unit_async())


def apply_new_fbs_stocks_from_unit() -> None:
    """Синхронный entrypoint для отправки новых FBS-остатков из UNIT в WB."""
    asyncio.run(apply_new_fbs_stocks_from_unit_async())


def auto_refill_fbs_stocks_from_unit() -> None:
    """Синхронный entrypoint для cron-автопополнения FBS-остатков из UNIT."""
    asyncio.run(auto_refill_fbs_stocks_from_unit_async())
