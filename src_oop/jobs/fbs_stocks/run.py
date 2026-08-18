from __future__ import annotations

import asyncio
import logging

from src_oop.jobs.fbs_stocks.service import FBSStocksService

logger = logging.getLogger(__name__)


async def update_fbs_stocks_in_unit_async() -> None:
    """Запускает обновление текущих FBS-остатков WB в тестовой UNIT-таблице.

    Бизнес-сценарий: задача перечитывает фактические остатки по всем активным
    внутренним складам аккаунта и обновляет поле `ФБС общий остаток` в UNIT,
    чтобы ручные и автоматические сценарии опирались на актуальные данные WB.
    """
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
    """Запускает только ручной сценарий отправки новых FBS-остатков из UNIT.

    Бизнес-сценарий: задача читает управляющие поля `Новый остаток для всех
    складов` и `Новый остаток Вешки`, отправляет новые остатки в WB, затем
    очищает успешно примененные ячейки и обновляет `ФБС общий остаток` из WB.
    Проверка `Минимальный остаток` и автопополнение выполняются отдельной
    задачей, чтобы ручная команда не смешивалась с cron-логикой.
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


async def auto_refill_fbs_stocks_from_unit_async() -> None:
    """Запускает cron-сценарий автопополнения FBS-остатков из UNIT.

    Бизнес-сценарий: задача проверяет средний остаток на внутренний склад
    относительно колонки `Минимальный остаток` и при необходимости устанавливает
    значение `Добавляем` из листа `Сопост` на каждом активном FBS-складе.
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
    """Синхронный entrypoint для обновления текущих FBS-остатков в UNIT.

    Бизнес-сценарий: запускает сервис чтения текущих остатков WB и записи
    актуального `ФБС общий остаток` в тестовую таблицу UNIT.
    """
    asyncio.run(update_fbs_stocks_in_unit_async())


def apply_new_fbs_stocks_from_unit() -> None:
    """Синхронный entrypoint для ручной отправки новых FBS-остатков из UNIT.

    Бизнес-сценарий: запускает только ручное применение значений из колонок
    `Новый остаток для всех складов` и `Новый остаток Вешки`, без автопополнения
    по минимальному остатку в этом же прогоне.
    """
    asyncio.run(apply_new_fbs_stocks_from_unit_async())


def auto_refill_fbs_stocks_from_unit() -> None:
    """Синхронный entrypoint для cron-автопополнения FBS-остатков из UNIT.

    Бизнес-сценарий: запускает отдельную проверку минимального остатка и при
    необходимости массово пополняет внутренние склады по данным листа `Сопост`.
    """
    asyncio.run(auto_refill_fbs_stocks_from_unit_async())
