"""Entrypoint новой выгрузки агрегированных дневных WMS-остатков."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from src_oop.jobs.wms_stocks.api_client import WMSStockService
from src_oop.jobs.wms_stocks.config import (
    WMS_STOCK_BACKFILL_START_DATE,
    WMS_STOCK_FBS_LOCATION_ID,
    WMS_STOCK_LOOKBACK_DAYS,
)
from src_oop.jobs.wms_stocks.process import Process
from src_oop.jobs.wms_stocks.repository import WMSStockRepository

logger = logging.getLogger(__name__)


def _format_date(value: datetime) -> str:
    """Преобразует `datetime` в строку `YYYY-MM-DD` для вызова WMS API.

    Бизнес-сценарий:
    и регулярная выгрузка, и backfill отправляют период в одном формате, чтобы
    WMS API получал предсказуемые параметры без различий между режимами запуска.
    """
    return value.strftime("%Y-%m-%d")


async def wms_stock_run(date_from: str | None = None, date_to: str | None = None) -> None:
    """Обновляет `public.wms_stock` за последние 7 дней или за переданный период.

    Бизнес-сценарий:
    job несколько раз в день перечитывает короткое скользящее окно и отдельно
    проходит каждый день периода, чтобы дневные остатки по товарам были
    актуальны и не пропускали даты при ретроизменениях источника WMS.
    """
    today = datetime.now()
    resolved_date_to = date_to or _format_date(today)
    resolved_date_from = date_from or _format_date(today - timedelta(days=WMS_STOCK_LOOKBACK_DAYS))

    logger.info(
        "Запущено обновление дневных WMS-остатков | date_from=%s | date_to=%s",
        resolved_date_from,
        resolved_date_to,
    )
    service = WMSStockService()
    total_data = await service.fetch_daily_balances(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
    )
    fbs_data = await service.fetch_daily_balances(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        location_id=WMS_STOCK_FBS_LOCATION_ID,
        include_subtree=True,
    )
    processor = Process(total_data)
    total_dataframe = processor.process_daily_balances(quantity_column_name="stock_qty")
    fbs_dataframe = Process(fbs_data).process_daily_balances(quantity_column_name="fbs")
    dataframe = processor.merge_daily_balance_frames(total_dataframe, fbs_dataframe)
    save_result = WMSStockRepository().save(dataframe)
    logger.info(
        "Обновление дневных WMS-остатков завершено | input_rows=%s | written_rows=%s | fbs_location_id=%s",
        save_result.input_rows,
        save_result.written_rows,
        WMS_STOCK_FBS_LOCATION_ID,
    )


async def wms_stock_backfill_run(
    date_from: str | None = None,
    date_to: str | None = None,
) -> None:
    """Запускает отдельный backfill `public.wms_stock` с 2026-07-29 или за указанный период.

    Бизнес-сценарий:
    исторический режим нужен для первоначального наполнения новой таблицы и для
    управляемого дозабора пропущенных интервалов без влияния на штатную короткую
    выгрузку последних 7 дней. Период также обходится подневно, чтобы таблица
    гарантированно пыталась получить данные за каждую дату backfill-окна.
    """
    today = datetime.now()
    resolved_date_from = date_from or WMS_STOCK_BACKFILL_START_DATE.isoformat()
    resolved_date_to = date_to or _format_date(today)

    logger.info(
        "Запущен backfill дневных WMS-остатков | date_from=%s | date_to=%s",
        resolved_date_from,
        resolved_date_to,
    )
    service = WMSStockService()
    total_data = await service.fetch_daily_balances(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
    )
    fbs_data = await service.fetch_daily_balances(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        location_id=WMS_STOCK_FBS_LOCATION_ID,
        include_subtree=True,
    )
    processor = Process(total_data)
    total_dataframe = processor.process_daily_balances(quantity_column_name="stock_qty")
    fbs_dataframe = Process(fbs_data).process_daily_balances(quantity_column_name="fbs")
    dataframe = processor.merge_daily_balance_frames(total_dataframe, fbs_dataframe)
    save_result = WMSStockRepository().save(dataframe)
    logger.info(
        "Backfill дневных WMS-остатков завершен | input_rows=%s | written_rows=%s | fbs_location_id=%s",
        save_result.input_rows,
        save_result.written_rows,
        WMS_STOCK_FBS_LOCATION_ID,
    )
