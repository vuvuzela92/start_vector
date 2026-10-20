"""Точка запуска дозагрузки товаров поставок WB."""

from __future__ import annotations

import asyncio
import logging

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.wb_missing_supplies_goods.service import WBSuppliesGoodsService

logger = logging.getLogger(__name__)


async def wb_missing_supplies_goods_async() -> None:
    """Запускает первичную и повторную загрузку товаров поставок WB.

    Сценарий получает свежие поставки всех ЛК, загружает товары новых поставок
    целиком, а для уже известных поставок добавляет только отсутствующие строки.
    """
    tokens = load_api_tokens()
    results = await WBSuppliesGoodsService().run(tokens)
    logger.info(
        "Сценарий первичной и повторной загрузки поставок завершён | accounts=%s | supplies=%s | supplies_saved=%s | goods_inserted=%s | goods_updated=%s",
        len(results),
        sum(result.supplies_checked for result in results),
        sum(result.supplies_saved for result in results),
        sum(result.goods_inserted for result in results),
        sum(result.goods_updated for result in results),
    )


def wb_missing_supplies_goods_run() -> None:
    """Запускает асинхронный сценарий дозагрузки товаров поставок из cron/task runner."""
    asyncio.run(wb_missing_supplies_goods_async())
