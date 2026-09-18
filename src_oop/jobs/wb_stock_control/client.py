"""Асинхронный клиент чтения остатков продавца WB."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

import aiohttp

from src_oop.jobs.wb_stock_control.config import (
    CHRT_IDS_CHUNK_SIZE,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_DELAYS_SECONDS,
    STOCKS_URL_TEMPLATE,
)

logger = logging.getLogger(__name__)


class WBStockControlClient:
    """Получает опубликованные FBS-остатки WB с защитой от неполной выгрузки."""

    async def fetch_stocks(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        chrt_ids: Sequence[int],
    ) -> tuple[dict[int, int], bool]:
        """Возвращает остатки и признак полной проверки всех чанков.

        Бизнес-правило: отсутствующий `chrtId` считается нулём только при
        успешном ответе по каждому чанку. Ошибка одного чанка делает результат
        неполным и запрещает создавать ложные нулевые остатки.
        """
        prepared = sorted({int(value) for value in chrt_ids if value})
        result: dict[int, int] = {}
        for start in range(0, len(prepared), CHRT_IDS_CHUNK_SIZE):
            chunk = prepared[start : start + CHRT_IDS_CHUNK_SIZE]
            payload = await self._fetch_chunk(session, account, token, wb_warehouse_id, chunk)
            if payload is None:
                return result, False
            result.update(payload)
        return result, True

    async def _fetch_chunk(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        chrt_ids: Sequence[int],
    ) -> dict[int, int] | None:
        """Запрашивает один чанк остатков и повторяет временные ошибки."""
        url = STOCKS_URL_TEMPLATE.format(warehouse_id=wb_warehouse_id)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.post(
                    url,
                    headers={"Authorization": token},
                    json={"chrtIds": list(chrt_ids)},
                    timeout=timeout,
                ) as response:
                    if response.status == 200:
                        payload = await response.json()
                        stocks = payload.get("stocks") if isinstance(payload, dict) else None
                        if not isinstance(stocks, list):
                            logger.error("WB вернул некорректный формат остатков | account=%s | warehouse_id=%s", account, wb_warehouse_id)
                            return None
                        return {
                            int(row["chrtId"]): int(row.get("amount", 0))
                            for row in stocks
                            if isinstance(row, dict) and row.get("chrtId") is not None
                        }
                    if response.status not in {429, 500, 502, 503, 504}:
                        logger.error("WB отклонил запрос остатков | account=%s | warehouse_id=%s | status=%s", account, wb_warehouse_id, response.status)
                        return None
                    logger.warning("WB временно не принял запрос остатков, повторяем | account=%s | warehouse_id=%s | status=%s | attempt=%s", account, wb_warehouse_id, response.status, attempt)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                logger.warning("Ошибка сети при запросе остатков WB, повторяем | account=%s | warehouse_id=%s | attempt=%s", account, wb_warehouse_id, attempt)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAYS_SECONDS[min(attempt - 1, len(RETRY_DELAYS_SECONDS) - 1)])
        logger.error("Проверка остатков WB завершилась неполно | account=%s | warehouse_id=%s", account, wb_warehouse_id)
        return None
