"""Асинхронный клиент API сборочных заданий Wildberries."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

import aiohttp

from src_oop.jobs.assembly_info.config import AssemblyInfoSettings

logger = logging.getLogger(__name__)


class AssemblyInfoClient:
    """Получает сборочные задания и статусы с ограничением нагрузки на API WB."""

    def __init__(self, settings: AssemblyInfoSettings | None = None) -> None:
        """Создает клиент с едиными таймаутами, retry и лимитом параллельности."""
        self.settings = settings or AssemblyInfoSettings.from_env()
        self._semaphore = asyncio.Semaphore(self.settings.concurrency)

    async def fetch_orders(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
    ) -> list[dict[str, object]]:
        """Загружает все сборочные задания аккаунта за доступный период WB."""
        result: list[dict[str, object]] = []
        async with self._semaphore:
            next_cursor = 0
            while True:
                payload = await self._request(
                    session, self.settings.orders_url, account, token,
                    params={"limit": 1000, "next": next_cursor},
                )
                if payload is None:
                    break
                orders = payload.get("orders", [])
                if not isinstance(orders, list):
                    logger.error("WB вернул некорректный список сборочных заданий | account=%s", account)
                    break
                result.extend({**order, "account": account} for order in orders if isinstance(order, dict))
                next_cursor = int(payload.get("next", 0) or 0)
                if not next_cursor:
                    break
                await asyncio.sleep(self.settings.request_delay_seconds)
        logger.info("Сборочные задания загружены | account=%s | rows=%s", account, len(result))
        return result

    async def fetch_statuses(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        order_ids: Sequence[int],
    ) -> list[dict[str, object]]:
        """Загружает статусы сборочных заданий чанками не более 1000 ID."""
        result: list[dict[str, object]] = []
        async with self._semaphore:
            for start in range(0, len(order_ids), self.settings.chunk_size):
                chunk = list(order_ids[start : start + self.settings.chunk_size])
                payload = await self._request(
                    session, self.settings.statuses_url, account, token,
                    json={"orders": chunk}, method="post",
                )
                if payload is not None:
                    statuses = payload.get("orders", [])
                    if isinstance(statuses, list):
                        result.extend({**status, "account": account} for status in statuses if isinstance(status, dict))
                await asyncio.sleep(self.settings.request_delay_seconds)
        logger.info("Статусы сборочных заданий загружены | account=%s | rows=%s", account, len(result))
        return result

    async def _request(self, session: aiohttp.ClientSession, url: str, account: str,
                       token: str, *, method: str = "get", **kwargs: object) -> dict | None:
        """Выполняет запрос WB с повтором временных ошибок и без вывода токена."""
        timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout_seconds)
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                async with session.request(
                    method, url, headers={"Authorization": token}, timeout=timeout, **kwargs
                ) as response:
                    if response.status == 200:
                        payload = await response.json()
                        return payload if isinstance(payload, dict) else None
                    if response.status in {401, 400} or 400 <= response.status < 500 and response.status != 429:
                        logger.error("WB отклонил запрос сборочных заданий | account=%s | status=%s", account, response.status)
                        return None
                    logger.warning("WB вернул временную ошибку | account=%s | status=%s | attempt=%s", account, response.status, attempt)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                logger.warning("Ошибка сети при запросе сборочных заданий, повторяем | account=%s | attempt=%s", account, attempt)
            if attempt < self.settings.max_retries:
                await asyncio.sleep(min(2 ** (attempt - 1), 10))
        logger.error("Запрос сборочных заданий не выполнен после повторов | account=%s", account)
        return None
