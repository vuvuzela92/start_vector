"""Асинхронный клиент API поставок Wildberries."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from src_oop.jobs.wb_missing_supplies_goods.config import (
    GOODS_PAGE_SIZE,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_DELAY_SECONDS,
    SUPPLIES_URL,
    SUPPLY_DETAIL_URL_TEMPLATE,
    SUPPLY_GOODS_URL_TEMPLATE,
    SUPPLY_PAGE_SIZE,
)

logger = logging.getLogger(__name__)


class WBSuppliesClient:
    """Получает список поставок и товары поставок для одного аккаунта WB."""

    def __init__(
        self,
        timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        """Настраивает таймаут и число повторов для внешнего API WB."""
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.max_retries = max_retries

    async def fetch_recent_supplies(
        self,
        session: aiohttp.ClientSession,
        token: str,
    ) -> list[dict[str, Any]]:
        """Возвращает все страницы списка поставок аккаунта без хранения их в БД."""
        payload = {"dates": [{"type": "createDate"}]}
        result: list[dict[str, Any]] = []
        offset = 0
        while True:
            batch = await self._request_json(
                session,
                "POST",
                SUPPLIES_URL,
                token,
                params={"limit": SUPPLY_PAGE_SIZE, "offset": offset},
                json=payload,
            )
            if not isinstance(batch, list):
                return result
            result.extend(
                {**row, "ID": row.get("supplyID")}
                for row in batch
                if isinstance(row, dict) and row.get("supplyID")
            )
            if len(batch) < SUPPLY_PAGE_SIZE:
                return result
            offset += SUPPLY_PAGE_SIZE

    async def fetch_supply_goods(
        self,
        session: aiohttp.ClientSession,
        token: str,
        supply_id: int,
    ) -> list[dict[str, Any]]:
        """Возвращает все товары одной поставки, добавляя к каждой строке её ID."""
        result: list[dict[str, Any]] = []
        offset = 0
        url = SUPPLY_GOODS_URL_TEMPLATE.format(supply_id=supply_id)
        while True:
            batch = await self._request_json(
                session,
                "GET",
                url,
                token,
                params={
                    "limit": GOODS_PAGE_SIZE,
                    "offset": offset,
                    "isPreorderID": "false",
                },
            )
            if not isinstance(batch, list):
                return result
            result.extend({**row, "ID": supply_id} for row in batch if isinstance(row, dict))
            if len(batch) < GOODS_PAGE_SIZE:
                return result
            offset += GOODS_PAGE_SIZE

    async def fetch_supply_details(
        self,
        session: aiohttp.ClientSession,
        token: str,
        supply_id: int,
    ) -> dict[str, Any] | None:
        """Получает полную поставку с обязательными для ``wb_supplies`` полями."""
        url = SUPPLY_DETAIL_URL_TEMPLATE.format(supply_id=supply_id)
        payload = await self._request_json(
            session,
            "GET",
            url,
            token,
            params={"isPreorderID": "false"},
        )
        if not isinstance(payload, dict):
            return None
        return {**payload, "ID": supply_id}

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        token: str,
        *,
        params: dict[str, Any],
        json: dict[str, Any] | None = None,
    ) -> Any:
        """Выполняет запрос WB с retry для временных ошибок и HTTP 429."""
        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.request(
                    method,
                    url,
                    headers={"Authorization": token, "Content-Type": "application/json"},
                    params=params,
                    json=json,
                    timeout=self.timeout,
                ) as response:
                    if response.status == 429 or response.status >= 500:
                        if attempt < self.max_retries:
                            await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)
                            continue
                    if response.status >= 400:
                        logger.error(
                            "Запрос API поставок WB завершился ошибкой | method=%s | status=%s",
                            method,
                            response.status,
                        )
                        return []
                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                if attempt >= self.max_retries:
                    logger.error(
                        "Запрос API поставок WB не выполнен после повторов | method=%s | error_type=%s",
                        method,
                        type(error).__name__,
                    )
                    return []
                await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)
        return []
