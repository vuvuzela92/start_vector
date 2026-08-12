from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from collections.abc import Sequence

import aiohttp

from src_oop.jobs.fbs_stocks.config import (
    CHRT_IDS_CHUNK_SIZE,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
    STOCKS_URL_TEMPLATE,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FBSStockFetchResult:
    """Результат чтения FBS-остатков WB по одному складу аккаунта."""

    account: str
    wb_warehouse_id: int
    stocks_by_chrt_id: dict[int, int]
    retries_used: int = 0


@dataclass(slots=True)
class FBSStockUpdateResult:
    """Результат отправки новых FBS-остатков WB по одному складу аккаунта."""

    account: str
    wb_warehouse_id: int
    sent_rows: int
    retries_used: int = 0


class WBFBSStocksClient:
    """Клиент WB Marketplace API для получения текущих остатков FBS-складов."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
        chrt_ids_chunk_size: int = CHRT_IDS_CHUNK_SIZE,
    ) -> None:
        """Настраивает HTTP-клиент для чтения остатков WB с retry и chunk-запросами."""
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds
        self.chrt_ids_chunk_size = chrt_ids_chunk_size

    async def fetch_stocks(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        chrt_ids: Sequence[int],
    ) -> FBSStockFetchResult:
        """Получает остатки WB по chrt_id для склада, чтобы обновить FBS-колонку UNIT."""
        prepared_chrt_ids = sorted({int(chrt_id) for chrt_id in chrt_ids if chrt_id})
        stocks_by_chrt_id: dict[int, int] = {}
        retries_used = 0

        for chunk_start in range(0, len(prepared_chrt_ids), self.chrt_ids_chunk_size):
            chunk = prepared_chrt_ids[chunk_start : chunk_start + self.chrt_ids_chunk_size]
            result_payload, chunk_retries = await self._request_stocks_chunk(
                session=session,
                account=account,
                token=token,
                wb_warehouse_id=wb_warehouse_id,
                chrt_ids=chunk,
            )
            retries_used += chunk_retries
            stocks_by_chrt_id.update(self._parse_stocks_payload(result_payload))

        logger.info(
            "FBS-остатки WB получены | account=%s | wb_warehouse_id=%s | requested_chrt_ids=%s | returned_rows=%s | retries_used=%s",
            account,
            wb_warehouse_id,
            len(prepared_chrt_ids),
            len(stocks_by_chrt_id),
            retries_used,
        )
        return FBSStockFetchResult(
            account=account,
            wb_warehouse_id=wb_warehouse_id,
            stocks_by_chrt_id=stocks_by_chrt_id,
            retries_used=retries_used,
        )

    async def update_stocks(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        stocks_by_chrt_id: dict[int, int],
    ) -> FBSStockUpdateResult:
        """Отправляет новые FBS-остатки WB по chrt_id для выбранного склада.

        Бизнес-правило: в WB отправляются только строки, где пользователь явно
        указал новый остаток в UNIT. Пустые значения не превращаются в ноль.
        """
        prepared_stocks = [
            {"chrtId": int(chrt_id), "amount": int(amount)}
            for chrt_id, amount in sorted(stocks_by_chrt_id.items())
        ]
        retries_used = 0

        for chunk_start in range(0, len(prepared_stocks), self.chrt_ids_chunk_size):
            chunk = prepared_stocks[chunk_start : chunk_start + self.chrt_ids_chunk_size]
            retries_used += await self._request_update_stocks_chunk(
                session=session,
                account=account,
                token=token,
                wb_warehouse_id=wb_warehouse_id,
                stocks=chunk,
            )

        logger.info(
            "Новые FBS-остатки отправлены в WB | account=%s | wb_warehouse_id=%s | rows=%s | retries_used=%s",
            account,
            wb_warehouse_id,
            len(prepared_stocks),
            retries_used,
        )
        return FBSStockUpdateResult(
            account=account,
            wb_warehouse_id=wb_warehouse_id,
            sent_rows=len(prepared_stocks),
            retries_used=retries_used,
        )

    async def _request_update_stocks_chunk(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        stocks: Sequence[dict[str, int]],
    ) -> int:
        """Выполняет один chunk PUT-запрос обновления остатков WB с защитой от временных сбоев."""
        headers = {"Authorization": token}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        url = STOCKS_URL_TEMPLATE.format(warehouse_id=wb_warehouse_id)
        json_payload = {"stocks": list(stocks)}

        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.put(
                    url,
                    headers=headers,
                    json=json_payload,
                    timeout=timeout,
                ) as response:
                    await self._read_payload(response)
                    if response.status == 429 or response.status in {500, 502, 503, 504}:
                        await self._sleep_for_retry(
                            account=account,
                            wb_warehouse_id=wb_warehouse_id,
                            attempt=attempt,
                            status=response.status,
                        )
                        continue
                    if response.status == 401:
                        raise PermissionError(
                            f"WB отклонил токен при обновлении FBS-остатков: account={account}"
                        )
                    response.raise_for_status()
                    return attempt - 1
            except PermissionError:
                raise
            except (
                asyncio.TimeoutError,
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
            ) as error:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        "Обновление FBS-остатков WB завершилось ошибкой после всех повторов: "
                        f"account={account} wb_warehouse_id={wb_warehouse_id} "
                        f"error={type(error).__name__}: {error}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    wb_warehouse_id=wb_warehouse_id,
                    attempt=attempt,
                    error=error,
                )

        raise RuntimeError(
            "Обновление FBS-остатков WB неожиданно исчерпало все попытки повтора: "
            f"account={account} wb_warehouse_id={wb_warehouse_id}"
        )

    async def _request_stocks_chunk(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        chrt_ids: Sequence[int],
    ) -> tuple[dict | list | None, int]:
        """Выполняет один chunk-запрос остатков WB, защищая сценарий от 429 и временных сбоев."""
        headers = {"Authorization": token}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        url = STOCKS_URL_TEMPLATE.format(warehouse_id=wb_warehouse_id)
        json_payload = {"chrtIds": list(chrt_ids)}

        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json=json_payload,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_payload(response)
                    if response.status == 429 or response.status in {500, 502, 503, 504}:
                        await self._sleep_for_retry(
                            account=account,
                            wb_warehouse_id=wb_warehouse_id,
                            attempt=attempt,
                            status=response.status,
                        )
                        continue
                    if response.status == 401:
                        raise PermissionError(
                            f"WB отклонил токен при чтении FBS-остатков: account={account}"
                        )
                    response.raise_for_status()
                    return payload, attempt - 1
            except PermissionError:
                raise
            except (
                asyncio.TimeoutError,
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
            ) as error:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        "Запрос FBS-остатков WB завершился ошибкой после всех повторов: "
                        f"account={account} wb_warehouse_id={wb_warehouse_id} "
                        f"error={type(error).__name__}: {error}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    wb_warehouse_id=wb_warehouse_id,
                    attempt=attempt,
                    error=error,
                )

        raise RuntimeError(
            "Запрос FBS-остатков WB неожиданно исчерпал все попытки повтора: "
            f"account={account} wb_warehouse_id={wb_warehouse_id}"
        )

    async def _read_payload(self, response: aiohttp.ClientResponse) -> dict | list | None:
        """Читает ответ WB по остаткам и сохраняет текст ошибки для диагностики."""
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            return await response.json()
        text_payload = await response.text()
        return {"text": text_payload} if text_payload else None

    def _parse_stocks_payload(self, payload: dict | list | None) -> dict[int, int]:
        """Нормализует разные формы ответа WB в маппинг `chrt_id -> quantity`."""
        if payload is None:
            return {}
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            raw_items = payload.get("stocks") or payload.get("data") or payload.get("items") or []
            items = raw_items if isinstance(raw_items, list) else []
        else:
            return {}

        stocks_by_chrt_id: dict[int, int] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            chrt_id = self._extract_int(item, ("chrtId", "chrtID", "chrt_id"))
            quantity = self._extract_int(item, ("amount", "quantity", "qty", "stock", "stocks"))
            if chrt_id is None:
                continue
            stocks_by_chrt_id[chrt_id] = quantity or 0
        return stocks_by_chrt_id

    async def _sleep_for_retry(
        self,
        account: str,
        wb_warehouse_id: int,
        attempt: int,
        status: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """Выдерживает паузу между retry, чтобы не сорвать чтение остатков лимитами WB."""
        sleep_seconds = self._calculate_retry_sleep_seconds(attempt=attempt, status=status)
        logger.warning(
            "Повторяем запрос FBS-остатков WB | account=%s | wb_warehouse_id=%s | attempt=%s/%s | status=%s | error=%s | sleep_seconds=%s",
            account,
            wb_warehouse_id,
            attempt,
            self.max_retries,
            status,
            repr(error) if error else None,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)

    def _calculate_retry_sleep_seconds(self, attempt: int, status: int | None) -> int:
        """Рассчитывает backoff для защиты чтения остатков от 429 и временных ошибок WB."""
        backoff_steps = (5, 15, 30, 60, 120)
        index = min(max(attempt - 1, 0), len(backoff_steps) - 1)
        sleep_seconds = min(backoff_steps[index], self.retry_max_sleep_seconds)
        if status == 429:
            sleep_seconds = max(sleep_seconds, 60)
        return max(sleep_seconds, self.retry_base_sleep_seconds)

    def _extract_int(
        self,
        payload: dict[str, object],
        field_names: tuple[str, ...],
    ) -> int | None:
        """Достает числовое поле WB по нескольким именам, чтобы пережить различия casing в API."""
        for field_name in field_names:
            value = payload.get(field_name)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.strip().isdigit():
                return int(value)
        return None
