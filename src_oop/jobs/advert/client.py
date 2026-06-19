from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
import time

import aiohttp

from src_oop.jobs.advert.config import (
    ADVERTS_URL,
    CAMPAIGN_BATCH_SIZE,
    CAMPAIGN_STATUSES,
    FULLSTATS_URL,
    FULLSTATS_INTERVAL_SECONDS,
    MAX_RETRIES,
    MAX_FULLSTATS_RANGE_DAYS,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
    SUPPORTED_BID_TYPES,
)

logger = logging.getLogger(__name__)


def chunk_campaign_ids(
    campaign_ids: Sequence[int],
    chunk_size: int = CAMPAIGN_BATCH_SIZE,
) -> list[list[int]]:
    """Разбивает campaign IDs на пачки до 50 элементов."""
    if chunk_size <= 0:
        raise ValueError("chunk_size должен быть положительным числом.")
    return [
        list(campaign_ids[index : index + chunk_size])
        for index in range(0, len(campaign_ids), chunk_size)
    ]


def chunk_date_range(
    date_from: date,
    date_to: date,
    max_days: int = MAX_FULLSTATS_RANGE_DAYS,
) -> list[tuple[date, date]]:
    """Разбивает период на интервалы не длиннее 31 дня."""
    if max_days <= 0:
        raise ValueError("max_days должен быть положительным числом.")
    if date_from > date_to:
        raise ValueError("date_from не может быть позже date_to.")

    chunks: list[tuple[date, date]] = []
    current_date_from = date_from
    max_delta = timedelta(days=max_days - 1)
    while current_date_from <= date_to:
        current_date_to = min(current_date_from + max_delta, date_to)
        chunks.append((current_date_from, current_date_to))
        current_date_from = current_date_to + timedelta(days=1)
    return chunks


@dataclass(slots=True)
class _AccountRateLimiter:
    """Ограничивает частоту fullstats-запросов внутри одного seller account."""

    min_interval_seconds: int
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _next_allowed_at: float = 0.0

    async def wait_turn(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_allowed_at:
                await asyncio.sleep(self._next_allowed_at - now)
                now = time.monotonic()
            self._next_allowed_at = now + self.min_interval_seconds


@dataclass(slots=True)
class RequestExecutionResult:
    payload: dict | list | None
    retries_used: int
    attempts_made: int
    failed: bool = False
    error_message: str | None = None


@dataclass(slots=True)
class FailedAdvertBatch:
    account: str
    date_from: str
    date_to: str
    ids_count: int
    ids_sample: list[int]
    attempts_made: int
    error_message: str


@dataclass(slots=True)
class FullstatsFetchResult:
    payload: list[dict] = field(default_factory=list)
    failed_batches: list[FailedAdvertBatch] = field(default_factory=list)
    retries_used: int = 0
    requests_total: int = 0
    requests_failed: int = 0


@dataclass(slots=True)
class CampaignIdsFetchResult:
    campaign_ids: list[int]
    retries_used: int = 0


class WBAdvertStatsClient:
    """Клиент WB API для получения списка кампаний и их ежедневной статистики."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
        fullstats_interval_seconds: int = FULLSTATS_INTERVAL_SECONDS,
        campaign_batch_size: int = CAMPAIGN_BATCH_SIZE,
        max_fullstats_range_days: int = MAX_FULLSTATS_RANGE_DAYS,
    ) -> None:
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds
        self.fullstats_interval_seconds = fullstats_interval_seconds
        self.campaign_batch_size = campaign_batch_size
        self.max_fullstats_range_days = max_fullstats_range_days
        self._fullstats_limiters_by_account: dict[str, _AccountRateLimiter] = {}

    async def fetch_campaign_ids(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
    ) -> CampaignIdsFetchResult:
        """Возвращает список рекламных кампаний нужных типов для аккаунта."""
        headers = {"Authorization": token}
        campaign_ids: set[int] = set()

        result = await self._request_json(
            session=session,
            method="GET",
            url=ADVERTS_URL,
            headers=headers,
            params={"statuses": ",".join(str(status_id) for status_id in CAMPAIGN_STATUSES)},
            account=account,
            request_name="campaign_list",
        )
        if result.failed or not isinstance(result.payload, dict):
            raise RuntimeError(
                f"Не удалось получить список кампаний WB для account={account}: {result.error_message}"
            )

        adverts = result.payload.get("adverts", [])
        for advert in adverts:
            if advert.get("bid_type") not in SUPPORTED_BID_TYPES:
                continue
            advert_id = advert.get("id")
            if isinstance(advert_id, int):
                campaign_ids.add(advert_id)

        result = sorted(campaign_ids)
        logger.info(
            "Получен список кампаний WB | account=%s | campaign_ids=%s",
            account,
            len(result),
        )
        return CampaignIdsFetchResult(
            campaign_ids=result,
            retries_used=result.retries_used if isinstance(result, RequestExecutionResult) else 0,
        )

    async def fetch_fullstats_chunk(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        campaign_ids: Sequence[int],
        date_from: date,
        date_to: date,
    ) -> FullstatsFetchResult:
        """Получает полную статистику WB за один date chunk по списку кампаний."""
        if not campaign_ids:
            logger.info(
                "Список кампаний пустой, запрос fullstats пропущен | account=%s | date_from=%s | date_to=%s",
                account,
                date_from.isoformat(),
                date_to.isoformat(),
            )
            return FullstatsFetchResult()

        headers = {"Authorization": token}
        campaign_chunks = chunk_campaign_ids(
            campaign_ids=campaign_ids,
            chunk_size=self.campaign_batch_size,
        )
        fetch_result = FullstatsFetchResult(requests_total=len(campaign_chunks))

        logger.info(
            "Старт batched fullstats chunk | account=%s | campaign_ids=%s | campaign_chunks=%s | requests_total=%s | date_from=%s | date_to=%s",
            account,
            len(campaign_ids),
            len(campaign_chunks),
            fetch_result.requests_total,
            date_from.isoformat(),
            date_to.isoformat(),
        )

        for campaign_chunk in campaign_chunks:
            await self._wait_fullstats_turn(account)
            batch_context = {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "ids_count": len(campaign_chunk),
                "ids_sample": list(campaign_chunk[:5]),
            }
            params = {
                "ids": ",".join(str(campaign_id) for campaign_id in campaign_chunk),
                "beginDate": date_from.isoformat(),
                "endDate": date_to.isoformat(),
            }
            result = await self._request_json(
                session=session,
                method="GET",
                url=FULLSTATS_URL,
                headers=headers,
                params=params,
                account=account,
                request_name="fullstats",
                batch_context=batch_context,
                allow_failure=True,
            )
            fetch_result.retries_used += result.retries_used

            if result.failed:
                fetch_result.requests_failed += 1
                fetch_result.failed_batches.append(
                    FailedAdvertBatch(
                        account=account,
                        date_from=date_from.isoformat(),
                        date_to=date_to.isoformat(),
                        ids_count=len(campaign_chunk),
                        ids_sample=list(campaign_chunk[:5]),
                        attempts_made=result.attempts_made,
                        error_message=result.error_message or "Неизвестная ошибка",
                    )
                )
                logger.error(
                    "Не удалось получить batch fullstats после всех retry | account=%s | date_from=%s | date_to=%s | ids_count=%s | ids_sample=%s | attempts=%s | error=%s",
                    account,
                    date_from.isoformat(),
                    date_to.isoformat(),
                    len(campaign_chunk),
                    list(campaign_chunk[:5]),
                    result.attempts_made,
                    result.error_message,
                )
                continue

            if isinstance(result.payload, list):
                for item in result.payload:
                    if not isinstance(item, dict):
                        continue
                    item["account"] = account
                    fetch_result.payload.append(item)

        logger.info(
            "Получена статистика WB за chunk | account=%s | date_from=%s | date_to=%s | rows=%s | requests_total=%s | requests_failed=%s | retries_used=%s",
            account,
            date_from.isoformat(),
            date_to.isoformat(),
            len(fetch_result.payload),
            fetch_result.requests_total,
            fetch_result.requests_failed,
            fetch_result.retries_used,
        )
        return fetch_result

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, str | int],
        account: str,
        request_name: str,
        batch_context: dict[str, object] | None = None,
        allow_failure: bool = False,
    ) -> RequestExecutionResult:
        """Выполняет HTTP-запрос с retry/backoff и JSON-ответом."""
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)

        last_error_message: str | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_json_payload(response)

                    if response.status == 429:
                        await self._sleep_for_retry(
                            account=account,
                            request_name=request_name,
                            attempt=attempt,
                            status=response.status,
                            batch_context=batch_context,
                        )
                        continue

                    if response.status in {500, 502, 503, 504}:
                        await self._sleep_for_retry(
                            account=account,
                            request_name=request_name,
                            attempt=attempt,
                            status=response.status,
                            batch_context=batch_context,
                        )
                        continue

                    if response.status == 400:
                        logger.warning(
                            "WB вернул 400, повторов не будет | account=%s | request=%s | params=%s | payload=%s | batch_context=%s",
                            account,
                            request_name,
                            params,
                            payload,
                            batch_context,
                        )
                        return RequestExecutionResult(
                            payload={} if request_name == "campaign_list" else [],
                            retries_used=attempt - 1,
                            attempts_made=attempt,
                        )

                    if response.status == 401:
                        raise PermissionError(
                            f"WB token rejected for account={account} request={request_name}"
                        )

                    response.raise_for_status()
                    return RequestExecutionResult(
                        payload=payload,
                        retries_used=attempt - 1,
                        attempts_made=attempt,
                    )
            except PermissionError:
                raise
            except (
                asyncio.TimeoutError,
                TimeoutError,
                ConnectionResetError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
            ) as error:
                last_error_message = f"{type(error).__name__}: {error}"
                if attempt >= self.max_retries:
                    if allow_failure:
                        return RequestExecutionResult(
                            payload=None,
                            retries_used=attempt - 1,
                            attempts_made=attempt,
                            failed=True,
                            error_message=last_error_message,
                        )
                    raise RuntimeError(
                        f"WB request failed after retries: account={account} request={request_name} error={last_error_message}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    request_name=request_name,
                    attempt=attempt,
                    error=error,
                    batch_context=batch_context,
                )

        exhausted_message = (
            f"WB request exhausted retries unexpectedly: account={account} request={request_name}"
        )
        if allow_failure:
            return RequestExecutionResult(
                payload=None,
                retries_used=max(self.max_retries - 1, 0),
                attempts_made=self.max_retries,
                failed=True,
                error_message=last_error_message or exhausted_message,
            )
        raise RuntimeError(exhausted_message)

    async def _read_json_payload(self, response: aiohttp.ClientResponse) -> dict | list:
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            return await response.json()

        text_payload = await response.text()
        return {"text": text_payload}

    async def _sleep_for_retry(
        self,
        account: str,
        request_name: str,
        attempt: int,
        status: int | None = None,
        error: Exception | None = None,
        batch_context: dict[str, object] | None = None,
    ) -> None:
        sleep_seconds = self._calculate_retry_sleep_seconds(
            attempt=attempt,
            request_name=request_name,
            status=status,
        )

        logger.warning(
            "Повтор WB запроса | account=%s | request=%s | attempt=%s/%s | status=%s | error=%s | sleep_seconds=%s | batch_context=%s",
            account,
            request_name,
            attempt,
            self.max_retries,
            status,
            repr(error) if error else None,
            sleep_seconds,
            batch_context,
        )
        await asyncio.sleep(sleep_seconds)

    async def _wait_fullstats_turn(self, account: str) -> None:
        limiter = self._fullstats_limiters_by_account.get(account)
        if limiter is None:
            limiter = _AccountRateLimiter(
                min_interval_seconds=self.fullstats_interval_seconds
            )
            self._fullstats_limiters_by_account[account] = limiter
        await limiter.wait_turn()

    def _calculate_retry_sleep_seconds(
        self,
        attempt: int,
        request_name: str,
        status: int | None,
    ) -> int:
        backoff_steps = (5, 15, 30, 60, 120)
        index = min(max(attempt - 1, 0), len(backoff_steps) - 1)
        sleep_seconds = min(backoff_steps[index], self.retry_max_sleep_seconds)

        if status == 429 or request_name == "fullstats":
            sleep_seconds = max(sleep_seconds, self.fullstats_interval_seconds)

        return max(sleep_seconds, self.retry_base_sleep_seconds)
