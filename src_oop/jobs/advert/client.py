from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import math
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
    validate_advert_config,
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

    min_interval_seconds: float
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _next_allowed_at: float = 0.0

    async def wait_turn(self, minimum_delay: float = 0) -> float:
        """Резервирует следующий безопасный старт fullstats с учётом серверного retry-after."""
        async with self._lock:
            now = time.monotonic()
            wait_seconds = max(self._next_allowed_at - now, minimum_delay, 0)
            if wait_seconds:
                await asyncio.sleep(wait_seconds)
                now = time.monotonic()
            self._next_allowed_at = now + self.min_interval_seconds
            return wait_seconds


@dataclass(slots=True)
class RequestExecutionResult:
    payload: dict | list | None
    retries_used: int
    attempts_made: int
    failed: bool = False
    error_message: str | None = None
    responses_429: int = 0


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
    attempts_total: int = 0
    responses_429: int = 0
    api_errors: int = 0


@dataclass(slots=True)
class CampaignIdsFetchResult:
    campaign_ids: list[int]
    retries_used: int = 0
    attempts_made: int = 1
    responses_429: int = 0


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
        """Настраивает HTTP-повторы и лимиты, защищая WB от превышения допустимой нагрузки."""
        validate_advert_config()
        if request_timeout_seconds <= 0 or max_retries <= 0:
            raise ValueError("Таймаут и число HTTP-попыток должны быть положительными.")
        if not 1 <= campaign_batch_size <= 50:
            raise ValueError("Размер пачки кампаний должен быть от 1 до 50.")
        if not 1 <= max_fullstats_range_days <= 31 or fullstats_interval_seconds <= 0:
            raise ValueError("Интервал fullstats и диапазон дат заданы некорректно.")
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

        request_result = await self._request_json(
            session=session,
            method="GET",
            url=ADVERTS_URL,
            headers=headers,
            params={"statuses": ",".join(str(status_id) for status_id in CAMPAIGN_STATUSES)},
            account=account,
            request_name="campaign_list",
        )
        if request_result.failed or not isinstance(request_result.payload, dict):
            raise RuntimeError(
                f"Не удалось получить список кампаний WB для account={account}: {request_result.error_message}"
            )

        adverts = request_result.payload.get("adverts")
        if not isinstance(adverts, list):
            raise RuntimeError(f"Ответ списка кампаний WB имеет неверную структуру | account={account}")
        for advert in adverts:
            if not isinstance(advert, dict):
                continue
            if advert.get("bid_type") not in SUPPORTED_BID_TYPES:
                continue
            advert_id = advert.get("id")
            if isinstance(advert_id, int):
                campaign_ids.add(advert_id)

        sorted_campaign_ids = sorted(campaign_ids)
        logger.info(
            "Получен список кампаний WB | account=%s | campaign_ids=%s",
            account,
            len(sorted_campaign_ids),
        )
        return CampaignIdsFetchResult(
            campaign_ids=sorted_campaign_ids,
            retries_used=request_result.retries_used,
            attempts_made=request_result.attempts_made,
            responses_429=request_result.responses_429,
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
            fetch_result.attempts_total += result.attempts_made
            fetch_result.responses_429 += result.responses_429
            fetch_result.api_errors += int(result.failed)

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
        """Выполняет HTTP-запросы WB с ограничением частоты и retry без утечки секретов."""
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)

        last_error_message: str | None = None
        responses_429 = 0
        retry_after_delay = 0.0

        for attempt in range(1, self.max_retries + 1):
            if request_name == "fullstats":
                await self._wait_fullstats_turn(account, minimum_delay=retry_after_delay)
                retry_after_delay = 0.0
            try:
                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=timeout,
                ) as response:
                    status = response.status
                    retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
                    payload = await self._read_json_payload(response) if status < 400 else None
                    response_content_type = response.headers.get("Content-Type", "не указан")
                    response_content_length = response.content_length

                if status in {401, 403}:
                    raise PermissionError(f"Доступ WB запрещён | account={account} | request={request_name} | HTTP {status}")
                if status == 400:
                    message = f"WB отклонил запрос | HTTP 400 | request={request_name}"
                    logger.warning("%s | account=%s | batch=%s; обработка продолжается с остальными пачками", message, account, batch_context)
                    return RequestExecutionResult(None, attempt - 1, attempt, failed=True, error_message=message, responses_429=responses_429)
                if status == 429:
                    responses_429 += 1
                elif status in {500, 502, 503, 504}:
                    pass
                elif status >= 400:
                    message = f"Ошибка API WB | HTTP {status} | request={request_name}"
                    return RequestExecutionResult(None, attempt - 1, attempt, failed=True, error_message=message, responses_429=responses_429)
                else:
                    expected_payload = isinstance(payload, list) if request_name == "fullstats" else isinstance(payload, dict)
                    if not expected_payload:
                        message = f"Ответ API WB имеет неверную JSON-структуру | request={request_name} | тип={type(payload).__name__}"
                        logger.error(
                            "Нестандартный формат ответа WB; batch не будет считаться успешным | account=%s | request=%s | HTTP=%s | content_type=%s | payload_type=%s | batch=%s",
                            account,
                            request_name,
                            status,
                            response_content_type,
                            type(payload).__name__,
                            batch_context,
                        )
                        if payload is None:
                            logger.error(
                                "Ответ WB декодирован как JSON null | account=%s | request=%s | HTTP=%s | content_type=%s | content_length=%s",
                                account,
                                request_name,
                                status,
                                response_content_type,
                                response_content_length,
                            )
                        return RequestExecutionResult(None, attempt - 1, attempt, failed=True, error_message=message, responses_429=responses_429)
                    if request_name == "campaign_list" and not isinstance(payload.get("adverts"), list):
                        message = "Ответ списка кампаний WB не содержит массив adverts"
                        return RequestExecutionResult(None, attempt - 1, attempt, failed=True, error_message=message, responses_429=responses_429)
                    return RequestExecutionResult(payload, attempt - 1, attempt, responses_429=responses_429)

                if attempt >= self.max_retries:
                    break
                backoff = self._calculate_retry_sleep_seconds(attempt, request_name, status)
                delay = max(backoff, retry_after or 0)
                if request_name != "fullstats":
                    logger.warning("Повтор запроса WB | account=%s | request=%s | HTTP %s | задержка=%s сек.", account, request_name, status, delay)
                    await asyncio.sleep(delay)
                else:
                    retry_after_delay = delay
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
                last_error_message = f"Сетевая ошибка WB ({type(error).__name__})"
                if attempt >= self.max_retries:
                    if allow_failure:
                        return RequestExecutionResult(None, attempt - 1, attempt, failed=True, error_message=last_error_message, responses_429=responses_429)
                    raise RuntimeError(f"Запрос WB завершился ошибкой после повторов | account={account} | request={request_name} | тип={type(error).__name__}") from None
                delay = self._calculate_retry_sleep_seconds(attempt, request_name, None)
                if request_name != "fullstats":
                    logger.warning("Повтор запроса WB после сетевой ошибки | account=%s | request=%s | тип=%s | задержка=%s сек.", account, request_name, type(error).__name__, delay)
                    await asyncio.sleep(delay)
                else:
                    retry_after_delay = delay

        exhausted_message = (
            f"Запрос WB неожиданно исчерпал все попытки повтора: "
            f"account={account} request={request_name}"
        )
        if allow_failure:
            return RequestExecutionResult(
                payload=None,
                retries_used=max(self.max_retries - 1, 0),
                attempts_made=self.max_retries,
                failed=True,
                error_message=last_error_message or exhausted_message,
                responses_429=responses_429,
            )
        raise RuntimeError(exhausted_message)

    async def _read_json_payload(self, response: aiohttp.ClientResponse) -> dict | list | None:
        """Читает JSON, если он объявлен типом содержимого или распознаётся по первому байту."""
        content_type = response.headers.get("Content-Type", "")
        media_type = content_type.split(";", maxsplit=1)[0].strip().lower()
        if media_type == "application/json" or media_type.endswith("+json"):
            payload = await response.json(content_type=None)
            if payload is None:
                logger.warning(
                    "JSON-ответ WB декодирован в null | HTTP=%s | content_type=%s | content_length=%s",
                    response.status,
                    media_type,
                    response.content_length,
                )
            return payload

        body = await response.read()
        if body.lstrip().startswith((b"[", b"{")):
            try:
                return await response.json(content_type=None)
            except (aiohttp.ContentTypeError, ValueError):
                logger.warning(
                    "Тело ответа WB похоже на JSON, но не разбирается | HTTP=%s | content_type=%s | bytes=%s",
                    response.status,
                    media_type or "не указан",
                    len(body),
                )
        return None

    async def _wait_fullstats_turn(self, account: str, minimum_delay: float = 0) -> float:
        """Ожидает индивидуальный слот аккаунта перед каждой HTTP-попыткой fullstats."""
        limiter = self._fullstats_limiters_by_account.get(account)
        if limiter is None:
            limiter = _AccountRateLimiter(
                min_interval_seconds=self.fullstats_interval_seconds
            )
            self._fullstats_limiters_by_account[account] = limiter
        return await limiter.wait_turn(minimum_delay=minimum_delay)

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        """Преобразует Retry-After в задержку секунд, поддерживая секунды и HTTP-date."""
        if not value:
            return None
        try:
            seconds = float(value)
            if not math.isfinite(seconds):
                return None
            return max(seconds, 0)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return max((retry_at - datetime.now(timezone.utc)).total_seconds(), 0)
            except (TypeError, ValueError, OverflowError):
                return None

    def _calculate_retry_sleep_seconds(
        self,
        attempt: int,
        request_name: str,
        status: int | None,
    ) -> int:
        """Рассчитывает экспоненциальный backoff, соблюдая минимальный интервал fullstats."""
        backoff_steps = (5, 15, 30, 60, 120)
        index = min(max(attempt - 1, 0), len(backoff_steps) - 1)
        sleep_seconds = min(backoff_steps[index], self.retry_max_sleep_seconds)

        if status == 429 or request_name == "fullstats":
            sleep_seconds = max(sleep_seconds, self.fullstats_interval_seconds)

        return max(sleep_seconds, self.retry_base_sleep_seconds)
