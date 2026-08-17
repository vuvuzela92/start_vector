"""Асинхронный клиент WB API для отчёта «Лента заказов»."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from datetime import datetime
from email.utils import parsedate_to_datetime

import aiohttp
from pydantic import ValidationError
from src_oop.jobs.orders_feed.config import (
    MAX_RETRIES,
    ORDER_FEED_URL,
    PAGE_LIMIT,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
)
from src_oop.jobs.orders_feed.exceptions import (
    OrderFeedAPIError,
    OrderFeedAuthenticationError,
    OrderFeedAuthorizationError,
    OrderFeedBadRequestError,
    OrderFeedRateLimitError,
    OrderFeedResponseValidationError,
    OrderFeedTransportError,
    OrderFeedUpstreamError,
)
from src_oop.jobs.orders_feed.models import (
    OrderFeedPaginationRequest,
    OrderFeedRequest,
)
from src_oop.jobs.orders_feed.schemas.api import (
    OrderFeedAPIErrorResponse,
    OrderFeedResponse,
)
from src_oop.jobs.orders_feed.schemas.internal import OrderFeedPage, OrderFeedPeriod

logger = logging.getLogger(__name__)


class WBOrderFeedClient:
    """Получает страницы неизменяемого снимка Order Feed для одного кабинета WB."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
        page_limit: int = PAGE_LIMIT,
    ) -> None:
        """Настраивает таймауты, повторы и размер страницы регулярной загрузки."""
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds
        self.page_limit = page_limit

    async def fetch_page(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        period: OrderFeedPeriod,
        offset: int = 0,
        snapshot_time: str | None = None,
    ) -> OrderFeedPage:
        """Получает одну страницу заказов, фиксируя snapshotTime для целостной пагинации."""
        body = self._build_request_body(period, offset, snapshot_time)
        headers = {"Authorization": token}

        for attempt in range(1, self.max_retries + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
                async with session.post(
                    ORDER_FEED_URL,
                    headers=headers,
                    json=body,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_json_payload(response)
                    if response.status == 429 or response.status in {
                        500,
                        502,
                        503,
                        504,
                    }:
                        if attempt >= self.max_retries:
                            raise self._api_error(response.status, account, payload)
                        await self._sleep_for_retry(
                            account, offset, attempt, response.status, response.headers
                        )
                        continue
                    if response.status >= 400:
                        raise self._api_error(response.status, account, payload)
                    response.raise_for_status()
                    return self._parse_page(account, offset, payload, attempt - 1)
            except (OrderFeedAPIError, OrderFeedResponseValidationError):
                raise
            except aiohttp.ClientResponseError:
                raise
            except (
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
                RuntimeError,
            ) as error:
                if attempt >= self.max_retries:
                    raise OrderFeedTransportError(
                        "Запрос Order Feed завершился ошибкой после всех повторов: "
                        f"account={account} offset={offset} error={error}"
                    ) from error
                await self._sleep_for_retry(account, offset, attempt, error=error)

        raise OrderFeedTransportError(
            f"Запрос Order Feed неожиданно исчерпал повторы: account={account} offset={offset}"
        )

    def _api_error(
        self,
        status: int,
        account: str,
        payload: object,
    ) -> OrderFeedAPIError:
        """Преобразует разные форматы ошибок WB в типизированное исключение."""
        parsed = (
            OrderFeedAPIErrorResponse.model_validate(payload)
            if isinstance(payload, dict)
            else None
        )
        title = parsed.title if parsed else None
        detail = parsed.detail if parsed else None
        request_id = parsed.request_id if parsed else None
        error_class: type[OrderFeedAPIError]
        if status == 400:
            error_class = OrderFeedBadRequestError
        elif status == 401:
            error_class = OrderFeedAuthenticationError
        elif status in {402, 403}:
            error_class = OrderFeedAuthorizationError
        elif status == 429:
            error_class = OrderFeedRateLimitError
        else:
            error_class = OrderFeedUpstreamError
        diagnostic = detail or title or "WB не передал описание ошибки"
        return error_class(
            f"Ошибка WB Order Feed: account={account} status={status} "
            f"request_id={request_id or '-'} detail={diagnostic}",
            status=status,
            account=account,
            request_id=request_id,
            detail=detail,
            response_body=payload,
        )

    def _build_request_body(
        self,
        period: OrderFeedPeriod,
        offset: int,
        snapshot_time: str | None,
    ) -> OrderFeedRequest:
        """Формирует запрос без фильтров, чтобы бизнес-витрина получила все заказы кабинета."""
        pagination: OrderFeedPaginationRequest = {
            "offset": offset,
            "limit": self.page_limit,
        }
        if snapshot_time:
            pagination["snapshotTime"] = snapshot_time
        return {
            "selectedPeriod": {
                "start": period.start.isoformat(timespec="milliseconds"),
                "end": period.end.isoformat(timespec="milliseconds"),
            },
            "nmIds": [],
            "subjectIds": [],
            "brandNames": [],
            "tagIds": [],
            "pagination": pagination,
        }

    def _parse_page(
        self,
        account: str,
        offset: int,
        payload: object,
        retries_used: int,
    ) -> OrderFeedPage:
        """Валидирует всю страницу через Pydantic до нормализации и записи в PostgreSQL."""
        try:
            validated = OrderFeedResponse.model_validate(payload)
        except ValidationError as error:
            raise OrderFeedResponseValidationError(
                "WB вернул Order Feed, не соответствующий контракту API: "
                f"{error.error_count()} ошибок валидации; "
                f"details={error.errors(include_url=False, include_input=False)}"
            ) from error

        response_snapshot_time = validated.data.snapshot_time
        if response_snapshot_time is None:
            snapshot_time = None
            logger.info(
                "WB вернул поседнюю страницу ленты заказов."
                "| account=%s | offset=%s | rows=%s",
                account,
                offset,
                len(validated.data.orders),
            )
        else:
            snapshot_time = response_snapshot_time.isoformat().replace("+00:00", "Z")
        return OrderFeedPage(
            account=account,
            snapshot_time=snapshot_time,
            currency=validated.data.currency,
            orders=validated.data.orders,
            offset=offset,
            limit=self.page_limit,
            retries_used=retries_used,
        )

    async def _read_json_payload(self, response: aiohttp.ClientResponse) -> object:
        """Читает JSON и сохраняет фрагмент ответа WB в диагностике невалидного тела."""
        try:
            return await response.json()
        except (aiohttp.ContentTypeError, ValueError) as error:
            response_text = await response.text()
            raise RuntimeError(
                f"WB вернул не JSON для Order Feed: status={response.status} body={response_text[:500]}"
            ) from error

    async def _sleep_for_retry(
        self,
        account: str,
        offset: int,
        attempt: int,
        status: int | None = None,
        headers: Mapping[str, str] | None = None,
        error: Exception | None = None,
    ) -> None:
        """Соблюдает Retry-After и backoff, чтобы временный сбой не оборвал загрузку кабинета."""
        sleep_seconds = self._retry_delay(attempt, headers)
        logger.warning(
            "Запрос Order Feed будет повторён | account=%s | offset=%s | attempt=%s/%s | status=%s | error=%s | sleep_seconds=%s",
            account,
            offset,
            attempt,
            self.max_retries,
            status,
            repr(error) if error else None,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)

    def _retry_delay(
        self,
        attempt: int,
        headers: Mapping[str, str] | None,
    ) -> float:
        """Вычисляет безопасную паузу повтора с приоритетом серверного Retry-After."""
        retry_after = headers.get("Retry-After") if headers else None
        if retry_after:
            try:
                return min(max(float(retry_after), 0.0), self.retry_max_sleep_seconds)
            except TypeError, ValueError:
                try:
                    retry_at = parsedate_to_datetime(str(retry_after))
                    now = datetime.now(tz=retry_at.tzinfo)
                    return min(
                        max((retry_at - now).total_seconds(), 0.0),
                        self.retry_max_sleep_seconds,
                    )
                except TypeError, ValueError, OverflowError:
                    pass
        delay = self.retry_base_sleep_seconds * (2 ** max(attempt - 1, 0))
        return min(delay, self.retry_max_sleep_seconds)
