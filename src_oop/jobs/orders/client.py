from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date

import aiohttp

from src_oop.jobs.orders.config import (
    MAX_RETRIES,
    ORDERS_URL,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class OrdersFetchResult:
    """Результат запроса заказов WB по одному кабинету и одной дате."""

    account: str
    date_from: date
    payload: list[dict]
    retries_used: int


class WBOrdersClient:
    """Клиент WB Statistics API для получения заказов по кабинетам продавцов."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
    ) -> None:
        """Настраивает HTTP-клиент для бизнес-сценария регулярной загрузки заказов WB."""
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds

    async def fetch_orders(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        date_from: date,
    ) -> OrdersFetchResult:
        """Получает заказы WB с флагом выгрузки изменений за конкретную дату.

        Бизнес-правило: запрос идет с `flag=1` чтобы
        API вернул заказы, измененные начиная с `date_from`.
        """
        headers = {"Authorization": token}
        params = {"dateFrom": date_from.isoformat(), "flag": 1}

        for attempt in range(1, self.max_retries + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
                async with session.get(
                    ORDERS_URL,
                    headers=headers,
                    params=params,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_json_payload(response)

                    if response.status == 429 or response.status in {500, 502, 503, 504}:
                        await self._sleep_for_retry(
                            account=account,
                            date_from=date_from,
                            attempt=attempt,
                            status=response.status,
                        )
                        continue

                    if response.status == 401:
                        raise PermissionError(
                            f"WB отклонил токен при загрузке заказов: account={account}"
                        )

                    response.raise_for_status()
                    if not isinstance(payload, list):
                        raise RuntimeError(
                            "WB вернул неожиданный формат заказов: ожидался список."
                        )

                    logger.info(
                        "Получены заказы WB | account=%s | date_from=%s | rows=%s | retries_used=%s",
                        account,
                        date_from.isoformat(),
                        len(payload),
                        attempt - 1,
                    )
                    return OrdersFetchResult(
                        account=account,
                        date_from=date_from,
                        payload=payload,
                        retries_used=attempt - 1,
                    )
            except PermissionError:
                raise
            except (
                asyncio.TimeoutError,
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
                RuntimeError,
            ) as error:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        "Запрос заказов WB завершился ошибкой после всех повторов: "
                        f"account={account} date_from={date_from.isoformat()} "
                        f"error={type(error).__name__}: {error}"
                    ) from error

                await self._sleep_for_retry(
                    account=account,
                    date_from=date_from,
                    attempt=attempt,
                    error=error,
                )

        raise RuntimeError(
            "Запрос заказов WB неожиданно исчерпал все попытки повтора: "
            f"account={account} date_from={date_from.isoformat()}"
        )

    async def _read_json_payload(self, response: aiohttp.ClientResponse) -> object:
        """Читает JSON-ответ WB и защищает сценарий от невалидного тела ответа."""
        try:
            return await response.json()
        except aiohttp.ContentTypeError as error:
            text_payload = await response.text()
            raise RuntimeError(
                "WB вернул не JSON при загрузке заказов: "
                f"status={response.status} body={text_payload[:500]}"
            ) from error

    async def _sleep_for_retry(
        self,
        account: str,
        date_from: date,
        attempt: int,
        status: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """Выдерживает паузу между повторами, чтобы не сорвать загрузку из-за временных сбоев WB."""
        sleep_seconds = self._calculate_retry_sleep_seconds(attempt=attempt, status=status)
        logger.warning(
            "Повторяем запрос заказов WB | account=%s | date_from=%s | attempt=%s/%s | status=%s | error=%s | sleep_seconds=%s",
            account,
            date_from.isoformat(),
            attempt,
            self.max_retries,
            status,
            repr(error) if error else None,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)

    def _calculate_retry_sleep_seconds(self, attempt: int, status: int | None) -> int:
        """Рассчитывает паузу retry для защиты сценария от 429 и временных ошибок WB."""
        backoff_steps = (5, 15, 30, 60, 120)
        index = min(max(attempt - 1, 0), len(backoff_steps) - 1)
        sleep_seconds = min(backoff_steps[index], self.retry_max_sleep_seconds)
        if status == 429:
            sleep_seconds = max(sleep_seconds, 60)
        return max(sleep_seconds, self.retry_base_sleep_seconds)
