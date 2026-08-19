"""Клиент WB API для получения баланса продавца."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import aiohttp

from src_oop.jobs.seller_balance.config import (
    BALANCE_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SellerBalanceFetchResult:
    """Хранит результат запроса баланса одного кабинета.

    Бизнес-сценарий:
    сервису важно знать не только сам баланс, но и сколько повторов
    потребовалось, чтобы корректно обновить общую витрину ДДС.
    """

    account: str
    payload: dict[str, object]
    retries_used: int = 0


class WBSellerBalanceClient:
    """Получает баланс продавца из WB API с retry на временные ошибки.

    Бизнес-сценарий:
    job обслуживает финансовую витрину, поэтому клиент должен переживать
    таймауты, сетевые сбои и `429`, но не скрывать постоянные ошибки доступа.
    """

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
    ) -> None:
        """Настраивает таймауты и повторы для запроса баланса продавца."""
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds

    async def fetch_balance(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
    ) -> SellerBalanceFetchResult:
        """Запрашивает баланс одного кабинета и повторяет запрос при временных ошибках.

        Бизнес-сценарий:
        метод возвращает только подтверждённые документацией поля виджета
        баланса продавца: валюта, текущий баланс и сумма к выводу.
        """
        headers = {"Authorization": token}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)

        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.get(
                    BALANCE_URL,
                    headers=headers,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_payload(response)
                    if response.status == 200 and isinstance(payload, dict):
                        logger.info(
                            "Получен баланс продавца WB | account=%s | currency=%s | retries_used=%s",
                            account,
                            payload.get("currency"),
                            attempt - 1,
                        )
                        return SellerBalanceFetchResult(
                            account=account,
                            payload=payload,
                            retries_used=attempt - 1,
                        )

                    error_detail = self._extract_error_detail(payload)
                    if response.status in {401, 402, 403}:
                        raise PermissionError(
                            "WB отклонил доступ к балансу продавца: "
                            f"account={account} status={response.status} detail={error_detail}"
                        )

                    if response.status in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                        await self._sleep_for_retry(
                            account=account,
                            attempt=attempt,
                            status=response.status,
                            error_detail=error_detail,
                        )
                        continue

                    response.raise_for_status()
                    raise RuntimeError(
                        "Запрос баланса продавца WB завершился ошибкой: "
                        f"account={account} status={response.status} detail={error_detail}"
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
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        "Запрос баланса продавца WB завершился ошибкой после всех повторов: "
                        f"account={account} error_type={type(error).__name__}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    attempt=attempt,
                    error_detail=type(error).__name__,
                )

        raise RuntimeError(
            f"Запрос баланса продавца WB исчерпал все попытки повтора: account={account}"
        )

    async def _read_payload(
        self,
        response: aiohttp.ClientResponse,
    ) -> dict[str, object] | list[object] | str | None:
        """Читает JSON или текст ответа WB для безопасной диагностики.

        Бизнес-сценарий:
        при сбоях job должна видеть причину ответа WB, не логируя заголовки
        запроса и другие чувствительные данные.
        """
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            payload = await response.json()
            if isinstance(payload, dict):
                return payload
            if isinstance(payload, list):
                return payload
        return await response.text()

    def _extract_error_detail(
        self,
        payload: dict[str, object] | list[object] | str | None,
    ) -> str:
        """Извлекает безопасное описание ошибки из ответа WB.

        Бизнес-сценарий:
        этот текст попадает в логи job и нужен для быстрой диагностики причин,
        по которым баланс не обновился в ДДС.
        """
        if isinstance(payload, dict):
            detail = payload.get("detail") or payload.get("message") or payload.get("title")
            return str(detail) if detail is not None else str(payload)
        return str(payload)

    async def _sleep_for_retry(
        self,
        account: str,
        attempt: int,
        status: int | None = None,
        error_detail: str | None = None,
    ) -> None:
        """Делает паузу между повторами после `429` и сетевых ошибок.

        Бизнес-сценарий:
        метод баланса ограничен одним запросом в минуту на кабинет, поэтому
        пауза должна быть достаточно длинной, чтобы следующая попытка имела
        шанс пройти, а не упереться в тот же лимит мгновенно.
        """
        sleep_seconds = min(
            max(self.retry_base_sleep_seconds * attempt, self.retry_base_sleep_seconds),
            self.retry_max_sleep_seconds,
        )
        logger.warning(
            "Повтор запроса баланса продавца WB | account=%s | attempt=%s/%s | status=%s | error=%s | sleep_seconds=%s",
            account,
            attempt,
            self.max_retries,
            status,
            error_detail,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)
