"""Клиент WB API для получения заявок покупателей на возврат."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import aiohttp

from src_oop.jobs.returns_to_customers.config import (
    CLAIMS_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BuyersReturnsFetchResult:
    """Результат одной страницы возвратов для конкретного кабинета и статуса архива."""

    account: str
    is_archive: bool
    payload: dict
    retries_used: int = 0


class WBBuyersReturnsClient:
    """Получает страницы заявок на возврат и защищает job от временных сбоев WB API."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
    ) -> None:
        """Настраивает таймауты и retry для стабильного чтения возвратов со всех кабинетов."""
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds

    async def fetch_claims_page(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        is_archive: bool,
        limit: int,
        offset: int,
    ) -> BuyersReturnsFetchResult:
        """Запрашивает одну страницу возвратов и повторяет запрос при временных ошибках."""
        headers = {"Authorization": token}
        params = {
            "is_archive": str(is_archive).lower(),
            "limit": limit,
            "offset": offset,
        }
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)

        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.get(
                    CLAIMS_URL,
                    headers=headers,
                    params=params,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_payload(response)
                    if response.status == 200 and isinstance(payload, dict):
                        logger.info(
                            "Получена страница возвратов WB | account=%s | is_archive=%s | offset=%s | rows=%s | retries_used=%s",
                            account,
                            is_archive,
                            offset,
                            len(payload.get("claims", []) or []),
                            attempt - 1,
                        )
                        return BuyersReturnsFetchResult(
                            account=account,
                            is_archive=is_archive,
                            payload=payload,
                            retries_used=attempt - 1,
                        )

                    error_detail = self._extract_error_detail(payload)
                    if response.status in {401, 403}:
                        raise PermissionError(
                            "WB отклонил доступ к возвратам покупателей: "
                            f"account={account} status={response.status} detail={error_detail}"
                        )

                    if response.status in {400, 429, 500, 502, 503, 504} and attempt < self.max_retries:
                        await self._sleep_for_retry(
                            account=account,
                            attempt=attempt,
                            status=response.status,
                            error_detail=error_detail,
                        )
                        continue

                    response.raise_for_status()
                    raise RuntimeError(
                        "Запрос возвратов покупателей WB завершился ошибкой: "
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
                        "Запрос возвратов покупателей WB завершился ошибкой после всех повторов: "
                        f"account={account} error_type={type(error).__name__} error={error}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    attempt=attempt,
                    error_detail=f"{type(error).__name__}: {error}",
                )

        raise RuntimeError(
            "Запрос возвратов покупателей WB исчерпал все попытки повтора: "
            f"account={account}"
        )

    async def _read_payload(
        self,
        response: aiohttp.ClientResponse,
    ) -> dict | list | str | None:
        """Читает JSON, если WB отдал JSON, иначе возвращает текст для диагностики."""
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            return await response.json()
        return await response.text()

    def _extract_error_detail(self, payload: dict | list | str | None) -> str:
        """Извлекает понятную причину ошибки из тела ответа WB для логов job."""
        if isinstance(payload, dict):
            detail = payload.get("detail") or payload.get("errorText") or payload.get("title")
            return str(detail) if detail is not None else str(payload)
        return str(payload)

    async def _sleep_for_retry(
        self,
        account: str,
        attempt: int,
        status: int | None = None,
        error_detail: str | None = None,
    ) -> None:
        """Делает паузу между повторами, чтобы пережить таймауты и ограничение 429."""
        sleep_seconds = min(
            max(self.retry_base_sleep_seconds * attempt, self.retry_base_sleep_seconds),
            self.retry_max_sleep_seconds,
        )
        logger.warning(
            "Повтор запроса возвратов покупателей WB | account=%s | attempt=%s/%s | status=%s | error=%s | sleep_seconds=%s",
            account,
            attempt,
            self.max_retries,
            status,
            error_detail,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)
