from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date

import aiohttp

from src_oop.jobs.advert_spend.config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
    SPEND_URL,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AdvertSpendFetchResult:
    account: str
    payload: list[dict]
    retries_used: int = 0


class WBAdvertSpendClient:
    """Клиент WB API для получения рекламных затрат по аккаунтам."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
    ) -> None:
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds

    async def fetch_account_spend(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        date_from: date,
        date_to: date,
    ) -> AdvertSpendFetchResult:
        """Получает расходы рекламы за период для одного аккаунта WB."""
        headers = {"Authorization": token}
        params = {
            "from": date_from.isoformat(),
            "to": date_to.isoformat(),
        }
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)

        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.get(
                    SPEND_URL,
                    headers=headers,
                    params=params,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_json_payload(response)

                    if response.status == 200:
                        rows = payload if isinstance(payload, list) else []
                        for row in rows:
                            if isinstance(row, dict):
                                row["account"] = account.upper()

                        logger.info(
                            "Получены рекламные затраты WB | account=%s | date_from=%s | date_to=%s | rows=%s | retries_used=%s",
                            account,
                            date_from.isoformat(),
                            date_to.isoformat(),
                            len(rows),
                            attempt - 1,
                        )
                        return AdvertSpendFetchResult(
                            account=account,
                            payload=[row for row in rows if isinstance(row, dict)],
                            retries_used=attempt - 1,
                        )

                    error_detail = self._extract_error_detail(payload)
                    if response.status == 401:
                        raise PermissionError(
                            f"WB token rejected for account={account}: {error_detail}"
                        )

                    if response.status in {400, 429, 500, 502, 503, 504}:
                        if attempt < self.max_retries:
                            await self._sleep_for_retry(
                                account=account,
                                attempt=attempt,
                                status=response.status,
                                error_detail=error_detail,
                            )
                            continue

                    response.raise_for_status()
                    raise RuntimeError(
                        f"WB spend request failed for account={account}: status={response.status} detail={error_detail}"
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
                        f"WB spend request failed after retries for account={account}: {type(error).__name__}: {error}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    attempt=attempt,
                    error_detail=f"{type(error).__name__}: {error}",
                )

        raise RuntimeError(f"WB spend request exhausted retries for account={account}")

    async def _read_json_payload(self, response: aiohttp.ClientResponse) -> dict | list | str | None:
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            return await response.json()
        return await response.text()

    def _extract_error_detail(self, payload: dict | list | str | None) -> str:
        if isinstance(payload, dict):
            detail = payload.get("detail")
            return str(detail) if detail is not None else str(payload)
        return str(payload)

    async def _sleep_for_retry(
        self,
        account: str,
        attempt: int,
        status: int | None = None,
        error_detail: str | None = None,
    ) -> None:
        sleep_seconds = min(
            max(self.retry_base_sleep_seconds * attempt, self.retry_base_sleep_seconds),
            self.retry_max_sleep_seconds,
        )
        logger.warning(
            "Повтор запроса рекламных затрат WB | account=%s | attempt=%s/%s | status=%s | error=%s | sleep_seconds=%s",
            account,
            attempt,
            self.max_retries,
            status,
            error_detail,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)
