from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date

import aiohttp

from src_oop.jobs.funnel_sales.config import (
    FUNNEL_URL,
    MAX_RETRIES,
    PAGE_LIMIT,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FunnelFetchResult:
    """Результат загрузки воронки WB по одному кабинету за один день."""

    account: str
    report_date: date
    payload: list[dict]
    retries_used: int


class WBFunnelSalesClient:
    """Клиент WB Sales Funnel API для подневной выгрузки товаров по кабинетам."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
        page_limit: int = PAGE_LIMIT,
    ) -> None:
        """Настраивает HTTP-клиент ежедневной воронки без скрытого глобального состояния."""
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds
        self.page_limit = page_limit

    async def fetch_daily_funnel(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        report_date: date,
    ) -> FunnelFetchResult:
        """Получает полную воронку WB за день с постраничной догрузкой.

        Бизнес-логика: один запрос относится к одному кабинету и одной дате, а
        пагинация продолжается, пока WB возвращает страницу целиком. Каждый товар
        помечается кабинетом, чтобы запись в БД сохраняла источник метрики.
        """
        headers = {"Authorization": token}
        offset = 0
        products: list[dict] = []
        retries_used = 0
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)

        while True:
            payload = self._build_payload(report_date=report_date, offset=offset)
            page_products, page_retries = await self._request_page(
                session=session,
                headers=headers,
                account=account,
                report_date=report_date,
                payload=payload,
                timeout=timeout,
            )
            retries_used += page_retries

            if not page_products:
                break

            for product in page_products:
                if isinstance(product, dict):
                    product["account"] = account
            products.extend(page_products)

            logger.info(
                "Получена страница воронки WB | account=%s | report_date=%s | offset=%s | page_rows=%s | total_rows=%s",
                account,
                report_date.isoformat(),
                offset,
                len(page_products),
                len(products),
            )

            if len(page_products) < self.page_limit:
                break
            offset += len(page_products)

        return FunnelFetchResult(
            account=account,
            report_date=report_date,
            payload=products,
            retries_used=retries_used,
        )

    def _build_payload(self, report_date: date, offset: int) -> dict[str, object]:
        """Собирает тело запроса daily funnel в формате, ожидаемом WB API.

        Бизнес-логика: daily-выгрузка всегда запрашивает один завершённый день,
        поэтому `selectedPeriod.start` и `selectedPeriod.end` совпадают.
        """
        iso_date = report_date.isoformat()
        return {
            "selectedPeriod": {
                "start": iso_date,
                "end": iso_date,
            },
            "limit": self.page_limit,
            "offset": offset,
        }

    async def _request_page(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        account: str,
        report_date: date,
        payload: dict[str, object],
        timeout: aiohttp.ClientTimeout,
    ) -> tuple[list[dict], int]:
        """Выполняет один запрос страницы воронки WB с retry.

        Бизнес-логика: временная ошибка одного кабинета или одной страницы не
        должна сразу ронять весь сценарий, поэтому запрос повторяется с backoff,
        а окончательная ошибка поднимается только после исчерпания попыток.
        """
        retries_used = 0
        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.post(
                    FUNNEL_URL,
                    headers=headers,
                    json=payload,
                    timeout=timeout,
                ) as response:
                    response_payload = await self._read_json_payload(response)

                    if response.status == 200:
                        products = response_payload.get("data", {}).get("products", [])
                        if not isinstance(products, list):
                            raise RuntimeError("WB вернул неожиданный формат products в воронке.")
                        return products, retries_used

                    if response.status in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                        retries_used += 1
                        await self._sleep_for_retry(
                            account=account,
                            report_date=report_date,
                            attempt=attempt,
                            status=response.status,
                        )
                        continue

                    if response.status in {400, 401, 403}:
                        raise PermissionError(
                            "WB отклонил запрос воронки по кабинету: "
                            f"account={account} status={response.status}"
                        )

                    response.raise_for_status()
                    return [], retries_used
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
                        "Запрос ежедневной воронки WB завершился ошибкой после всех повторов: "
                        f"account={account} report_date={report_date.isoformat()} "
                        f"error_type={type(error).__name__}"
                    ) from error

                retries_used += 1
                await self._sleep_for_retry(
                    account=account,
                    report_date=report_date,
                    attempt=attempt,
                    error=error,
                )

        raise RuntimeError(
            "Запрос ежедневной воронки WB неожиданно исчерпал повторы: "
            f"account={account} report_date={report_date.isoformat()}"
        )

    async def _read_json_payload(self, response: aiohttp.ClientResponse) -> dict:
        """Читает JSON-ответ WB и защищает сценарий от невалидного тела ответа."""
        try:
            payload = await response.json(content_type=None)
        except aiohttp.ContentTypeError as error:
            response_text = await response.text()
            raise RuntimeError(
                "WB вернул не JSON при загрузке ежедневной воронки: "
                f"status={response.status} body_preview={response_text[:200]}"
            ) from error

        if not isinstance(payload, dict):
            raise RuntimeError("WB вернул неожиданный тип JSON при загрузке ежедневной воронки.")
        return payload

    async def _sleep_for_retry(
        self,
        account: str,
        report_date: date,
        attempt: int,
        status: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """Выдерживает паузу между повторами, чтобы снизить риск лимитов и флапающих ошибок."""
        sleep_seconds = self._calculate_retry_sleep_seconds(attempt=attempt, status=status)
        logger.warning(
            "Повторяем запрос ежедневной воронки WB | account=%s | report_date=%s | attempt=%s/%s | status=%s | error_type=%s | sleep_seconds=%s",
            account,
            report_date.isoformat(),
            attempt,
            self.max_retries,
            status,
            type(error).__name__ if error else None,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)

    def _calculate_retry_sleep_seconds(self, attempt: int, status: int | None) -> int:
        """Рассчитывает паузу retry для защиты ежедневной выгрузки от 429 и временных сбоев."""
        backoff_steps = (10, 20, 40, 60, 120)
        index = min(max(attempt - 1, 0), len(backoff_steps) - 1)
        sleep_seconds = min(backoff_steps[index], self.retry_max_sleep_seconds)
        if status == 429:
            sleep_seconds = max(sleep_seconds, 60)
        return max(sleep_seconds, self.retry_base_sleep_seconds)
