from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import aiohttp

from src_oop.jobs.fbs_warehouses.config import (
    MAX_RETRIES,
    OFFICES_URL,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
    WAREHOUSES_URL,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WBRequestResult:
    """Результат одного запроса WB по управлению FBS-складами продавца."""

    payload: dict | list | None
    retries_used: int


class WBFBSWarehousesClient:
    """Клиент Marketplace API для выбора офисов WB и управления FBS-складами продавца."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
    ) -> None:
        """Настраивает HTTP-клиент для ручного сценария управления FBS-складами WB."""
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds

    async def fetch_offices(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
    ) -> WBRequestResult:
        """Получает список офисов WB, из которых бизнес выбирает `officeId` для создания склада продавца."""
        result = await self._request_json(
            session=session,
            method="GET",
            url=OFFICES_URL,
            account=account,
            token=token,
            request_name="получение списка офисов WB",
        )
        if not isinstance(result.payload, list):
            raise RuntimeError(
                f"WB вернул неожиданный формат списка офисов: account={account}"
            )
        logger.info(
            "Получен список офисов WB для выбора склада продавца | account=%s | offices=%s | retries_used=%s",
            account,
            len(result.payload),
            result.retries_used,
        )
        return result

    async def fetch_warehouses(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
    ) -> WBRequestResult:
        """Получает текущие FBS-склады продавца, чтобы сверить созданные склады и их `warehouseId`."""
        result = await self._request_json(
            session=session,
            method="GET",
            url=WAREHOUSES_URL,
            account=account,
            token=token,
            request_name="получение списка FBS-складов продавца",
        )
        if not isinstance(result.payload, list):
            raise RuntimeError(
                f"WB вернул неожиданный формат списка FBS-складов: account={account}"
            )
        logger.info(
            "Получен список FBS-складов продавца | account=%s | warehouses=%s | retries_used=%s",
            account,
            len(result.payload),
            result.retries_used,
        )
        return result

    async def create_warehouse(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        office_id: int,
        name: str,
    ) -> WBRequestResult:
        """Создает FBS-склад продавца WB по выбранному `officeId` и бизнес-названию склада."""
        payload = {"officeId": office_id, "name": name}
        result = await self._request_json(
            session=session,
            method="POST",
            url=WAREHOUSES_URL,
            account=account,
            token=token,
            request_name="создание FBS-склада продавца",
            json_payload=payload,
        )
        logger.info(
            "Создан FBS-склад продавца WB | account=%s | office_id=%s | name=%s | retries_used=%s | response=%s",
            account,
            office_id,
            name,
            result.retries_used,
            result.payload,
        )
        return result

    async def delete_warehouse(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        warehouse_id: int,
    ) -> WBRequestResult:
        """Удаляет FBS-склад продавца WB по `warehouseId`, когда склад больше не участвует в управлении остатками."""
        result = await self._request_json(
            session=session,
            method="DELETE",
            url=f"{WAREHOUSES_URL}/{warehouse_id}",
            account=account,
            token=token,
            request_name="удаление FBS-склада продавца",
        )
        logger.info(
            "Удален FBS-склад продавца WB | account=%s | warehouse_id=%s | retries_used=%s",
            account,
            warehouse_id,
            result.retries_used,
        )
        return result

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        account: str,
        token: str,
        request_name: str,
        json_payload: dict[str, object] | None = None,
    ) -> WBRequestResult:
        """Выполняет запрос к WB с retry, чтобы ручная операция не падала от временных 429/5xx-сбоев."""
        headers = {"Authorization": token}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)

        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_payload,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_payload(response)

                    if response.status == 429 or response.status in {500, 502, 503, 504}:
                        await self._sleep_for_retry(
                            account=account,
                            request_name=request_name,
                            attempt=attempt,
                            status=response.status,
                        )
                        continue

                    if response.status == 401:
                        raise PermissionError(
                            f"WB отклонил токен при операции со складами: account={account}"
                        )

                    response.raise_for_status()
                    return WBRequestResult(payload=payload, retries_used=attempt - 1)
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
                        "Запрос WB для управления FBS-складами завершился ошибкой после всех повторов: "
                        f"account={account} request={request_name} error={type(error).__name__}: {error}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    request_name=request_name,
                    attempt=attempt,
                    error=error,
                )

        raise RuntimeError(
            "Запрос WB для управления FBS-складами неожиданно исчерпал все попытки повтора: "
            f"account={account} request={request_name}"
        )

    async def _read_payload(self, response: aiohttp.ClientResponse) -> dict | list | None:
        """Читает ответ WB, сохраняя диагностическое тело для ошибок создания и удаления склада."""
        if response.status == 204:
            return None

        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            return await response.json()

        text_payload = await response.text()
        if not text_payload:
            return None
        return {"text": text_payload}

    async def _sleep_for_retry(
        self,
        account: str,
        request_name: str,
        attempt: int,
        status: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """Выдерживает паузу между повторами, чтобы соблюсти лимиты WB при операциях со складами."""
        sleep_seconds = self._calculate_retry_sleep_seconds(attempt=attempt, status=status)
        logger.warning(
            "Повторяем запрос WB по FBS-складам | account=%s | request=%s | attempt=%s/%s | status=%s | error=%s | sleep_seconds=%s",
            account,
            request_name,
            attempt,
            self.max_retries,
            status,
            repr(error) if error else None,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)

    def _calculate_retry_sleep_seconds(self, attempt: int, status: int | None) -> int:
        """Рассчитывает backoff для защиты сценария управления складами от 429 и временных ошибок WB."""
        backoff_steps = (5, 15, 30, 60, 120)
        index = min(max(attempt - 1, 0), len(backoff_steps) - 1)
        sleep_seconds = min(backoff_steps[index], self.retry_max_sleep_seconds)
        if status == 429:
            sleep_seconds = max(sleep_seconds, 60)
        return max(sleep_seconds, self.retry_base_sleep_seconds)
