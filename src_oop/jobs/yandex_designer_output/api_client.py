"""Асинхронный клиент Yandex Disk API для job дизайнеров."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from src_oop.jobs.yandex_designer_output.config import YandexDesignerOutputConfig

logger = logging.getLogger(__name__)


class YandexDiskClient:
    """Читает и создает папки в Yandex Disk с ограниченным retry."""

    def __init__(self, config: YandexDesignerOutputConfig) -> None:
        """Создает клиент с настройками API и ограничением параллельности."""

        if not config.api_token:
            raise ValueError("Не задан YANDEX_DISK_API_TOKEN для доступа к Yandex Disk")
        self.config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "YandexDiskClient":
        """Открывает общую HTTP-сессию на время выполнения job."""

        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_seconds)
        self._session = aiohttp.ClientSession(
            headers={"Accept": "application/json", "Authorization": self.config.api_token},
            timeout=timeout,
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Закрывает HTTP-сессию после завершения всех запросов."""

        if self._session is not None:
            await self._session.close()

    async def get_resource(self, path: str) -> dict[str, Any]:
        """Возвращает метаданные ресурса по пути на Yandex Disk."""

        return await self._request("GET", path)

    async def create_folder(self, path: str) -> bool:
        """Создает папку и возвращает False, если она уже существовала.

        Бизнес-правило: повторный запуск не считается ошибкой, если папка на
        указанную дату уже была создана ранее.
        """

        try:
            await self._request("PUT", path)
            return True
        except FileExistsError:
            return False

    async def _request(self, method: str, path: str) -> dict[str, Any]:
        """Выполняет запрос к ресурсу с обработкой временных ошибок API."""

        if self._session is None:
            raise RuntimeError("Клиент Yandex Disk должен использоваться внутри async with")
        url = "https://cloud-api.yandex.net/v1/disk/resources"
        retry_statuses = {429, 500, 502, 503, 504}

        for attempt in range(1, self.config.max_retries + 1):
            async with self._semaphore:
                try:
                    async with self._session.request(
                        method, url, params={"path": path}
                    ) as response:
                        if response.status in {200, 201}:
                            return {} if method == "PUT" else await response.json()
                        if response.status == 409 and method == "PUT":
                            raise FileExistsError(path)
                        if response.status not in retry_statuses:
                            logger.error(
                                "Yandex Disk вернул ошибку ресурса | method=%s | path=%s | status=%s",
                                method,
                                path,
                                response.status,
                            )
                            raise RuntimeError(f"Ошибка Yandex Disk: HTTP {response.status}")
                except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                    if attempt == self.config.max_retries:
                        logger.error(
                            "Yandex Disk недоступен после повторов | method=%s | path=%s | error_type=%s",
                            method,
                            path,
                            type(error).__name__,
                        )
                        raise
            await asyncio.sleep(min(2**attempt, 30))

        raise RuntimeError("Не удалось выполнить запрос к Yandex Disk")
