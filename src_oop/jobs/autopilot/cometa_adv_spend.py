from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass

import aiohttp

from src_oop.jobs.autopilot.config import (
    COMETA_AUTOPILOTS_URL,
    COMETA_SPEND_MULTIPLIER,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CometaAdvSpendClient:
    url: str = COMETA_AUTOPILOTS_URL
    request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS
    max_retries: int = MAX_RETRIES
    retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS
    retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS
    spend_multiplier: float = COMETA_SPEND_MULTIPLIER

    async def fetch_today_spend(self, articles: list[int] | None = None) -> dict[int, float]:
        """
        Загружает сегодняшние рекламные расходы из Cometa и агрегирует их по артикулу.

        Бизнес-логика:
        это источник метрики `adv_spend` для ПУ. Расход за сегодня учитывается даже по
        остановленным автопилотам, потому что деньги могли быть потрачены до остановки.
        Отсутствующие в Cometa артикулы пропускаются, а не заполняются нулем.
        Коэффициент `spend_multiplier` сохраняет legacy-правило умножения расхода на 1.1.
        """
        api_key = os.getenv("COMETA_API_KEY")
        if not api_key:
            raise RuntimeError("COMETA_API_KEY не задан в окружении.")

        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        headers = {"Authorization": api_key}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            payload = await self._request_json(session=session, headers=headers)

        if not isinstance(payload, list):
            logger.error("Cometa вернула неожиданный тип ответа: payload_type=%s", type(payload).__name__)
            return {}

        article_filter = set(articles) if articles is not None else None
        spend_by_article: dict[int, float] = {}
        for row in payload:
            if not isinstance(row, dict):
                continue
            try:
                article_id = int(row["product_id"])
                spent_today = float(row["budget_spent_today"])
            except (KeyError, TypeError, ValueError):
                continue
            if spent_today == 0:
                continue
            if article_filter is not None and article_id not in article_filter:
                continue
            spend_by_article[article_id] = spend_by_article.get(article_id, 0.0) + spent_today

        result = {
            article_id: round(spend * self.spend_multiplier, 4)
            for article_id, spend in spend_by_article.items()
        }
        logger.info("Рекламные расходы Cometa загружены для ПУ: rows=%s", len(result))
        return result

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
    ) -> object:
        """
        Выполняет HTTP-запрос к Cometa с retry для временных ошибок.

        Бизнес-логика:
        защищает почасовое обновление ПУ от кратковременных сбоев Cometa и лимитов,
        но не подменяет ошибочный ответ нулями, чтобы не исказить рекламные расходы.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.get(self.url, headers=headers) as response:
                    payload = await response.json(content_type=None)
                    if response.status == 200:
                        return payload
                    if response.status in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                        await self._sleep_for_retry(attempt=attempt, status=response.status)
                        continue
                    response.raise_for_status()
                    return payload
            except (
                asyncio.TimeoutError,
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
            ) as error:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"Запрос Cometa завершился ошибкой после всех повторов: error={error}"
                    ) from error
                await self._sleep_for_retry(attempt=attempt, error=error)
        raise RuntimeError("Запрос Cometa исчерпал все попытки повтора.")

    async def _sleep_for_retry(
        self,
        attempt: int,
        status: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """
        Выдерживает паузу между повторами запроса к Cometa.

        Бизнес-логика:
        соблюдает бережный режим обращения к внешнему сервису, чтобы не усиливать 429/5xx
        и не ронять весь hourly job из-за одного временного сбоя.
        """
        sleep_seconds = min(
            self.retry_base_sleep_seconds * attempt,
            self.retry_max_sleep_seconds,
        )
        logger.warning(
            "Повторяем запрос Cometa после временной ошибки: attempt=%s/%s status=%s "
            "error=%s sleep_seconds=%s",
            attempt,
            self.max_retries,
            status,
            repr(error) if error else None,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)
