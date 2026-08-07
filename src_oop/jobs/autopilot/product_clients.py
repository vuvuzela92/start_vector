from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import aiohttp

from src_oop.jobs.autopilot.config import (
    MAX_CONCURRENT_PUBLIC_CARD_REQUESTS,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
    WB_CARD_DETAIL_URL,
    WB_PRICES_URL,
    WB_PRICE_PAGE_LIMIT,
)
from src_oop.jobs.autopilot.models import WBCardSnapshot

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WBPriceClient:
    request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS
    page_limit: int = WB_PRICE_PAGE_LIMIT

    async def fetch_full_prices_by_account(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        articles: set[int],
    ) -> dict[int, float]:
        """
        Получает полные цены WB по одному личному кабинету.

        Бизнес-логика:
        эти цены используются для метрики `full_price` и расчета процента СПП.
        Возвращаются только артикула из ПУ; отсутствующие в ответе WB товары пропускаются.
        """
        if not articles:
            return {}

        prices: dict[int, float] = {}
        offset = 0
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        headers = {"Authorization": token}

        while True:
            params = {"limit": self.page_limit, "offset": offset}
            async with session.get(
                WB_PRICES_URL,
                headers=headers,
                params=params,
                timeout=timeout,
            ) as response:
                payload = await response.json(content_type=None)
                response.raise_for_status()

            goods = payload.get("data", {}).get("listGoods", []) if isinstance(payload, dict) else []
            if not goods:
                break

            for item in goods:
                if not isinstance(item, dict):
                    continue
                try:
                    article_id = int(item["nmID"])
                except (KeyError, TypeError, ValueError):
                    continue
                if article_id not in articles:
                    continue

                sizes = item.get("sizes") or []
                if not sizes or not isinstance(sizes[0], dict):
                    continue
                price = sizes[0].get("discountedPrice")
                if price is not None:
                    prices[article_id] = float(price)

            if len(goods) < self.page_limit:
                break
            offset += self.page_limit

        logger.info("Полные цены WB загружены для кабинета: account=%s rows=%s", account, len(prices))
        return prices


@dataclass(slots=True)
class WBPublicCardClient:
    request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS
    max_retries: int = MAX_RETRIES
    retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS
    retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS
    max_concurrent_requests: int = MAX_CONCURRENT_PUBLIC_CARD_REQUESTS

    async def fetch_cards(
        self,
        articles: list[int],
        full_prices: dict[int, float],
    ) -> dict[int, WBCardSnapshot]:
        """
        Получает публичные карточки WB и собирает снимки цены, СПП, рейтинга и промо.

        Бизнес-логика:
        объединяет полную цену из seller API с ценой карточки WB, чтобы посчитать СПП,
        а также подготовить данные для записи в ПУ и `spp_history`.
        """
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                self._fetch_card(
                    session=session,
                    semaphore=semaphore,
                    article_id=article_id,
                    full_price=full_prices.get(article_id),
                )
                for article_id in articles
                if article_id in full_prices
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        snapshots: dict[int, WBCardSnapshot] = {}
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Публичная карточка WB пропущена после ошибки, сценарий продолжается: %s", result)
                continue
            if result is not None:
                snapshots[result.article_id] = result
        logger.info("Публичные карточки WB загружены для ПУ: rows=%s", len(snapshots))
        return snapshots

    async def _fetch_card(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        article_id: int,
        full_price: float | None,
    ) -> WBCardSnapshot | None:
        """
        Загружает и нормализует публичную карточку одного артикула WB.

        Бизнес-логика:
        определяет наличие акции, рейтинг, цену с СПП и процент СПП для конкретной строки ПУ.
        Если карточка недоступна или неполная, артикул пропускается.
        """
        async with semaphore:
            payload = await self._request_card_json(session=session, article_id=article_id)

        products = payload.get("products", []) if isinstance(payload, dict) else []
        if not products or not isinstance(products[0], dict):
            return None

        product = products[0]
        discounted_price = self._extract_discounted_price(product)
        spp = None
        if full_price and discounted_price:
            spp = round((full_price - discounted_price) / full_price * 100, 1)

        rating = product.get("reviewRating")
        return WBCardSnapshot(
            article_id=article_id,
            promo_status=1 if product.get("promoTextCard") is not None else 0,
            rating=float(rating) if rating is not None else None,
            full_price=full_price,
            spp=spp,
            discounted_price=discounted_price,
        )

    async def _request_card_json(
        self,
        session: aiohttp.ClientSession,
        article_id: int,
    ) -> dict:
        """
        Выполняет запрос публичной карточки WB с retry.

        Бизнес-логика:
        публичная карточка нужна для СПП и рейтинга; временный сбой одного артикула
        не должен останавливать обновление остальных артикулов.
        """
        params = {
            "appType": 1,
            "curr": "rub",
            "dest": -1255987,
            "spp": 30,
            "hide_vflags": 4294967296,
            "hide_dtype": "9;11",
            "ab_testing": "false",
            "nm": article_id,
        }
        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.get(WB_CARD_DETAIL_URL, params=params) as response:
                    payload = await response.json(content_type=None)
                    if response.status == 200:
                        return payload if isinstance(payload, dict) else {}
                    if response.status in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                        await self._sleep_for_retry(article_id=article_id, attempt=attempt, status=response.status)
                        continue
                    response.raise_for_status()
                    return payload if isinstance(payload, dict) else {}
            except (
                asyncio.TimeoutError,
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
            ) as error:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"Запрос публичной карточки WB завершился ошибкой после всех повторов: "
                        f"article_id={article_id} error={error}"
                    ) from error
                await self._sleep_for_retry(article_id=article_id, attempt=attempt, error=error)
        return {}

    @staticmethod
    def _extract_discounted_price(product: dict) -> float | None:
        """
        Достает цену товара с учетом СПП из структуры публичной карточки WB.

        Бизнес-логика:
        WB отдает цену в копейках внутри вложенного `sizes[0].price.product`,
        а ПУ и `spp_history` ожидают значение в рублях.
        """
        try:
            return float(product["sizes"][0]["price"]["product"]) / 100
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    async def _sleep_for_retry(
        self,
        article_id: int,
        attempt: int,
        status: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """
        Делает паузу перед повтором запроса публичной карточки WB.

        Бизнес-логика:
        снижает риск блокировки/лимитов при массовом обходе карточек из ПУ.
        """
        sleep_seconds = min(
            self.retry_base_sleep_seconds * attempt,
            self.retry_max_sleep_seconds,
        )
        logger.warning(
            "Повторяем запрос публичной карточки WB после временной ошибки: article_id=%s "
            "attempt=%s/%s status=%s error=%s sleep_seconds=%s",
            article_id,
            attempt,
            self.max_retries,
            status,
            repr(error) if error else None,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)
