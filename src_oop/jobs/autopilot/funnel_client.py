from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, timedelta

import aiohttp
import pandas as pd

from src_oop.jobs.autopilot.config import (
    FUNNEL_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WBFunnelClient:
    request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS
    max_retries: int = MAX_RETRIES
    retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS
    retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS

    async def fetch_account_funnel(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        nm_ids: list[int],
        report_date: date,
    ) -> pd.DataFrame:
        """
        Получает воронку WB по одному личному кабинету за дату отчета.

        Бизнес-логика:
        формирует метрики ПУ по заказам, переходам в карточку, корзинам, конверсиям
        и остаткам WB. Если WB не возвращает товар, строка не создается, чтобы в ПУ
        остался пропуск вместо искусственного нуля.
        """
        if not nm_ids:
            return pd.DataFrame()

        payload = self._build_payload(nm_ids=nm_ids, report_date=report_date)
        response_payload = await self._request_json(
            session=session,
            account=account,
            headers={"Authorization": token},
            payload=payload,
        )
        products = response_payload.get("data", {}).get("products", [])
        if not products:
            logger.warning("Воронка WB не вернула товары, кабинет будет пропущен: account=%s", account)
            return pd.DataFrame()

        dataframe = pd.json_normalize(products)
        result_df = pd.DataFrame(
            {
                "article_id": pd.to_numeric(dataframe.get("product.nmId"), errors="coerce"),
                "open_card_count": pd.to_numeric(
                    dataframe.get("statistic.selected.openCount"), errors="coerce"
                ),
                "add_to_cart_count": pd.to_numeric(
                    dataframe.get("statistic.selected.cartCount"), errors="coerce"
                ),
                "orders_count": pd.to_numeric(
                    dataframe.get("statistic.selected.orderCount"), errors="coerce"
                ),
                "orders_sum_rub": pd.to_numeric(
                    dataframe.get("statistic.selected.orderSum"), errors="coerce"
                ),
                "to_cart_convers": pd.to_numeric(
                    dataframe.get("statistic.selected.conversions.addToCartPercent"),
                    errors="coerce",
                )
                / 100,
                "to_orders_convers": pd.to_numeric(
                    dataframe.get("statistic.selected.conversions.cartToOrderPercent"),
                    errors="coerce",
                )
                / 100,
                "total_quantity": pd.to_numeric(
                    dataframe.get("product.stocks.wb"), errors="coerce"
                ),
            }
        )
        result_df = result_df.dropna(subset=["article_id"]).copy()
        result_df["article_id"] = result_df["article_id"].astype(int)
        logger.info("Воронка WB загружена для кабинета: account=%s rows=%s", account, len(result_df.index))
        return result_df

    @staticmethod
    def _build_payload(nm_ids: list[int], report_date: date) -> dict:
        """
        Собирает тело запроса WB sales funnel в формате legacy.

        Бизнес-логика:
        `selectedPeriod` всегда равен одному дню `YYYY-mm-dd`, а `pastPeriod`
        оставлен как в legacy для совместимости с ожиданиями API WB.
        """
        current_day = report_date.strftime("%Y-%m-%d")
        return {
            "selectedPeriod": {"start": current_day, "end": current_day},
            "pastPeriod": {
                "start": (report_date - timedelta(days=7)).strftime("%Y-%m-%d"),
                "end": (report_date - timedelta(days=1)).strftime("%Y-%m-%d"),
            },
            "nmIds": nm_ids,
            "brandNames": [],
            "subjectIds": [],
            "tagIds": [],
            "skipDeletedNm": True,
            "orderBy": {"field": "orderSum", "mode": "asc"},
            "limit": 1000,
            "offset": 0,
        }

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        account: str,
        headers: dict[str, str],
        payload: dict,
    ) -> dict:
        """
        Выполняет запрос воронки WB с retry и возвращает JSON-объект.

        Бизнес-логика:
        не прерывает весь почасовой расчет из-за временного сбоя одного кабинета,
        но пробрасывает финальную ошибку в вызывающий слой, где кабинет будет пропущен.
        """
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.post(
                    FUNNEL_URL,
                    headers=headers,
                    json=payload,
                    timeout=timeout,
                ) as response:
                    response_payload = await response.json(content_type=None)
                    if response.status == 200:
                        return response_payload if isinstance(response_payload, dict) else {}
                    if response.status in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                        await self._sleep_for_retry(account=account, attempt=attempt, status=response.status)
                        continue
                    response.raise_for_status()
                    return response_payload if isinstance(response_payload, dict) else {}
            except (
                asyncio.TimeoutError,
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
            ) as error:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"Запрос Воронки WB завершился ошибкой после всех повторов: "
                        f"account={account} error={error}"
                    ) from error
                await self._sleep_for_retry(account=account, attempt=attempt, error=error)
        raise RuntimeError(f"Запрос Воронки WB исчерпал все попытки повтора: account={account}")

    async def _sleep_for_retry(
        self,
        account: str,
        attempt: int,
        status: int | None = None,
        error: Exception | None = None,
    ) -> None:
        """
        Делает паузу перед повтором запроса воронки WB.

        Бизнес-логика:
        ограничивает давление на WB API при лимитах и временных ошибках, чтобы сохранить
        шанс получить данные без ручного перезапуска hourly job.
        """
        sleep_seconds = min(
            self.retry_base_sleep_seconds * attempt,
            self.retry_max_sleep_seconds,
        )
        logger.warning(
            "Повторяем запрос Воронки WB после временной ошибки: account=%s attempt=%s/%s "
            "status=%s error=%s sleep_seconds=%s",
            account,
            attempt,
            self.max_retries,
            status,
            repr(error) if error else None,
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)
