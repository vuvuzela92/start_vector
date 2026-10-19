"""Бизнес-сервис дозагрузки товаров поставок WB."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

import aiohttp

from src_oop.jobs.wb_missing_supplies_goods.client import WBSuppliesClient
from src_oop.jobs.wb_missing_supplies_goods.config import (
    LOOKBACK_PERIOD,
    MAX_CONCURRENT_ACCOUNTS,
    REQUEST_DELAY_SECONDS,
)
from src_oop.jobs.wb_missing_supplies_goods.models import AccountResult
from src_oop.jobs.wb_missing_supplies_goods.repository import WBSuppliesRepository

logger = logging.getLogger(__name__)


class WBSuppliesGoodsService:
    """Оркестрирует поиск и сохранение отсутствующих товаров поставок."""

    def __init__(
        self,
        client: WBSuppliesClient | None = None,
        repository: WBSuppliesRepository | None = None,
    ) -> None:
        """Собирает внешний клиент и репозиторий, допускающие подмену в тестах."""
        self.client = client or WBSuppliesClient()
        self.repository = repository or WBSuppliesRepository()

    async def run(self, tokens: dict[str, str]) -> list[AccountResult]:
        """Обрабатывает свежие поставки всех ЛК и добавляет товары в PostgreSQL.

        Бизнес-правило: новая поставка также должна загружаться в
        ``wb_supplies_goods`` целиком, поэтому список поставок ограничивается
        только окном актуальности, а не наличием ID в локальной таблице.
        """
        existing_goods = self.repository.fetch_existing_goods()
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                self._run_account(session, account, token, existing_goods, semaphore)
                for account, token in tokens.items()
            ]
            return await asyncio.gather(*tasks)

    async def _run_account(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        existing_goods: dict[int, set[str]],
        semaphore: asyncio.Semaphore,
    ) -> AccountResult:
        """Обрабатывает один ЛК; ошибка одного ЛК не прерывает остальные."""
        async with semaphore:
            try:
                supplies = await self.client.fetch_recent_supplies(session, token)
                threshold = datetime.now() - LOOKBACK_PERIOD
                supply_ids = {
                    int(row["supplyID"])
                    for row in supplies
                    if self._is_recent_supply(row, threshold)
                }
                recent_supply_ids = [
                    int(row["supplyID"])
                    for row in supplies
                    if self._is_recent_supply(row, threshold)
                ]
                supply_details = []
                for supply_id in recent_supply_ids:
                    details = await self.client.fetch_supply_details(
                        session,
                        token,
                        supply_id,
                    )
                    if details is not None:
                        supply_details.append(details)
                    else:
                        logger.error(
                            "Полные данные поставки не получены; поставка пропущена | account=%s | supply_id=%s",
                            account,
                            supply_id,
                        )
                supplies_saved = self.repository.insert_supplies(supply_details)
                inserted = 0
                for index, supply_id in enumerate(supply_ids):
                    goods = await self.client.fetch_supply_goods(session, token, supply_id)
                    # Для новой поставки множество пустое, поэтому загрузятся
                    # все товары; для известной — только отсутствующие vendorCode.
                    known = existing_goods.get(supply_id, set())
                    missing = [
                        row for row in goods if str(row.get("vendorCode")) not in known
                    ]
                    inserted += self.repository.insert_missing_goods(missing)
                    if index + 1 < len(supply_ids):
                        await asyncio.sleep(REQUEST_DELAY_SECONDS)
                logger.info(
                    "Обработка товаров поставок завершена | account=%s | supplies=%s | supplies_saved=%s | goods_inserted=%s",
                    account,
                    len(supply_ids),
                    supplies_saved,
                    inserted,
                )
                return AccountResult(account, len(supply_ids), supplies_saved, inserted)
            except Exception as error:
                logger.error(
                    "Обработка товаров поставок прервана для ЛК; другие ЛК продолжат работу | account=%s | error_type=%s",
                    account,
                    type(error).__name__,
                )
                return AccountResult(account, 0, 0, 0)

    @staticmethod
    def _is_recent_supply(row: dict[str, Any], threshold: datetime) -> bool:
        """Проверяет наличие ID и дату обновления поставки в окне загрузки."""
        supply_id = row.get("supplyID")
        updated_date = row.get("updatedDate")
        if not supply_id or not updated_date:
            return False
        try:
            parsed = datetime.fromisoformat(str(updated_date).replace("Z", "+00:00"))
            return parsed.replace(tzinfo=None) >= threshold
        except ValueError:
            return False
