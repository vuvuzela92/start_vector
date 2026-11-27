"""Бизнес-сценарии выгрузок старого контура сборочных заданий."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta

import aiohttp
import pandas as pd

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.assembly_market_sheets.client import (
    AssemblyMarketSheetsClient,
    AssemblyOrdersClient,
    normalize_market_statuses,
    normalize_orders_for_sheet,
)
from src_oop.jobs.assembly_market_sheets.config import (
    REQUIRED_ASSEMBLY_COLUMNS,
    AssemblyMarketSheetsSettings,
)
from src_oop.jobs.assembly_market_sheets.repository import AssemblyMarketSheetsRepository

logger = logging.getLogger(__name__)


class AssemblyMarketSheetsService:
    """Оркестрирует сбор заказов, обновление статусов и перенос листа сборки."""

    def __init__(
        self,
        settings: AssemblyMarketSheetsSettings | None = None,
        sheets: AssemblyMarketSheetsClient | None = None,
        orders_client: AssemblyOrdersClient | None = None,
        repository: AssemblyMarketSheetsRepository | None = None,
    ) -> None:
        """Собирает компоненты трех связанных операций старого контура Sheets."""
        self.settings = settings or AssemblyMarketSheetsSettings()
        self.sheets = sheets or AssemblyMarketSheetsClient(self.settings)
        self.orders_client = orders_client or AssemblyOrdersClient(self.settings)
        self.repository = repository or AssemblyMarketSheetsRepository()

    async def update_orders(self) -> None:
        """Загружает заказы всех кабинетов за последние 11 дней и обновляет тестовый лист."""
        started_at = time.perf_counter()
        end_time = datetime.now()
        begin = int((end_time - timedelta(days=self.settings.orders_lookback_days)).timestamp())
        end = int(end_time.timestamp())
        tokens = {str(account): str(token) for account, token in load_api_tokens().items()}
        timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            batches = await asyncio.gather(*(
                self.orders_client.fetch_account_orders(session, account, token, begin, end)
                for account, token in tokens.items()
            ))
        rows = [row for batch in batches for row in batch]
        api_seconds = time.perf_counter() - started_at
        dataframe = normalize_orders_for_sheet(
            rows, self.settings.orders_time_zone_offset_hours
        )
        self.sheets.upload(
            dataframe, self.settings.market_table, self.settings.market_orders_sheet
        )
        logger.info(
            "Выгрузка заказов в тестовый лист завершена | rows=%s | wb_api_seconds=%.1f | total_seconds=%.1f",
            len(dataframe), api_seconds, time.perf_counter() - started_at,
        )

    def update_statuses(self) -> None:
        """Публикует статусы PostgreSQL для заданий, присутствующих в рабочем листе."""
        started_at = time.perf_counter()
        values = self.sheets.read_values(
            self.settings.market_table, self.settings.market_orders_sheet
        )
        statuses = self.repository.fetch_statuses()
        dataframe = normalize_market_statuses(values, statuses)
        self.sheets.update_statuses(dataframe)
        logger.info(
            "Обновление листа статусов сборки завершено | rows=%s | total_seconds=%.1f",
            len(dataframe), time.perf_counter() - started_at,
        )

    def migrate_assembly_data(self) -> None:
        """Переносит выбранные колонки сборочных заданий в таблицу висящих заказов."""
        started_at = time.perf_counter()
        values = self.sheets.read_values(
            self.settings.market_table, self.settings.source_assembly_sheet
        )
        if not values:
            dataframe = pd.DataFrame(columns=REQUIRED_ASSEMBLY_COLUMNS)
        else:
            dataframe = pd.DataFrame(values[1:], columns=values[0])
            missing = set(REQUIRED_ASSEMBLY_COLUMNS).difference(dataframe.columns)
            if missing:
                raise ValueError(
                    "На листе сборочных заданий отсутствуют колонки: "
                    + ", ".join(sorted(missing))
                )
            dataframe = dataframe.loc[:, REQUIRED_ASSEMBLY_COLUMNS]
        self.sheets.upload(
            dataframe, self.settings.hanging_table, self.settings.destination_assembly_sheet
        )
        logger.info(
            "Перенос сборочных заданий завершен | rows=%s | total_seconds=%.1f",
            len(dataframe), time.perf_counter() - started_at,
        )
