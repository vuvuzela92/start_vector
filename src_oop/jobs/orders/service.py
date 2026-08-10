from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import aiohttp
import pandas as pd

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.orders.client import OrdersFetchResult, WBOrdersClient
from src_oop.jobs.orders.config import DEFAULT_DAYS_BACK, MAX_CONCURRENT_ACCOUNTS
from src_oop.jobs.orders.normalizer import OrdersNormalizer
from src_oop.jobs.orders.repository import OrdersRepository, OrdersSaveResult

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class OrdersRunSummary:
    """Сводка выполнения загрузки заказов WB."""

    accounts_total: int = 0
    requests_total: int = 0
    requests_succeeded: int = 0
    requests_failed: int = 0
    raw_rows: int = 0
    normalized_rows: int = 0
    written_rows: int = 0
    dropped_missing_key_rows: int = 0
    collapsed_duplicate_rows: int = 0
    total_retry_count: int = 0
    failed_requests: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None


class OrdersService:
    """Оркестрирует получение, нормализацию и запись заказов WB."""

    def __init__(
        self,
        client: WBOrdersClient | None = None,
        normalizer: OrdersNormalizer | None = None,
        repository: OrdersRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
    ) -> None:
        """Собирает зависимости сценария загрузки заказов без глобального состояния."""
        self.client = client or WBOrdersClient()
        self.normalizer = normalizer or OrdersNormalizer()
        self.repository = repository or OrdersRepository()
        self.tokens_loader = tokens_loader or load_api_tokens

    async def run(
        self,
        date_from: date | None = None,
        date_to: date | None = None,
        account: str | None = None,
    ) -> OrdersRunSummary:
        """Запускает полный бизнес-сценарий обновления таблицы orders.

        Если период не передан, повторяется legacy-логика: загружаются четыре
        прошедших дня, от вчерашнего до четвертого дня назад включительно.
        """
        resolved_date_from, resolved_date_to = self._resolve_period(date_from, date_to)
        tokens_by_account = self._resolve_tokens(account=account)
        dates = self._build_dates_range(resolved_date_from, resolved_date_to)

        summary = OrdersRunSummary(
            accounts_total=len(tokens_by_account),
            requests_total=len(tokens_by_account) * len(dates),
        )
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)

        logger.info(
            "Старт загрузки заказов WB | date_from=%s | date_to=%s | accounts_total=%s | requests_total=%s | account_filter=%s",
            resolved_date_from.isoformat(),
            resolved_date_to.isoformat(),
            summary.accounts_total,
            summary.requests_total,
            account,
        )

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_orders_for_account_date(
                    semaphore=semaphore,
                    session=session,
                    account=account_name,
                    token=token,
                    date_from=current_date,
                )
                for current_date in dates
                for account_name, token in tokens_by_account.items()
            ]
            fetch_results = await asyncio.gather(*tasks, return_exceptions=True)

        raw_rows: list[dict] = []
        for result in fetch_results:
            if isinstance(result, Exception):
                summary.requests_failed += 1
                summary.failed_requests.append(str(result))
                logger.error(
                    "Загрузка заказов WB по одному запросу завершилась ошибкой, сценарий продолжает обработку остальных данных | error_type=%s | error=%s",
                    type(result).__name__,
                    result,
                )
                continue

            summary.requests_succeeded += 1
            summary.total_retry_count += result.retries_used
            raw_rows.extend(result.payload)

        raw_dataframe = pd.DataFrame(raw_rows)
        summary.raw_rows = len(raw_dataframe.index)

        normalized_dataframe = self.normalizer.normalize(raw_dataframe)
        summary.normalized_rows = len(normalized_dataframe.index)

        save_result = self.repository.save(normalized_dataframe)
        self._apply_save_result(summary=summary, save_result=save_result)

        summary.finished_at = datetime.now()
        logger.info(
            "Загрузка заказов WB завершена | requests_succeeded=%s | requests_failed=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | retries=%s",
            summary.requests_succeeded,
            summary.requests_failed,
            summary.raw_rows,
            summary.normalized_rows,
            summary.written_rows,
            summary.total_retry_count,
        )
        return summary

    async def _fetch_orders_for_account_date(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        date_from: date,
    ) -> OrdersFetchResult:
        """Ограничивает параллельность запросов WB, чтобы загрузка не упиралась в лимиты API."""
        async with semaphore:
            return await self.client.fetch_orders(
                session=session,
                account=account,
                token=token,
                date_from=date_from,
            )

    def _resolve_period(
        self,
        date_from: date | None,
        date_to: date | None,
    ) -> tuple[date, date]:
        """Определяет период загрузки заказов с сохранением legacy-дефолта за четыре дня."""
        if date_from is None and date_to is None:
            yesterday = date.today() - timedelta(days=1)
            return yesterday - timedelta(days=DEFAULT_DAYS_BACK - 1), yesterday

        if date_from is None:
            date_from = date_to
        if date_to is None:
            date_to = date_from

        if date_from is None or date_to is None:
            raise ValueError("Период загрузки заказов WB не определен.")
        if date_from > date_to:
            raise ValueError("date_from не может быть позже date_to.")
        return date_from, date_to

    def _build_dates_range(self, date_from: date, date_to: date) -> list[date]:
        """Строит список дат для подневных запросов WB Statistics API."""
        return [
            date_from + timedelta(days=day_offset)
            for day_offset in range((date_to - date_from).days + 1)
        ]

    def _resolve_tokens(self, account: str | None) -> dict[str, str]:
        """Загружает WB-токены и при необходимости оставляет один выбранный кабинет."""
        loaded_tokens = self.tokens_loader()
        if not isinstance(loaded_tokens, Mapping):
            raise TypeError("load_api_tokens() должен возвращать Mapping account -> token.")

        tokens_by_account = {
            account_name.strip(): token.strip()
            for account_name, token in loaded_tokens.items()
            if isinstance(account_name, str)
            and account_name.strip()
            and isinstance(token, str)
            and token.strip()
        }

        if account is None:
            return tokens_by_account

        normalized_account = account.strip()
        if normalized_account in tokens_by_account:
            return {normalized_account: tokens_by_account[normalized_account]}

        raise ValueError(f"Аккаунт '{account}' не найден в токенах WB.")

    def _apply_save_result(
        self,
        summary: OrdersRunSummary,
        save_result: OrdersSaveResult,
    ) -> None:
        """Переносит показатели записи в итоговую сводку бизнес-сценария."""
        summary.written_rows = save_result.written_rows
        summary.dropped_missing_key_rows = save_result.dropped_missing_key_rows
        summary.collapsed_duplicate_rows = save_result.collapsed_duplicate_rows
