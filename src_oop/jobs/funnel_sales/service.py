from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import aiohttp
import pandas as pd

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.funnel_sales.client import FunnelFetchResult, WBFunnelSalesClient
from src_oop.jobs.funnel_sales.config import (
    DEFAULT_DAYS_BACK,
    MAX_CONCURRENT_REQUESTS,
    REQUEST_STAGGER_SECONDS,
)
from src_oop.jobs.funnel_sales.normalizer import FunnelSalesNormalizer
from src_oop.jobs.funnel_sales.repository import FunnelSalesRepository, FunnelSalesSaveResult

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FunnelSalesRunSummary:
    """Сводка выполнения ежедневной выгрузки воронки продаж WB."""

    accounts_total: int = 0
    report_dates_total: int = 0
    requests_total: int = 0
    requests_succeeded: int = 0
    requests_failed: int = 0
    raw_rows: int = 0
    normalized_rows: int = 0
    rows_after_drop_duplicates: int = 0
    rows_after_business_deduplication: int = 0
    rows_after_exclusions: int = 0
    removed_exact_duplicates: int = 0
    collapsed_business_duplicates: int = 0
    excluded_rows: int = 0
    written_rows: int = 0
    total_retry_count: int = 0
    failed_requests: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None


class FunnelSalesService:
    """Оркестрирует получение, нормализацию и запись daily funnel в PostgreSQL."""

    def __init__(
        self,
        client: WBFunnelSalesClient | None = None,
        normalizer: FunnelSalesNormalizer | None = None,
        repository: FunnelSalesRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
    ) -> None:
        """Собирает зависимости сценария выгрузки воронки без изменения архитектуры проекта."""
        self.client = client or WBFunnelSalesClient()
        self.normalizer = normalizer or FunnelSalesNormalizer()
        self.repository = repository or FunnelSalesRepository()
        self.tokens_loader = tokens_loader or load_api_tokens

    async def run(
        self,
        date_from: date | None = None,
        date_to: date | None = None,
        account: str | None = None,
    ) -> FunnelSalesRunSummary:
        """Запускает полный бизнес-сценарий обновления таблицы funnel_daily.

        Если период не передан, сценарий загружает последние 28 дней, включая
        текущую дату. Это поддерживает hourly-обновление текущего дня на cron,
        чтобы витрина дополнялась новыми значениями без ручной передачи дат.
        """
        resolved_date_from, resolved_date_to = self._resolve_period(date_from, date_to)
        tokens_by_account = self._resolve_tokens(account=account)
        report_dates = self._build_dates_range(resolved_date_from, resolved_date_to)
        request_plan = self._build_request_plan(
            report_dates=report_dates,
            tokens_by_account=tokens_by_account,
        )

        summary = FunnelSalesRunSummary(
            accounts_total=len(tokens_by_account),
            report_dates_total=len(report_dates),
            requests_total=len(request_plan),
        )
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        fetch_started_at = datetime.now()

        logger.info(
            "Старт выгрузки ежедневной воронки продаж WB | date_from=%s | date_to=%s | accounts_total=%s | report_dates_total=%s | requests_total=%s | account_filter=%s",
            resolved_date_from.isoformat(),
            resolved_date_to.isoformat(),
            summary.accounts_total,
            summary.report_dates_total,
            summary.requests_total,
            account,
        )

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_funnel_for_account_date(
                    semaphore=semaphore,
                    session=session,
                    account=request_item.account,
                    token=request_item.token,
                    report_date=request_item.report_date,
                    stagger_seconds=request_item.stagger_seconds,
                )
                for request_item in request_plan
            ]
            fetch_results = await asyncio.gather(*tasks, return_exceptions=True)

        raw_rows: list[dict] = []
        for result in fetch_results:
            if isinstance(result, Exception):
                summary.requests_failed += 1
                summary.failed_requests.append(str(result))
                logger.error(
                    "Один запрос ежедневной воронки WB завершился ошибкой, сценарий продолжает остальные даты и кабинеты | error_type=%s | error=%s",
                    type(result).__name__,
                    result,
                )
                continue

            summary.requests_succeeded += 1
            summary.total_retry_count += result.retries_used
            raw_rows.extend(result.payload)

        raw_dataframe = pd.DataFrame(raw_rows)
        summary.raw_rows = len(raw_dataframe.index)
        fetch_finished_at = datetime.now()
        logger.info(
            "Этап получения daily funnel завершён | requests_succeeded=%s | requests_failed=%s | raw_rows=%s | retries=%s | duration_seconds=%.2f",
            summary.requests_succeeded,
            summary.requests_failed,
            summary.raw_rows,
            summary.total_retry_count,
            (fetch_finished_at - fetch_started_at).total_seconds(),
        )

        normalize_started_at = datetime.now()
        normalized_dataframe = self.normalizer.normalize(raw_dataframe)
        summary.normalized_rows = len(normalized_dataframe.index)
        normalize_finished_at = datetime.now()
        logger.info(
            "Этап нормализации daily funnel завершён | normalized_rows=%s | duration_seconds=%.2f",
            summary.normalized_rows,
            (normalize_finished_at - normalize_started_at).total_seconds(),
        )

        save_started_at = datetime.now()
        save_result = self.repository.save(normalized_dataframe)
        self._apply_save_result(summary=summary, save_result=save_result)
        save_finished_at = datetime.now()
        logger.info(
            "Этап записи daily funnel завершён | written_rows=%s | duration_seconds=%.2f",
            summary.written_rows,
            (save_finished_at - save_started_at).total_seconds(),
        )

        summary.finished_at = datetime.now()
        logger.info(
            "Выгрузка ежедневной воронки продаж WB завершена | requests_succeeded=%s | requests_failed=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | retries=%s",
            summary.requests_succeeded,
            summary.requests_failed,
            summary.raw_rows,
            summary.normalized_rows,
            summary.written_rows,
            summary.total_retry_count,
        )
        return summary

    async def _fetch_funnel_for_account_date(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        report_date: date,
        stagger_seconds: float,
    ) -> FunnelFetchResult:
        """Ограничивает параллельность и слегка разносит старт запросов к WB.

        Бизнес-логика: daily funnel должен стабильно доезжать по всем кабинетам.
        Небольшой stagger снижает риск burst-нагрузки, когда несколько тяжёлых
        кабинетов одновременно попадают под лимит и получают HTTP 429.
        """
        async with semaphore:
            if stagger_seconds > 0:
                await asyncio.sleep(stagger_seconds)
            return await self.client.fetch_daily_funnel(
                session=session,
                account=account,
                token=token,
                report_date=report_date,
            )

    def _resolve_period(
        self,
        date_from: date | None,
        date_to: date | None,
    ) -> tuple[date, date]:
        """Определяет период выгрузки daily funnel по умолчанию на последние 28 дней.

        Бизнес-логика: если явный период не передан, ежедневная воронка должна
        включать текущий день, потому что hourly-cron обновляет витрину внутри
        дня и не должен ждать закрытия суток для появления новых данных в БД.
        """
        if date_from is None and date_to is None:
            today = date.today()
            return today - timedelta(days=DEFAULT_DAYS_BACK - 1), today

        if date_from is None:
            date_from = date_to
        if date_to is None:
            date_to = date_from

        if date_from is None or date_to is None:
            raise ValueError("Период выгрузки ежедневной воронки продаж WB не определён.")
        if date_from > date_to:
            raise ValueError("date_from не может быть позже date_to.")
        return date_from, date_to

    def _build_dates_range(self, date_from: date, date_to: date) -> list[date]:
        """Строит список дат для подневных запросов daily funnel."""
        return [
            date_from + timedelta(days=day_offset)
            for day_offset in range((date_to - date_from).days + 1)
        ]

    def _build_request_plan(
        self,
        report_dates: list[date],
        tokens_by_account: dict[str, str],
    ) -> list["FunnelRequestItem"]:
        """Строит план запросов с ротацией кабинетов и небольшим stagger.

        Бизнес-логика: в legacy-порядке одни и те же первые кабинеты слишком
        часто стартовали одновременно и регулярно ловили HTTP 429. Ротация по
        датам и лёгкий разнос старта уменьшают шанс, что под лимит снова
        попадёт одна и та же группа кабинетов.
        """
        account_items = list(tokens_by_account.items())
        if not account_items:
            return []

        request_plan: list[FunnelRequestItem] = []
        stagger_cycle = max(MAX_CONCURRENT_REQUESTS, 1)
        for day_index, report_date in enumerate(report_dates):
            rotation = day_index % len(account_items)
            rotated_accounts = account_items[rotation:] + account_items[:rotation]
            for account_index, (account_name, token) in enumerate(rotated_accounts):
                request_plan.append(
                    FunnelRequestItem(
                        report_date=report_date,
                        account=account_name,
                        token=token,
                        stagger_seconds=(account_index % stagger_cycle) * REQUEST_STAGGER_SECONDS,
                    )
                )
        return request_plan

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
        summary: FunnelSalesRunSummary,
        save_result: FunnelSalesSaveResult,
    ) -> None:
        """Переносит показатели записи daily funnel в итоговую сводку бизнес-сценария."""
        summary.rows_after_drop_duplicates = save_result.rows_after_drop_duplicates
        summary.rows_after_business_deduplication = save_result.rows_after_business_deduplication
        summary.rows_after_exclusions = save_result.rows_after_exclusions
        summary.removed_exact_duplicates = save_result.removed_exact_duplicates
        summary.collapsed_business_duplicates = save_result.collapsed_business_duplicates
        summary.excluded_rows = save_result.excluded_rows
        summary.written_rows = save_result.written_rows


@dataclass(slots=True)
class FunnelRequestItem:
    """Описывает один запрос daily funnel к кабинету WB за конкретную дату."""

    report_date: date
    account: str
    token: str
    stagger_seconds: float
