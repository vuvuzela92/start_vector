"""Оркестрация выгрузки баланса продавцов WB."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime

import aiohttp
import pandas as pd
from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.seller_balance.client import (
    SellerBalanceFetchResult,
    WBSellerBalanceClient,
)
from src_oop.jobs.seller_balance.config import (
    FINANCIAL_REPORTS_COLUMNS,
    FINANCIAL_REPORTS_START_DATE,
    MAX_CONCURRENT_ACCOUNTS,
)
from src_oop.jobs.seller_balance.queries import FINANCIAL_REPORTS_QUERY
from src_oop.jobs.seller_balance.repository import (
    SellerBalanceRepository,
    SellerBalanceSaveResult,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SellerBalanceRunSummary:
    """Сводка полного запуска выгрузки баланса и начислений продавцов.

    Бизнес-сценарий:
    сводка нужна логам и планировщику, чтобы было видно количество успешно
    обработанных кабинетов, ошибок и опубликованных строк в двух витринах ДДС.
    """

    accounts_total: int = 0
    accounts_processed: int = 0
    written_rows: int = 0
    financial_rows: int = 0
    total_retry_count: int = 0
    succeeded_accounts: list[str] = field(default_factory=list)
    failed_accounts: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None


class SellerBalanceService:
    """Собирает баланс и начисления продавцов и публикует их в Google Sheets.

    Бизнес-сценарий:
    job обновляет общую финансовую витрину ДДС: получает агрегированные
    начисления из PostgreSQL и один актуальный срез баланса по каждому кабинету.
    """

    def __init__(
        self,
        client: WBSellerBalanceClient | None = None,
        repository: SellerBalanceRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
        database_cls: type[Database] = Database,
    ) -> None:
        """Собирает зависимости job для сопровождения и тестирования.

        Бизнес-сценарий:
        кроме клиента WB и Google Sheets job получает общий класс доступа к
        PostgreSQL, чтобы перед публикацией проверить актуальность начислений.
        """
        self.client = client or WBSellerBalanceClient()
        self.repository = repository or SellerBalanceRepository()
        self.tokens_loader = tokens_loader or load_api_tokens
        self.database_cls = database_cls

    async def run(self, account: str | None = None) -> SellerBalanceRunSummary:
        """Запускает полный сценарий обновления баланса и начислений в ДДС.

        Бизнес-сценарий:
        сначала читаются начисления из PostgreSQL, затем баланс по всем или
        выбранному кабинету WB, после чего оба результата передаются в одну
        запись вкладки `Переменные.`. Ошибка PostgreSQL блокирует публикацию.
        """
        financial_dataframe = self._load_financial_reports()
        tokens_by_account = self._resolve_tokens(account=account)
        summary = SellerBalanceRunSummary(accounts_total=len(tokens_by_account))
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)

        logger.info(
            "Старт выгрузки баланса продавцов WB | accounts_total=%s | account_filter=%s",
            summary.accounts_total,
            account,
        )

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_account_balance(
                    semaphore=semaphore,
                    session=session,
                    account_name=account_name,
                    token=token,
                )
                for account_name, token in tokens_by_account.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        rows: list[dict[str, object]] = []
        for account_name, result in zip(tokens_by_account, results, strict=True):
            if isinstance(result, Exception):
                summary.failed_accounts.append(account_name)
                error_type = type(result).__name__
                summary.warnings.append(error_type)
                logger.error(
                    "Не удалось выгрузить баланс продавца по кабинету, но job продолжает обработку остальных | account=%s | error_type=%s",
                    account_name,
                    error_type,
                )
                continue

            summary.accounts_processed += 1
            summary.succeeded_accounts.append(account_name)
            summary.total_retry_count += result.retries_used
            rows.append(self._build_row(result))

        save_result = self.repository.save(
            rows=rows,
            financial_dataframe=financial_dataframe,
        )
        if not isinstance(save_result, SellerBalanceSaveResult):
            raise TypeError("save_result должен быть экземпляром SellerBalanceSaveResult.")

        summary.written_rows = save_result.written_rows
        summary.financial_rows = save_result.financial_rows
        summary.finished_at = datetime.now()
        logger.info(
            "Завершена выгрузка баланса и начислений продавцов WB | accounts_processed=%s | failed_accounts=%s | balance_rows=%s | financial_rows=%s | retries=%s",
            summary.accounts_processed,
            summary.failed_accounts,
            summary.written_rows,
            summary.financial_rows,
            summary.total_retry_count,
        )
        return summary

    def _load_financial_reports(self) -> pd.DataFrame:
        """Читает агрегированные начисления из PostgreSQL для текущей выгрузки.

        Бизнес-сценарий:
        до обращения к WB и Google Sheets job получает данные из
        `public.daily_fin_reports_full` за период с 14.09.2026 по вчерашнюю
        дату БД. Ошибка чтения или нарушение схемы результата останавливает
        запуск, чтобы не публиковать частично актуальную вкладку.
        """
        try:
            dataframe = self.database_cls.read_sql_to_dataframe(
                text(FINANCIAL_REPORTS_QUERY),
                params={"date_from": FINANCIAL_REPORTS_START_DATE},
            )
        except Exception as error:
            logger.error(
                "Не удалось получить агрегированные начисления из PostgreSQL, выгрузка Google Sheets прервана | error_type=%s",
                type(error).__name__,
            )
            raise

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("Результат PostgreSQL должен быть pandas.DataFrame.")

        missing_columns = set(FINANCIAL_REPORTS_COLUMNS) - set(dataframe.columns)
        if missing_columns:
            missing_columns_text = ", ".join(sorted(missing_columns))
            raise ValueError(
                "В результате PostgreSQL отсутствуют колонки: "
                f"{missing_columns_text}",
            )

        logger.info(
            "Агрегированные начисления получены из PostgreSQL | date_from=%s | rows=%s",
            FINANCIAL_REPORTS_START_DATE,
            len(dataframe.index),
        )
        return dataframe

    async def _fetch_account_balance(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        account_name: str,
        token: str,
    ) -> SellerBalanceFetchResult:
        """Ограничивает параллелизм запросов баланса по кабинетам.

        Бизнес-сценарий:
        метод защищает job от лишней одновременной нагрузки на WB API при
        массовом обновлении баланса по нескольким кабинетам.
        """
        async with semaphore:
            return await self.client.fetch_balance(
                session=session,
                account=account_name,
                token=token,
            )

    def _resolve_tokens(self, account: str | None) -> dict[str, str]:
        """Загружает токены WB и при необходимости оставляет один кабинет.

        Бизнес-сценарий:
        job должна брать все доступные кабинеты из конфигурации, а при ручной
        диагностике уметь безопасно запускаться только для одного кабинета.
        """
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

    def _build_row(self, result: SellerBalanceFetchResult) -> dict[str, object]:
        """Собирает одну строку витрины из ответа API по кабинету.

        Бизнес-сценарий:
        на вкладке `Переменные.` хранится одна строка на кабинет, поэтому из
        ответа WB берутся только поля подтверждённого контракта метода баланса.
        """
        return {
            "account": result.account,
            "currency": result.payload.get("currency"),
            "current": result.payload.get("current"),
            "for_withdraw": result.payload.get("for_withdraw"),
        }
