"""Оркестрация выгрузки возвратов покупателей WB в Google Sheets."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime

import aiohttp

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.returns_to_customers.client import (
    BuyersReturnsFetchResult,
    WBBuyersReturnsClient,
)
from src_oop.jobs.returns_to_customers.config import (
    ARCHIVE_STATES,
    MAX_CONCURRENT_ACCOUNTS,
    PAGE_LIMIT,
    REQUEST_INTERVAL_SECONDS,
)
from src_oop.jobs.returns_to_customers.normalizer import (
    BuyersReturnsNormalizer,
    ClaimEnvelope,
)
from src_oop.jobs.returns_to_customers.repository import (
    BuyersReturnsRepository,
    BuyersReturnsSaveResult,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BuyersReturnsRunSummary:
    """Сводка полной выгрузки возвратов, чтобы планировщик и разработчик видели итог сценария."""

    accounts_total: int = 0
    accounts_processed: int = 0
    pages_received: int = 0
    raw_rows: int = 0
    normalized_rows: int = 0
    written_rows: int = 0
    db_written_rows: int = 0
    reconciled_rows: int = 0
    total_retry_count: int = 0
    succeeded_accounts: list[str] = field(default_factory=list)
    failed_accounts: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None


class BuyersReturnsService:
    """Собирает возвраты покупателей со всех кабинетов и публикует общую витрину в Google Sheets."""

    def __init__(
        self,
        client: WBBuyersReturnsClient | None = None,
        normalizer: BuyersReturnsNormalizer | None = None,
        repository: BuyersReturnsRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
        request_interval_seconds: float = REQUEST_INTERVAL_SECONDS,
    ) -> None:
        """Собирает заменяемые зависимости сценария, чтобы job было удобно поддерживать и тестировать."""
        self.client = client or WBBuyersReturnsClient()
        self.normalizer = normalizer or BuyersReturnsNormalizer()
        self.repository = repository or BuyersReturnsRepository()
        self.tokens_loader = tokens_loader or load_api_tokens
        self.request_interval_seconds = request_interval_seconds

    async def run(self, account: str | None = None) -> BuyersReturnsRunSummary:
        """Запускает основной сценарий: WB API -> PostgreSQL, а Google Sheets только при явном флаге."""
        return await self.run_with_options(
            account=account,
            dry_run=False,
            write_to_google=False,
        )

    async def run_with_options(
        self,
        account: str | None = None,
        dry_run: bool = False,
        write_to_google: bool = False,
    ) -> BuyersReturnsRunSummary:
        """Запускает возвраты покупателей с обязательной записью в БД и опциональной выгрузкой в Google Sheets."""
        tokens_by_account = self._resolve_tokens(account=account)
        summary = BuyersReturnsRunSummary(accounts_total=len(tokens_by_account))
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)

        logger.info(
            "Старт выгрузки возвратов покупателей WB | accounts_total=%s | account_filter=%s | dry_run=%s | write_to_google=%s",
            summary.accounts_total,
            account,
            dry_run,
            write_to_google,
        )

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._collect_account_claims(
                    semaphore=semaphore,
                    session=session,
                    account_name=account_name,
                    token=token,
                )
                for account_name, token in tokens_by_account.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        envelopes: list[ClaimEnvelope] = []
        for account_name, result in zip(tokens_by_account, results, strict=True):
            if isinstance(result, Exception):
                summary.failed_accounts.append(account_name)
                summary.warnings.append(str(result))
                logger.error(
                    "Не удалось выгрузить возвраты покупателей по кабинету, но job продолжает обработку остальных | account=%s | error=%s",
                    account_name,
                    result,
                )
                continue

            summary.accounts_processed += 1
            summary.succeeded_accounts.append(account_name)
            summary.pages_received += result["pages_received"]
            summary.raw_rows += len(result["envelopes"])
            summary.total_retry_count += result["retries_used"]
            envelopes.extend(result["envelopes"])

        database_dataframe = self.normalizer.normalize_for_database(envelopes)
        normalized_dataframe = self.normalizer.normalize(envelopes)
        summary.normalized_rows = len(normalized_dataframe.index)
        save_result = self.repository.save(
            google_dataframe=normalized_dataframe,
            database_dataframe=database_dataframe,
            dry_run=dry_run,
            write_to_google=write_to_google,
        )
        self._apply_save_result(summary=summary, save_result=save_result)
        summary.finished_at = datetime.now()

        logger.info(
            "Завершена выгрузка возвратов покупателей WB | accounts_processed=%s | pages_received=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | db_written_rows=%s | failed_accounts=%s | retries=%s | dry_run=%s | write_to_google=%s",
            summary.accounts_processed,
            summary.pages_received,
            summary.raw_rows,
            summary.normalized_rows,
            summary.written_rows,
            summary.db_written_rows,
            summary.failed_accounts,
            summary.total_retry_count,
            dry_run,
            write_to_google,
        )
        return summary

    async def _collect_account_claims(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        account_name: str,
        token: str,
    ) -> dict[str, object]:
        """Собирает все страницы возвратов одного кабинета, чтобы не смешивать его лимиты с другими."""
        async with semaphore:
            pages_received = 0
            retries_used = 0
            envelopes: list[ClaimEnvelope] = []

            for is_archive in ARCHIVE_STATES:
                offset = 0
                while True:
                    fetch_result = await self.client.fetch_claims_page(
                        session=session,
                        account=account_name,
                        token=token,
                        is_archive=is_archive,
                        limit=PAGE_LIMIT,
                        offset=offset,
                    )
                    pages_received += 1
                    retries_used += fetch_result.retries_used
                    claims = self._extract_claims(fetch_result)
                    envelopes.extend(
                        ClaimEnvelope(
                            account=account_name,
                            is_archive=is_archive,
                            claim=claim,
                        )
                        for claim in claims
                    )
                    if len(claims) < PAGE_LIMIT:
                        break

                    offset += PAGE_LIMIT
                    logger.info(
                        "Ожидание перед следующей страницей возвратов WB | account=%s | is_archive=%s | next_offset=%s | seconds=%s",
                        account_name,
                        is_archive,
                        offset,
                        self.request_interval_seconds,
                    )
                    await asyncio.sleep(self.request_interval_seconds)

                # Между active/archive делаем ту же паузу, чтобы не бить лимиты одного кабинета.
                await asyncio.sleep(self.request_interval_seconds)

            return {
                "pages_received": pages_received,
                "retries_used": retries_used,
                "envelopes": envelopes,
            }

    def _extract_claims(self, fetch_result: BuyersReturnsFetchResult) -> list[dict]:
        """Извлекает список заявок из ответа WB и валидирует базовый контракт ответа."""
        claims = fetch_result.payload.get("claims", [])
        if claims is None:
            return []
        if not isinstance(claims, list):
            raise TypeError(
                "WB API вернул неожиданный формат claims для возвратов покупателей: "
                f"account={fetch_result.account}"
            )
        return [claim for claim in claims if isinstance(claim, dict)]

    def _resolve_tokens(self, account: str | None) -> dict[str, str]:
        """Загружает токены WB и при необходимости оставляет только один выбранный кабинет."""
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
        summary: BuyersReturnsRunSummary,
        save_result: BuyersReturnsSaveResult,
    ) -> None:
        """Переносит итоги записи в финальную сводку job для логов и мониторинга."""
        summary.written_rows = save_result.written_rows
        summary.db_written_rows = save_result.db_written_rows
        summary.reconciled_rows = save_result.reconciled_rows
