from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import aiohttp

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.advert_spend.client import AdvertSpendFetchResult, WBAdvertSpendClient
from src_oop.jobs.advert_spend.config import MAX_CONCURRENT_ACCOUNTS
from src_oop.jobs.advert_spend.normalizer import AdvertSpendNormalizer
from src_oop.jobs.advert_spend.repository import AdvertSpendRepository, AdvertSpendSaveResult

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AdvertSpendRunSummary:
    accounts_total: int = 0
    accounts_processed: int = 0
    succeeded_accounts: list[str] = field(default_factory=list)
    accounts_without_rows: list[str] = field(default_factory=list)
    failed_accounts: list[str] = field(default_factory=list)
    raw_rows: int = 0
    normalized_rows: int = 0
    written_rows: int = 0
    dropped_missing_key_rows: int = 0
    collapsed_duplicate_rows: int = 0
    total_retry_count: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None
    warnings: list[str] = field(default_factory=list)


class AdvertSpendService:
    """Оркестрирует получение, нормализацию и запись рекламных затрат WB."""

    def __init__(
        self,
        client: WBAdvertSpendClient | None = None,
        normalizer: AdvertSpendNormalizer | None = None,
        repository: AdvertSpendRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
    ) -> None:
        self.client = client or WBAdvertSpendClient()
        self.normalizer = normalizer or AdvertSpendNormalizer()
        self.repository = repository or AdvertSpendRepository()
        self.tokens_loader = tokens_loader or load_api_tokens

    async def run(
        self,
        date_from: date,
        date_to: date,
        account: str | None = None,
    ) -> AdvertSpendRunSummary:
        tokens_by_account = self._resolve_tokens(account=account)
        summary = AdvertSpendRunSummary(accounts_total=len(tokens_by_account))
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)
        api_date_from = date_from - timedelta(days=1)

        logger.info(
            "Старт AdvertSpendService.run | date_from=%s | date_to=%s | api_date_from=%s | accounts_total=%s | account_filter=%s",
            date_from.isoformat(),
            date_to.isoformat(),
            api_date_from.isoformat(),
            summary.accounts_total,
            account,
        )

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_account(
                    semaphore=semaphore,
                    session=session,
                    account_name=account_name,
                    token=token,
                    date_from=api_date_from,
                    date_to=date_to,
                )
                for account_name, token in tokens_by_account.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        account_payloads: list[list[dict]] = []
        clean_accounts: list[str] = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(
                    "Ошибка получения advert_spend | error_type=%s | error=%s",
                    type(result).__name__,
                    result,
                )
                summary.warnings.append(str(result))
                continue

            summary.accounts_processed += 1
            summary.succeeded_accounts.append(result.account)
            clean_accounts.append(result.account.upper())
            if not result.payload:
                summary.accounts_without_rows.append(result.account.upper())
            summary.raw_rows += len(result.payload)
            summary.total_retry_count += result.retries_used
            account_payloads.append(result.payload)

        normalized_df = self.normalizer.normalize(
            account_payloads=account_payloads,
            date_from=date_from,
            date_to=date_to,
        )
        summary.normalized_rows = len(normalized_df.index)
        save_result = self.repository.save(
            dataframe=normalized_df,
            date_from=date_from,
            date_to=date_to,
            accounts=clean_accounts,
        )
        if not isinstance(save_result, AdvertSpendSaveResult):
            raise TypeError("save_result должен быть экземпляром AdvertSpendSaveResult.")

        summary.written_rows = save_result.written_rows
        summary.dropped_missing_key_rows = save_result.dropped_missing_key_rows
        summary.collapsed_duplicate_rows = save_result.collapsed_duplicate_rows
        summary.failed_accounts = [
            account_name
            for account_name in tokens_by_account
            if account_name not in summary.succeeded_accounts
        ]
        summary.finished_at = datetime.now()

        logger.info(
            "Завершён AdvertSpendService.run | accounts_processed=%s | accounts_without_rows=%s | failed_accounts=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | dropped_missing_key_rows=%s | collapsed_duplicate_rows=%s | retries=%s",
            summary.accounts_processed,
            summary.accounts_without_rows,
            summary.failed_accounts,
            summary.raw_rows,
            summary.normalized_rows,
            summary.written_rows,
            summary.dropped_missing_key_rows,
            summary.collapsed_duplicate_rows,
            summary.total_retry_count,
        )
        return summary

    async def _fetch_account(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        account_name: str,
        token: str,
        date_from: date,
        date_to: date,
    ) -> AdvertSpendFetchResult:
        async with semaphore:
            return await self.client.fetch_account_spend(
                session=session,
                account=account_name,
                token=token,
                date_from=date_from,
                date_to=date_to,
            )

    def _resolve_tokens(self, account: str | None) -> dict[str, str]:
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
