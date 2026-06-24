from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from datetime import date, timedelta

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.bukh_docs.api_client import WBDocsClient
from src_oop.jobs.bukh_docs.config import (
    DEFAULT_DATE_TO_OFFSET,
    DEFAULT_DAYS_BACK,
    MAX_CONCURRENCY,
)
from src_oop.jobs.bukh_docs.models import JobRunResult
from src_oop.jobs.bukh_docs.parser import BukhDocsParser
from src_oop.jobs.bukh_docs.repository import BukhDocsRepository

logger = logging.getLogger(__name__)


class BukhDocsService:
    """Orchestration-слой для получения и загрузки бухгалтерских документов WB."""

    def __init__(
        self,
        client: WBDocsClient | None = None,
        parser: BukhDocsParser | None = None,
        repository: BukhDocsRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
        max_concurrency: int = MAX_CONCURRENCY,
    ) -> None:
        self._client = client or WBDocsClient()
        self._parser = parser or BukhDocsParser()
        self._repository = repository or BukhDocsRepository()
        self._tokens_loader = tokens_loader or load_api_tokens
        self._max_concurrency = max_concurrency

    async def run(
        self,
        date_from: date | None = None,
        date_to: date | None = None,
        tokens_by_account: Mapping[str, str] | None = None,
    ) -> JobRunResult:
        resolved_date_from, resolved_date_to = self._resolve_period(date_from, date_to)
        account_tokens = self._resolve_tokens(tokens_by_account)
        logger.info(
            "Старт bukh_docs job: date_from=%s date_to=%s accounts=%s max_concurrency=%s",
            resolved_date_from.isoformat(),
            resolved_date_to.isoformat(),
            len(account_tokens),
            self._max_concurrency,
        )

        result = JobRunResult(
            status="success",
            date_from=resolved_date_from,
            date_to=resolved_date_to,
            accounts_total=len(account_tokens),
        )

        semaphore = asyncio.Semaphore(self._max_concurrency)
        tasks = [
            self._process_account(
                semaphore=semaphore,
                account=account,
                token=token,
                date_from=resolved_date_from,
                date_to=resolved_date_to,
            )
            for account, token in account_tokens.items()
        ]

        account_results = await asyncio.gather(*tasks)
        for account_result in account_results:
            self._merge_results(result, account_result)

        if result.errors and result.written_rows:
            result.status = "partial"
        elif result.errors:
            result.status = "failed"

        logger.info(
            "Завершен bukh_docs job: status=%s documents_found=%s documents_downloaded=%s extracted_files=%s weekly_rows=%s redeem_rows=%s written_rows=%s warnings=%s errors=%s",
            result.status,
            result.documents_found,
            result.documents_downloaded,
            result.extracted_files,
            result.weekly_report_rows,
            result.redeem_rows,
            result.written_rows,
            len(result.warnings),
            len(result.errors),
        )
        return result

    async def _process_account(
        self,
        semaphore: asyncio.Semaphore,
        account: str,
        token: str,
        date_from: date,
        date_to: date,
    ) -> JobRunResult:
        result = JobRunResult(
            status="success",
            date_from=date_from,
            date_to=date_to,
            accounts_total=1,
        )

        async with semaphore:
            try:
                logger.info(
                    "Старт обработки аккаунта bukh_docs: account=%s date_from=%s date_to=%s",
                    account,
                    date_from.isoformat(),
                    date_to.isoformat(),
                )
                documents = await self._client.list_documents_for_account(
                    account=account,
                    token=token,
                    date_from=date_from,
                    date_to=date_to,
                )
                result.documents_found = len(documents)
                if not documents:
                    logger.info("Для аккаунта нет документов: account=%s", account)
                    return result

                downloaded_payload = await self._client.download_documents_for_account(
                    account=account,
                    token=token,
                    document_requests=documents,
                )
                if downloaded_payload is None:
                    result.status = "failed"
                    result.errors.append(f"Не удалось скачать документы для аккаунта {account}.")
                    return result

                result.documents_downloaded = len(documents)
                extracted_files = self._parser.extract_files(downloaded_payload)
                result.extracted_files = len(extracted_files)

                unknown_files = [file.path for file in extracted_files if file.doc_type == "unknown"]
                if unknown_files:
                    warning = (
                        f"Аккаунт {account}: не удалось определить тип документа для "
                        f"{len(unknown_files)} файлов."
                    )
                    result.warnings.append(warning)
                    logger.warning("%s Files=%s", warning, unknown_files[:10])

                weekly_report_files = [
                    file for file in extracted_files if file.doc_type == "weekly-implementation-report"
                ]
                redeem_notification_files = [
                    file for file in extracted_files if file.doc_type == "redeem-notification"
                ]

                weekly_reports_df = self._parser.parse_weekly_reports(weekly_report_files)
                redeem_notifications_df = self._parser.parse_redeem_notifications(
                    redeem_notification_files
                )

                result.weekly_report_rows = len(weekly_reports_df.index)
                result.redeem_rows = len(redeem_notifications_df.index)

                weekly_save_result = self._repository.save_weekly_reports(weekly_reports_df)
                redeem_save_result = self._repository.save_redeem_notifications(
                    redeem_notifications_df
                )

                result.written_rows += (
                    weekly_save_result.written_rows + redeem_save_result.written_rows
                )
                result.warnings.extend(weekly_save_result.warnings)
                result.warnings.extend(redeem_save_result.warnings)
                result.errors.extend(weekly_save_result.errors)
                result.errors.extend(redeem_save_result.errors)

                if result.errors and result.written_rows:
                    result.status = "partial"
                elif result.errors:
                    result.status = "failed"

                logger.info(
                    "Завершена обработка аккаунта bukh_docs: account=%s status=%s documents=%s extracted_files=%s weekly_files=%s redeem_files=%s weekly_rows=%s redeem_rows=%s written_rows=%s",
                    account,
                    result.status,
                    result.documents_found,
                    result.extracted_files,
                    len(weekly_report_files),
                    len(redeem_notification_files),
                    result.weekly_report_rows,
                    result.redeem_rows,
                    result.written_rows,
                )
            except Exception as error:
                logger.exception("Ошибка обработки аккаунта %s: %s", account, error)
                result.status = "failed"
                result.errors.append(f"Ошибка обработки аккаунта {account}: {error}")

        return result

    def _resolve_period(
        self,
        date_from: date | None,
        date_to: date | None,
    ) -> tuple[date, date]:
        if date_to is None:
            date_to = date.today() - DEFAULT_DATE_TO_OFFSET
        if date_from is None:
            date_from = date_to - timedelta(days=DEFAULT_DAYS_BACK)
        if date_from > date_to:
            raise ValueError("Параметр date_from не может быть позже date_to.")
        return date_from, date_to

    def _resolve_tokens(
        self,
        tokens_by_account: Mapping[str, str] | None,
    ) -> dict[str, str]:
        raw_tokens = tokens_by_account if tokens_by_account is not None else self._tokens_loader()
        if not isinstance(raw_tokens, Mapping):
            raise TypeError("load_api_tokens() должен возвращать Mapping account -> token.")

        resolved_tokens = {
            account.strip(): token.strip()
            for account, token in raw_tokens.items()
            if isinstance(account, str)
            and account.strip()
            and isinstance(token, str)
            and token.strip()
        }
        logger.info("Подготовлены токены для bukh_docs: accounts=%s", len(resolved_tokens))
        return resolved_tokens

    def _merge_results(self, target: JobRunResult, source: JobRunResult) -> None:
        target.documents_found += source.documents_found
        target.documents_downloaded += source.documents_downloaded
        target.extracted_files += source.extracted_files
        target.weekly_report_rows += source.weekly_report_rows
        target.redeem_rows += source.redeem_rows
        target.written_rows += source.written_rows
        target.warnings.extend(source.warnings)
        target.errors.extend(source.errors)
