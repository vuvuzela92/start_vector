"""Orchestration-слой для нового модуля актов WB."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from datetime import date
from itertools import islice
from typing import TypeVar

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.wb_api.acceptance_acts.api_client import WBActsClient
from src_oop.jobs.wb_api.acceptance_acts.archive_parser import ActArchiveExtractor
from src_oop.jobs.wb_api.acceptance_acts.config import ACT_TYPE_FBO, ACT_TYPE_FBS, ActType
from src_oop.jobs.wb_api.acceptance_acts.excel_parser import AcceptanceExcelParser
from src_oop.jobs.wb_api.acceptance_acts.models import (
    DBWriteResult,
    DownloadedDocumentBatch,
    ExcelParseResult,
    ExtractedExcelFile,
    JobRunResult,
    NormalizedFboRow,
    NormalizedFbsRow,
    ValidationResult,
)
from src_oop.jobs.wb_api.acceptance_acts.normalizers import (
    FboAcceptanceNormalizer,
    FbsAcceptanceNormalizer,
)
from src_oop.jobs.wb_api.acceptance_acts.repository import (
    AcceptanceActsRepository,
    DryRunAcceptanceActsRepository,
)
from src_oop.jobs.wb_api.acceptance_acts.validators import AcceptanceActsValidator

logger = logging.getLogger(__name__)

ChunkItemT = TypeVar("ChunkItemT")
NormalizedRowT = TypeVar("NormalizedRowT", NormalizedFboRow, NormalizedFbsRow)


def chunked(items: Iterable[ChunkItemT], size: int) -> Iterator[list[ChunkItemT]]:
    """Разбивает итерируемый источник на чанки фиксированного размера."""
    if size <= 0:
        raise ValueError("Размер чанка должен быть положительным числом.")

    iterator = iter(items)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            break
        yield batch


class AcceptanceActsService:
    """Координирует pipeline обработки актов WB по потоковой схеме."""

    def __init__(
        self,
        client: WBActsClient | None = None,
        archive_extractor: ActArchiveExtractor | None = None,
        excel_parser: AcceptanceExcelParser | None = None,
        validator: AcceptanceActsValidator | None = None,
        fbo_normalizer: FboAcceptanceNormalizer | None = None,
        fbs_normalizer: FbsAcceptanceNormalizer | None = None,
        repository: AcceptanceActsRepository | DryRunAcceptanceActsRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
        dry_run: bool = False,
    ) -> None:
        """Собирает зависимости сервиса с поддержкой dependency injection."""
        self.dry_run = dry_run
        self.client = client or WBActsClient()
        self.archive_extractor = archive_extractor or ActArchiveExtractor()
        self.excel_parser = excel_parser or AcceptanceExcelParser()
        self.validator = validator or AcceptanceActsValidator()
        self.fbo_normalizer = fbo_normalizer or FboAcceptanceNormalizer()
        self.fbs_normalizer = fbs_normalizer or FbsAcceptanceNormalizer()
        self.repository = self._resolve_repository(repository)
        self.tokens_loader = tokens_loader or load_api_tokens

    async def run_fbo(
        self,
        date_from: date,
        date_to: date,
        tokens_by_account: Mapping[str, str] | None = None,
    ) -> JobRunResult:
        """Запускает потоковый pipeline для ФБО."""
        logger.info(
            "Старт run_fbo: date_from=%s date_to=%s",
            date_from.isoformat(),
            date_to.isoformat(),
        )
        logger.info("run_fbo mode: dry_run=%s", self.dry_run)
        result = self._build_empty_result(
            act_type=ACT_TYPE_FBO,
            date_from=date_from,
            date_to=date_to,
        )

        account_tokens = self._resolve_account_tokens(tokens_by_account)
        result.accounts_total = len(account_tokens)

        for account, token in account_tokens.items():
            account_result = await self._process_account(
                account=account,
                token=token,
                date_from=date_from,
                date_to=date_to,
                act_type=ACT_TYPE_FBO,
            )
            self._merge_job_results(result, account_result)

        logger.info(
            "run_fbo завершён: accounts=%s documents_downloaded=%s excel_files=%s written_rows=%s warnings=%s errors=%s",
            result.accounts_total,
            result.documents_downloaded,
            result.excel_files_found,
            result.written_rows,
            len(result.warnings),
            len(result.errors),
        )
        logger.info("run_fbo completed with dry_run=%s", self.dry_run)
        return result

    async def run_fbs(
        self,
        date_from: date,
        date_to: date,
        tokens_by_account: Mapping[str, str] | None = None,
    ) -> JobRunResult:
        """Запускает потоковый pipeline для ФБС и non-fatal refresh MV."""
        logger.info(
            "Старт run_fbs: date_from=%s date_to=%s",
            date_from.isoformat(),
            date_to.isoformat(),
        )
        logger.info("run_fbs mode: dry_run=%s", self.dry_run)
        result = self._build_empty_result(
            act_type=ACT_TYPE_FBS,
            date_from=date_from,
            date_to=date_to,
        )

        account_tokens = self._resolve_account_tokens(tokens_by_account)
        result.accounts_total = len(account_tokens)

        for account, token in account_tokens.items():
            account_result = await self._process_account(
                account=account,
                token=token,
                date_from=date_from,
                date_to=date_to,
                act_type=ACT_TYPE_FBS,
            )
            self._merge_job_results(result, account_result)

        if self._should_refresh_fbs(result):
            refresh_result = self.repository.refresh_fbs_check_mv()
            self._apply_refresh_result(result, refresh_result)

        logger.info(
            "run_fbs завершён: accounts=%s documents_downloaded=%s excel_files=%s written_rows=%s warnings=%s errors=%s",
            result.accounts_total,
            result.documents_downloaded,
            result.excel_files_found,
            result.written_rows,
            len(result.warnings),
            len(result.errors),
        )
        logger.info("run_fbs completed with dry_run=%s", self.dry_run)
        return result

    async def run_all(
        self,
        date_from: date,
        date_to: date,
        tokens_by_account: Mapping[str, str] | None = None,
    ) -> JobRunResult:
        """Запускает ФБО и ФБС последовательно и собирает единый summary."""
        logger.info(
            "Старт run_all: date_from=%s date_to=%s",
            date_from.isoformat(),
            date_to.isoformat(),
        )

        logger.info("run_all mode: dry_run=%s", self.dry_run)
        resolved_tokens = self._resolve_account_tokens(tokens_by_account)
        fbo_result = await self.run_fbo(
            date_from=date_from,
            date_to=date_to,
            tokens_by_account=resolved_tokens,
        )
        fbs_result = await self.run_fbs(
            date_from=date_from,
            date_to=date_to,
            tokens_by_account=resolved_tokens,
        )

        result = self._build_empty_result(
            act_type="all",
            date_from=date_from,
            date_to=date_to,
        )
        self._merge_job_results(result, fbo_result)
        self._merge_job_results(result, fbs_result)
        result.accounts_total = max(fbo_result.accounts_total, fbs_result.accounts_total)

        logger.info(
            "run_all завершён: accounts=%s documents_downloaded=%s excel_files=%s written_rows=%s warnings=%s errors=%s",
            result.accounts_total,
            result.documents_downloaded,
            result.excel_files_found,
            result.written_rows,
            len(result.warnings),
            len(result.errors),
        )
        logger.info("run_all completed with dry_run=%s", self.dry_run)
        return result

    def _resolve_repository(
        self,
        repository: AcceptanceActsRepository | DryRunAcceptanceActsRepository | None,
    ) -> AcceptanceActsRepository | DryRunAcceptanceActsRepository:
        """Выбирает repository с учётом dry-run и dependency injection."""
        if repository is not None:
            if self.dry_run:
                logger.info(
                    "Dry-run включён, но используется явно переданный repository: %s",
                    repository.__class__.__name__,
                )
            return repository

        if self.dry_run:
            logger.info("Dry-run включён: используется DryRunAcceptanceActsRepository.")
            return DryRunAcceptanceActsRepository()

        return AcceptanceActsRepository()

    def _should_refresh_fbs(self, result: JobRunResult) -> bool:
        """Определяет, нужен ли вызов refresh MV после ФБС."""
        if self.dry_run:
            return result.normalized_rows > 0
        return result.written_rows > 0

    async def _process_account(
        self,
        account: str,
        token: str,
        date_from: date,
        date_to: date,
        act_type: ActType,
    ) -> JobRunResult:
        """Обрабатывает один аккаунт в рамках одного типа акта."""
        logger.info(
            "Старт обработки аккаунта: account=%s act_type=%s date_from=%s date_to=%s",
            account,
            act_type,
            date_from.isoformat(),
            date_to.isoformat(),
        )
        result = self._build_empty_result(
            act_type=act_type,
            date_from=date_from,
            date_to=date_to,
        )
        result.accounts_total = 1

        try:
            async for batch in self.client.iter_downloaded_batches(
                account=account,
                token=token,
                begin_date=date_from,
                end_date=date_to,
                expected_act_type=act_type,
            ):
                result.documents_found += len(batch.service_names)
                result.documents_downloaded += len(batch.service_names)

                batch_result = await self._process_document_batch(
                    batch=batch,
                    act_type=act_type,
                )
                self._merge_job_results(result, batch_result)
        except Exception as error:
            message = (
                f"Ошибка обработки аккаунта {account} для {act_type}: {error}"
            )
            logger.exception(message)
            result.errors.append(message)

        return result

    async def _process_document_batch(
        self,
        batch: DownloadedDocumentBatch,
        act_type: ActType,
    ) -> JobRunResult:
        """Обрабатывает один скачанный batch документов до уровня Excel-файлов."""
        logger.info(
            "Старт обработки batch: account=%s act_type=%s batch_index=%s service_names=%s",
            batch.account,
            act_type,
            batch.batch_index,
            len(batch.service_names),
        )
        result = self._build_empty_result(act_type=act_type)

        try:
            excel_files = self.archive_extractor.iter_excel_files(batch)
            for excel_file in excel_files:
                result.excel_files_found += 1
                excel_result = self._process_excel_file(
                    excel_file=excel_file,
                    act_type=act_type,
                )
                self._merge_job_results(result, excel_result)
        except Exception as error:
            message = (
                f"Ошибка обработки batch account={batch.account} "
                f"batch_index={batch.batch_index}: {error}"
            )
            logger.exception(message)
            result.errors.append(message)

        return result

    def _process_excel_file(
        self,
        excel_file: ExtractedExcelFile,
        act_type: ActType,
    ) -> JobRunResult:
        """Обрабатывает один Excel-файл до нормализованных строк и записи в БД."""
        logger.info(
            "Старт обработки Excel: account=%s act_type=%s excel=%s",
            excel_file.account,
            act_type,
            excel_file.excel_name,
        )
        result = self._build_empty_result(act_type=act_type)

        try:
            parse_result = self.excel_parser.parse(excel_file)
        except Exception as error:
            message = (
                f"Ошибка parsing Excel {excel_file.excel_name} "
                f"account={excel_file.account}: {error}"
            )
            logger.exception(message)
            result.parsed_failed += 1
            result.errors.append(message)
            return result

        self._increment_parse_counter(result, parse_result)

        try:
            validation_result = self.validator.validate_parse_result(parse_result)
        except Exception as error:
            message = (
                f"Ошибка validation Excel {excel_file.excel_name} "
                f"account={excel_file.account}: {error}"
            )
            logger.exception(message)
            result.errors.append(message)
            return result

        self._append_messages(
            result.warnings,
            validation_result.warnings,
        )
        if validation_result.status == "failed":
            self._append_messages(result.errors, validation_result.errors)
            logger.warning(
                "Validation failed, Excel пропущен: account=%s act_type=%s excel=%s",
                excel_file.account,
                act_type,
                excel_file.excel_name,
            )
            return result

        if validation_result.status == "partial":
            warning = (
                f"Validation partial для Excel {excel_file.excel_name} "
                f"account={excel_file.account}"
            )
            result.warnings.append(warning)

        normalized_rows_total = 0
        normalizer = self._select_normalizer(act_type)
        save_chunk_method = self._select_chunk_saver(act_type)

        try:
            for rows_chunk in normalizer.iter_normalized_chunks(parse_result):
                normalized_rows_total += len(rows_chunk)
                write_result = save_chunk_method(rows_chunk)
                result.normalized_rows += len(rows_chunk)
                self._apply_db_write_result(result, write_result)
        except Exception as error:
            message = (
                f"Ошибка normalizer/repository Excel {excel_file.excel_name} "
                f"account={excel_file.account}: {error}"
            )
            logger.exception(message)
            result.errors.append(message)
            return result

        logger.info(
            "Excel обработан: account=%s act_type=%s excel=%s parse_status=%s validation_status=%s normalized_rows=%s written_rows=%s",
            excel_file.account,
            act_type,
            excel_file.excel_name,
            parse_result.status,
            validation_result.status,
            normalized_rows_total,
            result.written_rows,
        )
        return result

    def _resolve_account_tokens(
        self,
        tokens_by_account: Mapping[str, str] | None,
    ) -> dict[str, str]:
        """Возвращает словарь account -> token для текущего запуска."""
        raw_tokens = tokens_by_account if tokens_by_account is not None else self.tokens_loader()
        resolved: dict[str, str] = {}

        for account, token in raw_tokens.items():
            if not isinstance(account, str) or not account.strip():
                continue
            if not isinstance(token, str) or not token.strip():
                continue
            resolved[account.strip()] = token.strip()

        return resolved

    def _select_normalizer(
        self,
        act_type: ActType,
    ) -> FboAcceptanceNormalizer | FbsAcceptanceNormalizer:
        """Выбирает normalizer по типу акта."""
        if act_type == ACT_TYPE_FBO:
            return self.fbo_normalizer
        if act_type == ACT_TYPE_FBS:
            return self.fbs_normalizer
        raise ValueError(f"Неподдерживаемый act_type: {act_type}")

    def _select_chunk_saver(
        self,
        act_type: ActType,
    ):
        """Выбирает repository chunk saver по типу акта."""
        if act_type == ACT_TYPE_FBO:
            return self.repository.save_fbo_rows_chunk
        if act_type == ACT_TYPE_FBS:
            return self.repository.save_fbs_rows_chunk
        raise ValueError(f"Неподдерживаемый act_type: {act_type}")

    def _build_empty_result(
        self,
        act_type: ActType | str,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> JobRunResult:
        """Создаёт пустой summary для одного участка pipeline."""
        return JobRunResult(
            act_type=act_type,
            date_from=date_from,
            date_to=date_to,
        )

    def _merge_job_results(
        self,
        target: JobRunResult,
        source: JobRunResult,
    ) -> None:
        """Агрегирует счётчики и сообщения из одного summary в другой."""
        target.documents_found += source.documents_found
        target.documents_downloaded += source.documents_downloaded
        target.excel_files_found += source.excel_files_found
        target.parsed_success += source.parsed_success
        target.parsed_partial += source.parsed_partial
        target.parsed_failed += source.parsed_failed
        target.normalized_rows += source.normalized_rows
        target.written_rows += source.written_rows
        self._append_messages(target.warnings, source.warnings)
        self._append_messages(target.errors, source.errors)

    def _apply_db_write_result(
        self,
        result: JobRunResult,
        write_result: DBWriteResult,
    ) -> None:
        """Переносит результат записи chunk в общий summary."""
        result.written_rows += write_result.written_rows
        self._append_messages(result.warnings, write_result.warnings)
        self._append_messages(result.errors, write_result.errors)

        if write_result.status != "success":
            warning = (
                f"Chunk write status={write_result.status} "
                f"table={write_result.table_name}"
            )
            result.warnings.append(warning)

    def _apply_refresh_result(
        self,
        result: JobRunResult,
        refresh_result: DBWriteResult,
    ) -> None:
        """Переносит результат refresh materialized view в общий summary."""
        if refresh_result.status != "success":
            result.warnings.append(
                "Refresh materialized view public.check_act_fbs завершился non-fatal ошибкой."
            )
        self._append_messages(result.warnings, refresh_result.warnings)
        self._append_messages(result.errors, refresh_result.errors)

    def _increment_parse_counter(
        self,
        result: JobRunResult,
        parse_result: ExcelParseResult,
    ) -> None:
        """Увеличивает счётчики parsed_* по статусу parse result."""
        if parse_result.status == "success":
            result.parsed_success += 1
        elif parse_result.status == "partial":
            result.parsed_partial += 1
        else:
            result.parsed_failed += 1

    def _append_messages(
        self,
        target: list[str],
        messages: Sequence[str],
    ) -> None:
        """Добавляет сообщения без дубликатов, сохраняя порядок."""
        for message in messages:
            if message not in target:
                target.append(message)
