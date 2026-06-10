"""Слой распаковки архивов с актами приёма-передачи WB.

Слой превращает транспортный payload из WB Documents API в поток
`ExtractedExcelFile`:
base64 payload -> outer zip -> inner zip -> .xlsx bytes.

На этом этапе здесь нет чтения Excel, нормализации и записи в БД.
"""

from __future__ import annotations

import base64
import binascii
import io
import logging
import re
import zipfile
from collections.abc import Iterator
from typing import Final

from src_oop.jobs.wb_api.acceptance_acts.config import (
    ACT_TYPE_FBO,
    ACT_TYPE_FBS,
    FBO_DOCUMENT_NUMBER_REGEX,
    FBS_DOCUMENT_NUMBER_REGEX,
)
from src_oop.jobs.wb_api.acceptance_acts.models import (
    DownloadedDocumentBatch,
    ExtractedExcelFile,
)

logger = logging.getLogger(__name__)

ZIP_EXTENSION: Final[str] = ".zip"
XLSX_EXTENSION: Final[str] = ".xlsx"


class ActArchiveExtractor:
    """Извлекает Excel-файлы из вложенных архивов WB.

    Слой отвечает только за трансформацию транспортного payload в набор файлов,
    пригодных для Excel parsing. Он не должен принимать решения о типе акта,
    не должен нормализовать строки и не должен работать с БД.
    """

    def extract_excel_files(
        self,
        batch: DownloadedDocumentBatch,
    ) -> list[ExtractedExcelFile]:
        """Извлекает список Excel-файлов из одного скачанного батча.

        Метод оставлен как совместимый wrapper над потоковым
        `iter_excel_files(...)`. Для большого запуска предпочтительно
        использовать именно iterator-версию, чтобы не накапливать все
        файлы в памяти.
        """
        return list(self.iter_excel_files(batch))

    def iter_excel_files(
        self,
        batch: DownloadedDocumentBatch,
    ) -> Iterator[ExtractedExcelFile]:
        """Постепенно отдаёт Excel-файлы из одного скачанного батча.

        Будущий метод нужен для потоковой обработки памяти:
        распаковали batch, нашли один Excel-файл и сразу передали его дальше,
        не накапливая все Excel-файлы за весь запуск в одном списке.
        """
        logger.info(
            "Старт распаковки batch WB: account=%s act_type=%s batch_index=%s service_names=%s",
            batch.account,
            batch.expected_act_type,
            batch.batch_index,
            len(batch.service_names),
        )

        archive_bytes = self._decode_base64_payload(batch.base64_payload)
        outer_entries = self._extract_outer_zip(archive_bytes)

        logger.info(
            "Outer ZIP прочитан: account=%s act_type=%s batch_index=%s outer_entries=%s",
            batch.account,
            batch.expected_act_type,
            batch.batch_index,
            len(outer_entries),
        )

        extracted_count = 0
        for outer_entry_name, outer_entry_bytes in outer_entries:
            logger.debug(
                "Обработка outer entry: account=%s batch_index=%s outer_entry=%s",
                batch.account,
                batch.batch_index,
                outer_entry_name,
            )

            if not self._is_zip_entry(outer_entry_name):
                logger.warning(
                    "Outer entry пропущен: это не ZIP: account=%s batch_index=%s outer_entry=%s",
                    batch.account,
                    batch.batch_index,
                    outer_entry_name,
                )
                continue

            try:
                inner_entries = self._extract_inner_zip(outer_entry_bytes)
            except ValueError as error:
                logger.error(
                    "Ошибка чтения inner ZIP: account=%s batch_index=%s outer_entry=%s error=%s",
                    batch.account,
                    batch.batch_index,
                    outer_entry_name,
                    error,
                )
                continue

            found_excel_in_inner = False
            for inner_entry_name, excel_bytes in inner_entries:
                logger.debug(
                    "Найден entry во вложенном архиве: account=%s batch_index=%s outer_entry=%s inner_entry=%s",
                    batch.account,
                    batch.batch_index,
                    outer_entry_name,
                    inner_entry_name,
                )

                if not self._is_excel_entry(inner_entry_name):
                    continue

                if not excel_bytes:
                    logger.warning(
                        "Пропущен пустой Excel-файл: account=%s batch_index=%s outer_entry=%s inner_entry=%s",
                        batch.account,
                        batch.batch_index,
                        outer_entry_name,
                        inner_entry_name,
                    )
                    continue

                found_excel_in_inner = True
                extracted_count += 1
                service_name = self._match_service_name(
                    batch.service_names,
                    outer_entry_name=outer_entry_name,
                    inner_entry_name=inner_entry_name,
                )
                document_number_hint = self._extract_document_number_hint(
                    expected_act_type=batch.expected_act_type,
                    candidates=(outer_entry_name, inner_entry_name),
                )

                logger.info(
                    "Найден Excel-файл в batch WB: account=%s act_type=%s batch_index=%s outer_entry=%s inner_entry=%s",
                    batch.account,
                    batch.expected_act_type,
                    batch.batch_index,
                    outer_entry_name,
                    inner_entry_name,
                )

                yield ExtractedExcelFile(
                    account=batch.account,
                    expected_act_type=batch.expected_act_type,
                    service_name=service_name,
                    outer_entry_name=outer_entry_name,
                    inner_entry_name=inner_entry_name,
                    excel_name=inner_entry_name,
                    excel_bytes=excel_bytes,
                    document_number_hint=document_number_hint,
                )

            if not found_excel_in_inner:
                logger.warning(
                    "Во вложенном ZIP не найдено Excel-файлов: account=%s batch_index=%s outer_entry=%s",
                    batch.account,
                    batch.batch_index,
                    outer_entry_name,
                )

        logger.info(
            "Распаковка batch WB завершена: account=%s act_type=%s batch_index=%s excel_files=%s",
            batch.account,
            batch.expected_act_type,
            batch.batch_index,
            extracted_count,
        )

    def _decode_base64_payload(self, payload: str) -> bytes:
        """Декодирует base64 payload в бинарный архив.

        Ошибка декодирования считается batch-level проблемой: если исходный
        payload невалиден, дальнейшая распаковка невозможна.
        """
        if not payload or not payload.strip():
            raise ValueError("Получен пустой base64_payload.")

        try:
            return base64.b64decode(payload, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ValueError("Не удалось декодировать base64_payload.") from error

    def _extract_outer_zip(self, archive_bytes: bytes) -> list[tuple[str, bytes]]:
        """Извлекает записи из верхнего архива WB.

        На уровне outer ZIP ошибка считается batch-level проблемой, потому что
        без него нельзя продолжить обработку батча.
        """
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as archive:
                return [
                    (entry_name, self._safe_read_zip_entry(archive, entry_name))
                    for entry_name in archive.namelist()
                    if not entry_name.endswith("/")
                ]
        except zipfile.BadZipFile as error:
            raise ValueError("Outer ZIP повреждён или имеет неверный формат.") from error

    def _extract_inner_zip(self, archive_bytes: bytes) -> list[tuple[str, bytes]]:
        """Извлекает записи из вложенного inner zip.

        Ошибка inner ZIP не должна валить весь batch: caller может залогировать
        проблему и продолжить обработку остальных outer entries.
        """
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as archive:
                return [
                    (entry_name, self._safe_read_zip_entry(archive, entry_name))
                    for entry_name in archive.namelist()
                    if not entry_name.endswith("/")
                ]
        except zipfile.BadZipFile as error:
            raise ValueError("Inner ZIP повреждён или имеет неверный формат.") from error

    def _safe_read_zip_entry(
        self,
        archive: zipfile.ZipFile,
        entry_name: str,
    ) -> bytes:
        """Безопасно читает одну запись из ZIP-архива."""
        try:
            with archive.open(entry_name) as entry:
                return entry.read()
        except KeyError as error:
            raise ValueError(f"Entry {entry_name} отсутствует в ZIP-архиве.") from error
        except OSError as error:
            raise ValueError(f"Не удалось прочитать entry {entry_name}.") from error

    def _is_zip_entry(self, entry_name: str) -> bool:
        """Проверяет, похожа ли запись на вложенный ZIP."""
        return entry_name.lower().endswith(ZIP_EXTENSION)

    def _is_excel_entry(self, entry_name: str) -> bool:
        """Проверяет, похожа ли запись на Excel-файл."""
        return entry_name.lower().endswith(XLSX_EXTENSION)

    def _extract_document_number_hint(
        self,
        expected_act_type: str,
        candidates: tuple[str, ...],
    ) -> str | None:
        """Пытается извлечь номер документа из имён archive entries.

        Значение используется только как hint. Если номер не найден, это не
        считается ошибкой распаковки.
        """
        pattern = (
            FBS_DOCUMENT_NUMBER_REGEX
            if expected_act_type == ACT_TYPE_FBS
            else FBO_DOCUMENT_NUMBER_REGEX
        )

        for candidate in candidates:
            match = re.search(pattern, candidate, flags=re.IGNORECASE)
            if match:
                return match.group(1)

        logger.debug(
            "Не удалось извлечь document_number_hint: act_type=%s candidates=%s",
            expected_act_type,
            candidates,
        )
        return None

    def _match_service_name(
        self,
        service_names: list[str],
        outer_entry_name: str,
        inner_entry_name: str,
    ) -> str | None:
        """Пытается безопасно сопоставить Excel-файл конкретному serviceName.

        Если соответствие неоднозначно, метод возвращает `None` и оставляет
        решение следующему этапу, чтобы не придумывать неверное сопоставление.
        """
        matching = [
            service_name
            for service_name in service_names
            if service_name in outer_entry_name or service_name in inner_entry_name
        ]

        if len(matching) == 1:
            return matching[0]

        if len(matching) > 1:
            logger.warning(
                "Найдено несколько кандидатов service_name для Excel-файла: outer_entry=%s inner_entry=%s candidates=%s",
                outer_entry_name,
                inner_entry_name,
                matching,
            )
            return None

        logger.debug(
            "Не удалось однозначно сопоставить service_name: outer_entry=%s inner_entry=%s",
            outer_entry_name,
            inner_entry_name,
        )
        return None
