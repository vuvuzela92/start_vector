from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal


RunStatus = Literal["success", "partial", "failed"]


@dataclass(slots=True)
class DocumentRequest:
    account: str
    doc_type: str
    service_name: str
    extension: str


@dataclass(slots=True)
class DownloadedDocumentsPayload:
    account: str
    document_requests: list[DocumentRequest]
    base64_document: str


@dataclass(slots=True)
class ExtractedFile:
    account: str
    doc_type: str
    path: str
    content: bytes


@dataclass(slots=True)
class SaveResult:
    table_name: str
    input_rows: int
    written_rows: int
    status: RunStatus
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class JobRunResult:
    status: RunStatus
    date_from: date
    date_to: date
    accounts_total: int = 0
    documents_found: int = 0
    documents_downloaded: int = 0
    extracted_files: int = 0
    weekly_report_rows: int = 0
    redeem_rows: int = 0
    written_rows: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
