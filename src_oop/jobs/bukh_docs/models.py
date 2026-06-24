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
    account_name: str | None = None
    accounts_total: int = 0
    documents_found: int = 0
    documents_downloaded: int = 0
    extracted_files: int = 0
    weekly_report_rows: int = 0
    redeem_rows: int = 0
    written_rows: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DocumentListingDiagnostic:
    account: str
    weekly_documents: int = 0
    redeem_documents: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DocumentListingRunResult:
    status: RunStatus
    date_from: date
    date_to: date
    accounts_total: int = 0
    accounts_with_weekly: list[str] = field(default_factory=list)
    accounts_with_redeem: list[str] = field(default_factory=list)
    accounts_with_both: list[str] = field(default_factory=list)
    accounts_with_redeem_only: list[str] = field(default_factory=list)
    accounts_with_weekly_only: list[str] = field(default_factory=list)
    accounts_without_documents: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class WeeklyProcessingDiagnostic:
    account: str
    weekly_documents: int = 0
    redeem_documents: int = 0
    downloaded: bool = False
    extracted_weekly_files: int = 0
    parsed_weekly_rows: int = 0
    weekly_file_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class WeeklyProcessingRunResult:
    status: RunStatus
    date_from: date
    date_to: date
    accounts_total: int = 0
    accounts_with_weekly_documents: list[str] = field(default_factory=list)
    accounts_with_downloaded_payload: list[str] = field(default_factory=list)
    accounts_with_extracted_weekly_files: list[str] = field(default_factory=list)
    accounts_with_parsed_weekly_rows: list[str] = field(default_factory=list)
    accounts_with_weekly_but_no_extracted_files: list[str] = field(default_factory=list)
    accounts_with_weekly_but_no_parsed_rows: list[str] = field(default_factory=list)
    accounts_without_weekly_documents: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class WeekNRedeemRunResult:
    status: RunStatus
    sql_rows: int = 0
    rows_after_processing: int = 0
    rows_after_filter: int = 0
    unique_accounts: int = 0
    accounts_in_sql: list[str] = field(default_factory=list)
    accounts_after_filter: list[str] = field(default_factory=list)
    missing_redeem_notifications_in_sql: int = 0
    missing_redeem_notifications_after_filter: int = 0
    duplicate_rows_by_account_report: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
