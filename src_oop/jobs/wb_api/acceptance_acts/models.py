"""Dataclass-модели для каркаса модуля актов приёма-передачи WB.

Модели описывают контракты между слоями и помогают заранее зафиксировать,
какие данные критичны для идемпотентности, диагностики и будущей записи в БД.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Literal

from src_oop.jobs.wb_api.acceptance_acts.config import ActType, ParseStatus

if TYPE_CHECKING:
    import pandas as pd


RunStatus = Literal["success", "partial", "failed"]


@dataclass(slots=True)
class WBDocumentMeta:
    """Метаданные документа WB из ответа `documents/list`.

    Сущность передаётся из API-слоя в orchestration и дальше используется для
    группировки батчей и диагностики. Поле `service_name` критично для
    повторного скачивания документа, а `account` и `expected_act_type` важны
    для идемпотентности и трассировки ошибок по аккаунту.
    """

    account: str
    service_name: str
    category: str
    expected_act_type: ActType
    created_at: datetime | None = None
    raw_payload: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class DownloadedDocumentBatch:
    """Результат скачивания батча документов из WB.

    Сущность связывает список `service_name` с полученным base64 payload и
    нужна как контракт между API-слоем и распаковкой архивов. Поля
    `account`, `expected_act_type` и `service_names` критичны для корректной
    повторной обработки и диагностики частичных ошибок по батчам.
    """

    account: str
    expected_act_type: ActType
    service_names: list[str]
    base64_payload: str
    batch_index: int
    downloaded_at: datetime


@dataclass(slots=True)
class ExtractedExcelFile:
    """Excel-файл, извлечённый из вложенных архивов WB.

    Сущность передаётся из слоя распаковки в Excel parser. Поля с именами
    outer/inner entries помогают восстанавливать путь до проблемного файла,
    а `document_number_hint` пригодится для безопасного сопоставления с
    уникальными ключами до полноценного парсинга.
    """

    account: str
    expected_act_type: ActType
    service_name: str | None
    outer_entry_name: str
    inner_entry_name: str
    excel_name: str
    excel_bytes: bytes
    document_number_hint: str | None = None


@dataclass(slots=True)
class ExcelStructureInfo:
    """Диагностическая информация о найденной структуре Excel.

    Сущность нужна для объяснимого парсинга нестабильных Excel-форм и
    последующей отладки проблемных файлов. Поля `selected_header_rows`,
    `data_start_row` и `canonical_headers` особенно важны для диагностики
    расхождений между старыми и новыми форматами актов.
    """

    sheet_name: str | None = None
    header_row_candidates: list[int] = field(default_factory=list)
    selected_header_rows: list[int] = field(default_factory=list)
    data_start_row: int | None = None
    date_cell_candidates: list[str] = field(default_factory=list)
    selected_date_source: str | None = None
    raw_headers: list[str] = field(default_factory=list)
    canonical_headers: list[str] = field(default_factory=list)
    confidence_score: float = 0.0


@dataclass(slots=True)
class ExcelParseResult:
    """Результат чтения и первичного распознавания одного Excel-файла.

    Сущность передаётся из Excel parser в validator и normalizer. Поля
    `document_number`, `document_date`, `actual_act_type` и `status`
    критичны для будущей идемпотентной загрузки и для определения, можно ли
    продолжать обработку файла или его нужно оставить в partial/failed.
    """

    status: ParseStatus
    account: str
    expected_act_type: ActType
    actual_act_type: ActType | None
    document_name: str
    document_number: str | None
    document_date: date | None
    structure_info: ExcelStructureInfo = field(default_factory=ExcelStructureInfo)
    raw_dataframe: "pd.DataFrame | None" = None
    canonical_dataframe: "pd.DataFrame | None" = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class NormalizedFboRow:
    """Нормализованная строка ФБО для таблицы `acceptance_fbo_acts_new`.

    Эта сущность является будущим контрактом между normalizer-ом и repository.
    Поля `vendor_code`, `box_barcode`, `document_number` и `shk_id` критичны,
    потому что входят в уникальный ключ и определяют идемпотентность загрузки.
    """

    num: int
    product_name: str
    unit: str
    barcode: str
    vendor_code: str
    size: str
    kiz: str
    box_barcode: str
    quantity: int
    document: str
    document_number: str
    date: date | None
    shk_id: int
    account: str


@dataclass(slots=True)
class NormalizedFbsRow:
    """Нормализованная строка ФБС для таблицы `acceptance_fbs_acts_new`.

    Сущность передаётся из normalizer-а в repository. Поля `order_number`,
    `sticker` и `document_number` критичны для уникального ключа и безопасных
    повторных запусков, а `document` и `account` нужны для диагностики.
    """

    num: int
    order_number: str
    unit: str
    sticker: str
    quantity: int
    document: str
    document_number: str
    date: date | None
    account: str


@dataclass(slots=True)
class ValidationResult:
    """Итог валидации документа или набора строк.

    Сущность нужна как контракт между validator-ом и orchestration. Поля
    `status`, `errors` и `warnings` позволяют различать фатальные и
    нефатальные проблемы, чтобы один плохой файл не останавливал весь job.
    """

    status: ParseStatus
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DBWriteResult:
    """Результат попытки записи набора строк в БД.

    Сущность возвращается repository-слоем в service и фиксирует, сколько
    строк было принято на вход, сколько дошло до операции записи и какие
    ошибки возникли. Поле `table_name` важно для диагностики, а `status`
    помогает строить итоговый summary job.
    """

    table_name: str
    input_rows: int
    written_rows: int
    status: RunStatus
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class JobRunResult:
    """Итог выполнения одного запуска job по актам WB.

    Сущность нужна как внешний отчёт orchestration-слоя. Поля со счётчиками
    позволяют сравнивать старую и новую реализацию, а `warnings` и `errors`
    нужны для безопасного анализа частичных отказов без реального запуска
    бизнес-логики на этом этапе.
    """

    act_type: ActType | Literal["all"]
    date_from: date | None = None
    date_to: date | None = None
    accounts_total: int = 0
    documents_found: int = 0
    documents_downloaded: int = 0
    excel_files_found: int = 0
    parsed_success: int = 0
    parsed_partial: int = 0
    parsed_failed: int = 0
    normalized_rows: int = 0
    written_rows: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
