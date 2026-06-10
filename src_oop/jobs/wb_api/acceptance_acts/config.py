"""Конфигурационные константы для модуля актов приёма-передачи WB.

На этом этапе файл фиксирует подтверждённые контракты старой реализации и
служит единым источником настроек для будущих слоёв: API, распаковки архивов,
Excel parsing, нормализации и записи в БД.
"""

from __future__ import annotations

from typing import Final, Literal

ActType = Literal["fbo", "fbs"]
ParseStatus = Literal["success", "partial", "failed"]

DOCUMENTS_LIST_URL: Final[str] = (
    "https://documents-api.wildberries.ru/api/v1/documents/list"
)
DOCUMENTS_DOWNLOAD_ALL_URL: Final[str] = (
    "https://documents-api.wildberries.ru/api/v1/documents/download/all"
)

ACT_TYPE_FBO: Final[ActType] = "fbo"
ACT_TYPE_FBS: Final[ActType] = "fbs"
SUPPORTED_ACT_TYPES: Final[tuple[ActType, ...]] = (ACT_TYPE_FBO, ACT_TYPE_FBS)

FBO_DOCUMENT_CATEGORY: Final[str] = "act-income"
FBS_DOCUMENT_CATEGORY: Final[str] = "act-income-mp"

DOCUMENT_BATCH_SIZE: Final[int] = 50
NORMALIZED_ROWS_CHUNK_SIZE: Final[int] = 1000
DB_WRITE_CHUNK_SIZE: Final[int] = 1000
REQUEST_TIMEOUT_SECONDS: Final[float] = 60.0
RETRY_ATTEMPTS: Final[int] = 5
RETRY_BASE_DELAY_SECONDS: Final[int] = 10
MAX_CONCURRENT_ACCOUNTS: Final[int] = 5
MAX_CONCURRENT_DOWNLOADS: Final[int] = 5

FBO_TABLE_NAME: Final[str] = "acceptance_fbo_acts_new"
FBS_TABLE_NAME: Final[str] = "acceptance_fbs_acts_new"

FBO_UNIQUE_KEYS: Final[tuple[str, ...]] = (
    "vendor_code",
    "box_barcode",
    "document_number",
    "shk_id",
)
FBS_UNIQUE_KEYS: Final[tuple[str, ...]] = (
    "order_number",
    "sticker",
    "document_number",
)

FBO_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "vendor_code",
    "box_barcode",
    "document_number",
    "date",
)
FBS_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "order_number",
    "sticker",
    "document_number",
    "date",
)

FBO_OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "num",
    "product_name",
    "unit",
    "barcode",
    "vendor_code",
    "size",
    "kiz",
    "box_barcode",
    "quantity",
    "document",
    "document_number",
    "date",
    "shk_id",
    "account",
)
FBS_OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "num",
    "order_number",
    "unit",
    "sticker",
    "quantity",
    "document",
    "document_number",
    "date",
    "account",
)

FBO_DOCUMENT_NUMBER_REGEX: Final[str] = r"(\d+)\.zip"
FBS_DOCUMENT_NUMBER_REGEX: Final[str] = r"act-income-mp-(\d+)\.zip"

FBO_DEFAULT_KIZ: Final[str] = "Нет КИЗов"
FBO_DEFAULT_SHK_ID: Final[int] = 0
FBO_DEFAULT_QUANTITY: Final[int] = 1
FBS_DEFAULT_QUANTITY: Final[int] = 0

PARSER_VERSION: Final[str] = "1.0"

CANONICAL_COLUMN_NAMES: Final[tuple[str, ...]] = (
    "num",
    "product_name",
    "order_number",
    "unit",
    "sticker",
    "barcode",
    "vendor_code",
    "size",
    "kiz",
    "box_barcode",
    "quantity",
    "document",
    "document_number",
    "date",
    "shk_id",
    "account",
)

# TODO: Расширить словарь после проверки реальных Excel-файлов из старого контура.
EXCEL_COLUMN_SYNONYMS: Final[dict[str, tuple[str, ...]]] = {
    "num": ("№ п/п", "№ п\\п", "номер строки"),
    "product_name": ("товар (наименование)", "товар", "наименование товара"),
    "order_number": ("номер заказа", "заказ", "№ заказа"),
    "unit": ("ед. изм.", "ед изм", "единица измерения"),
    "sticker": (
        "фактически принято - стикер/этикетка",
        "стикер/этикетка",
        "стикер",
        "этикетка",
    ),
    "barcode": (
        "фактически принято - баркод",
        "баркод",
        "штрихкод",
    ),
    "vendor_code": ("артикул продавца", "артикул", "код продавца"),
    "size": ("сорт, размер", "размер", "сорт"),
    "kiz": ("киз", "маркировка", "код маркировки"),
    "box_barcode": ("шк короба", "штрихкод короба", "barcode короба"),
    "quantity": ("кол-во", "количество", "шт"),
    "document": ("документ", "имя документа"),
    "document_number": ("номер_документа", "номер документа"),
    "date": ("дата", "дата документа"),
    "shk_id": ("шк товара", "штрихкод товара", "шк"),
}

FBO_SIGNATURE_FIELDS: Final[tuple[str, ...]] = ("box_barcode", "vendor_code", "barcode")
FBS_SIGNATURE_FIELDS: Final[tuple[str, ...]] = ("sticker", "order_number")

REFRESH_FBS_MATERIALIZED_VIEW_SQL: Final[str] = (
    "REFRESH MATERIALIZED VIEW public.check_act_fbs;"
)
