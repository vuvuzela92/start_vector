"""Конфигурация job выгрузки возвратов покупателей WB."""

from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy import BigInteger, Boolean, DateTime, Integer, Numeric, Text

CLAIMS_URL = "https://returns-api.wildberries.ru/api/v1/claims"

GOOGLE_TABLE_TITLE = "Start-Потенциал матрицы"
GOOGLE_SHEET_TITLE = "Возвраты"

REQUEST_TIMEOUT_SECONDS = 40
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 3
RETRY_MAX_SLEEP_SECONDS = 30

# Ограничиваем количество одновременно обрабатываемых кабинетов, чтобы не создавать
# лишнее давление на WB API и не повышать риск 429.
MAX_CONCURRENT_ACCOUNTS = 3
PAGE_LIMIT = 200
REQUEST_INTERVAL_SECONDS = 3.0

ARCHIVE_STATES: tuple[bool, ...] = (False, True)

DB_TABLE_SCHEMA = "public"
DB_TABLE_NAME = "claims"
DB_KEY_COLUMNS: tuple[str, ...] = ("id",)


@dataclass(frozen=True, slots=True)
class SheetConfig:
    """Параметры листа, куда job публикует витрину возвратов для бизнес-команды."""

    table_title: str
    sheet_title: str


SHEET_CONFIG = SheetConfig(
    table_title=GOOGLE_TABLE_TITLE,
    sheet_title=GOOGLE_SHEET_TITLE,
)

DATABASE_COLUMN_RENAME_MAP: dict[str, str] = {
    "origin_id_info_checked_pvz": "origin_id_checked_pvz",
    "origin_id_info_client": "origin_id_client",
    "origin_id_info_system": "origin_id_system",
}

# Русские названия для наиболее полезных бизнес-полей WB API.
COLUMN_RENAME_MAP: dict[str, str] = {
    "account": "Аккаунт",
    "is_archive": "В архиве",
    "id": "ID заявки",
    "claim_id": "ID заявки",
    "dt": "Дата заявки WB",
    "imt_name": "Название IMT",
    "order_dt": "Дата заказа WB",
    "dt_update": "Дата обновления WB",
    "claim_type": "Тип заявки (код)",
    "claim_type_text": "Тип заявки",
    "status": "Статус заявки (код)",
    "status_text": "Статус заявки",
    "status_ex": "Статус товара (код)",
    "status_ex_text": "Статус товара",
    "nm_id": "Артикул WB",
    "imt_id": "IMT ID",
    "subject_name": "Предмет",
    "brand_name": "Бренд",
    "supplier_article": "Артикул продавца",
    "barcode": "Баркод",
    "size": "Размер",
    "user_comment": "Комментарий покупателя",
    "wb_comment": "Комментарий WB",
    "created_at": "Дата создания",
    "updated_at": "Дата обновления",
    "order_created_at": "Дата заказа",
    "return_deadline": "Срок возврата",
    "return_address": "Адрес возврата",
    "office_id": "ID ПВЗ",
    "office_name": "ПВЗ",
    "office_address": "Адрес ПВЗ",
    "actions": "Доступные действия",
    "photos": "Фото",
    "video_paths": "Видео пути",
    "videos": "Видео",
    "attachments": "Вложения",
    "price": "Цена",
    "currency_code": "Код валюты",
    "srid": "SRID",
    "origin_id_checked_pvz": "origin_id_checked_pvz",
    "origin_id_client": "origin_id_client",
    "origin_id_system": "origin_id_system",
    "delivery_dt": "Дата доставки",
    "reason": "Причина",
    "claim_reason": "Причина заявки",
    "updated_at_export": "Дата обновления выгрузки",
}

# Приоритетный порядок колонок для листа. Остальные поля WB добавляются следом.
PRIORITY_COLUMNS: tuple[str, ...] = (
    "Аккаунт",
    "В архиве",
    "ID заявки",
    "Дата заявки WB",
    "Тип заявки",
    "Тип заявки (код)",
    "Статус заявки",
    "Статус заявки (код)",
    "Статус товара",
    "Статус товара (код)",
    "Артикул WB",
    "Артикул продавца",
    "Бренд",
    "Предмет",
    "Комментарий покупателя",
    "Комментарий WB",
    "Доступные действия",
    "Дата создания",
    "Дата обновления",
    "Фото",
    "Видео пути",
    "Видео",
    "Вложения",
    "Дата обновления выгрузки",
)

# Справочники кодов WB API для человекочитаемой выгрузки в лист возвратов.
# Для status_ex используем подтверждённые значения из документации getV1Claims.
ENUM_TEXT_MAPS: dict[str, dict[int, str]] = {
    "status_ex": {
        0: "Заявка на рассмотрении",
        1: "Товар остается у покупателя (заявка отклонена)",
        2: "Покупатель сдает товар на WB, товар отправляется в утиль",
        5: "Товар остается у покупателя (заявка одобрена)",
        8: "Товар будет возвращен в реализацию после проверки WB",
        10: "Товар возвращается продавцу",
    },
    "status": {
        0: "На рассмотрении",
        1: "Отказ",
        2: "Одобрено",
    },
    "claim_type": {
        1: "Портал покупателей",
        3: "Чат",
    },
}

DB_COLUMNS: tuple[str, ...] = (
    "id",
    "account",
    "is_archive",
    "claim_id",
    "dt",
    "imt_name",
    "order_dt",
    "dt_update",
    "claim_type",
    "claim_type_text",
    "status",
    "status_text",
    "status_ex",
    "status_ex_text",
    "nm_id",
    "imt_id",
    "subject_name",
    "brand_name",
    "supplier_article",
    "barcode",
    "size",
    "user_comment",
    "wb_comment",
    "created_at",
    "updated_at",
    "order_created_at",
    "return_deadline",
    "return_address",
    "office_id",
    "office_name",
    "office_address",
    "actions",
    "photos",
    "video_paths",
    "videos",
    "attachments",
    "price",
    "currency_code",
    "srid",
    "origin_id_checked_pvz",
    "origin_id_client",
    "origin_id_system",
    "delivery_dt",
    "reason",
    "claim_reason",
    "fin_srid_matched",
    "fin_matched_rows",
    "fin_return_rows",
    "fin_return_sum",
    "fin_compensation_rows",
    "fin_compensation_sum",
    "fin_has_return",
    "fin_has_compensation",
    "fin_operation_names",
    "fin_checked_at",
    "updated_at_export",
)

DB_INTEGER_COLUMNS: tuple[str, ...] = (
    "claim_id",
    "price",
    "claim_type",
    "status",
    "status_ex",
    "nm_id",
    "imt_id",
    "office_id",
)

DB_DATETIME_COLUMNS: tuple[str, ...] = (
    "dt",
    "order_dt",
    "dt_update",
    "delivery_dt",
    "created_at",
    "updated_at",
    "order_created_at",
    "return_deadline",
    "updated_at_export",
)

DB_SCHEMA_DEFINITION = {
    "id": Text,
    "account": Text,
    "is_archive": Boolean,
    "claim_id": BigInteger,
    "dt": DateTime,
    "imt_name": Text,
    "order_dt": DateTime,
    "dt_update": DateTime,
    "claim_type": Integer,
    "claim_type_text": Text,
    "status": Integer,
    "status_text": Text,
    "status_ex": Integer,
    "status_ex_text": Text,
    "nm_id": BigInteger,
    "imt_id": BigInteger,
    "subject_name": Text,
    "brand_name": Text,
    "supplier_article": Text,
    "barcode": Text,
    "size": Text,
    "user_comment": Text,
    "wb_comment": Text,
    "created_at": DateTime,
    "updated_at": DateTime,
    "order_created_at": DateTime,
    "return_deadline": DateTime,
    "return_address": Text,
    "office_id": BigInteger,
    "office_name": Text,
    "office_address": Text,
    "actions": Text,
    "photos": Text,
    "video_paths": Text,
    "videos": Text,
    "attachments": Text,
    "price": Integer,
    "currency_code": Text,
    "srid": Text,
    "origin_id_checked_pvz": Text,
    "origin_id_client": Text,
    "origin_id_system": Text,
    "delivery_dt": DateTime,
    "reason": Text,
    "claim_reason": Text,
    "fin_srid_matched": Boolean,
    "fin_matched_rows": Integer,
    "fin_return_rows": Integer,
    "fin_return_sum": Numeric(14, 2),
    "fin_compensation_rows": Integer,
    "fin_compensation_sum": Numeric(14, 2),
    "fin_has_return": Boolean,
    "fin_has_compensation": Boolean,
    "fin_operation_names": Text,
    "fin_checked_at": DateTime,
    "updated_at_export": DateTime,
}
