from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import aiohttp
import gspread
from gspread import Cell
from gspread.exceptions import APIError
from gspread.utils import a1_range_to_grid_range
from sqlalchemy import text

from polygon.wb_clients.wb_cards import CardsWBAPI
from src_oop.core.database import Database
from src_oop.jobs.add_new_items.config import (
    AUTOPILOT_SKU_HEADER,
    ColumnAlias,
    COMPETITORS_WILD_HEADER,
    INPUT_COLUMNS,
    NEW_ITEM_STATUS_TO_PROCESS,
    SHEETS,
    SOPOST_WILD_HEADER,
    UNIT_MAIN_SKU_HEADER,
    WorksheetConfig,
)
from src_oop.jobs.add_new_items.models import NewItemCard

logger = logging.getLogger(__name__)

_WILD_NUMERIC_PATTERN = re.compile(r"^wild(?P<nm_id>\d+)$", re.IGNORECASE)
_GSHEETS_RETRY_ATTEMPTS = 5
_GSHEETS_RETRY_DELAY_SECONDS = 2


@dataclass(frozen=True, slots=True)
class ProductRecord:
    """Данные для записи новой карточки в таблицу products."""

    wild: str
    name: str
    photo_link: str
    is_kit: bool = False
    share_of_kit: bool = False
    kit_components: None = None


class AddNewItemsRepository:
    """Работа с Google Sheets, WB API и PostgreSQL для job add_new_items."""

    def __init__(self, *, database_cls: type[Database] = Database) -> None:
        self.database_cls = database_cls
        self._creds_path = Path(__file__).resolve().parents[3] / "creds" / "creds.json"
        self._google_client = gspread.service_account(filename=str(self._creds_path))
        self._worksheet_cache: dict[tuple[str, str], gspread.Worksheet] = {}

    def fetch_new_item_cards(self) -> list[NewItemCard]:
        worksheet = self.get_worksheet(SHEETS.new_items)
        headers = self._worksheet_row_values(worksheet, SHEETS.new_items.header_row)
        resolved_headers = self._resolve_input_headers(headers, worksheet.title)
        header_index = self._build_header_index(headers)

        all_rows = self._worksheet_get_all_values(worksheet)
        data_rows = all_rows[SHEETS.new_items.header_row :]
        cards: list[NewItemCard] = []

        for index, row in enumerate(data_rows, start=1):
            status = self._get_row_value(row, header_index, resolved_headers["status"]).strip().lower()
            sku_value = self._get_row_value(row, header_index, resolved_headers["sku"]).strip()
            if status != NEW_ITEM_STATUS_TO_PROCESS or not sku_value:
                continue

            try:
                sku = int(sku_value)
            except ValueError:
                logger.error(
                    "Пропускаем строку с нечисловым sku: row=%s sku=%s",
                    SHEETS.new_items.header_row + index,
                    sku_value,
                )
                continue

            cards.append(
                NewItemCard(
                    row_number=SHEETS.new_items.header_row + index,
                    supplier_name=self._get_row_value(
                        row,
                        header_index,
                        resolved_headers["supplier_name"],
                    ).strip(),
                    sku=sku,
                    client=self._get_row_value(row, header_index, resolved_headers["client"]).strip(),
                    supplier_code_duplicates=self._get_row_value(
                        row,
                        header_index,
                        resolved_headers["supplier_code_duplicates"],
                    ).strip(),
                    status=status,
                    item_name=self._get_row_value(row, header_index, resolved_headers["item_name"]).strip(),
                    category=self._get_row_value(row, header_index, resolved_headers["category"]).strip(),
                    supplier_code_unique=self._get_row_value(
                        row,
                        header_index,
                        resolved_headers["supplier_code_unique"],
                    ).strip(),
                    purchase_price=self._get_row_value(
                        row,
                        header_index,
                        resolved_headers["purchase_price"],
                    ).strip(),
                    manager=self._get_row_value(row, header_index, resolved_headers["manager"]).strip(),
                )
            )

        logger.info("Загружены строки из 'Для юнит': %s", len(cards))
        return cards

    def fetch_existing_wilds_in_sopost(self) -> set[str]:
        return self._fetch_existing_string_values(SHEETS.sopost, SOPOST_WILD_HEADER)

    def fetch_existing_skus_in_unit_main(self) -> set[int]:
        return self._fetch_existing_int_values(SHEETS.unit_main, UNIT_MAIN_SKU_HEADER)

    def fetch_existing_skus_in_autopilot(self) -> set[int]:
        return self._fetch_existing_int_values(SHEETS.autopilot, AUTOPILOT_SKU_HEADER)

    def fetch_existing_wilds_in_competitors(self) -> set[str]:
        return self._fetch_existing_string_values(SHEETS.competitors, COMPETITORS_WILD_HEADER)

    def fetch_existing_product_wilds(self) -> set[str]:
        query = "SELECT DISTINCT p.id FROM products AS p"
        rows = self.database_cls.read_sql_to_dict(query)
        wilds = {str(row["id"]).strip() for row in rows if row.get("id")}
        logger.info("Загружены wild из products: %s", len(wilds))
        return wilds

    def append_rows_by_headers(
        self,
        sheet_config: WorksheetConfig,
        rows_data: Sequence[Sequence[object]],
        target_headers: Sequence[str],
    ) -> int:
        worksheet = self.get_worksheet(sheet_config)
        indexes = self._resolve_header_indexes(
            worksheet=worksheet,
            header_row=sheet_config.header_row,
            target_headers=target_headers,
        )
        self._append_rows_with_template(
            worksheet=worksheet,
            rows_data=rows_data,
            target_indexes=indexes,
            header_row=sheet_config.header_row,
        )
        return len(rows_data)

    def append_rows_by_indexes(
        self,
        sheet_config: WorksheetConfig,
        rows_data: Sequence[Sequence[object]],
        target_indexes: Sequence[int],
    ) -> int:
        worksheet = self.get_worksheet(sheet_config)
        self._append_rows_with_template(
            worksheet=worksheet,
            rows_data=rows_data,
            target_indexes=target_indexes,
            header_row=sheet_config.header_row,
        )
        return len(rows_data)

    async def build_missing_products(self, cards: Sequence[NewItemCard]) -> list[ProductRecord]:
        if not cards:
            return []

        logger.info("Получаем данные карточек WB для записи в products: %s", len(cards))
        async with aiohttp.ClientSession() as session:
            clients: dict[str, CardsWBAPI] = {}
            results: list[ProductRecord] = []
            for card in cards:
                client_name = card.client.strip().capitalize()
                wb_client = clients.get(client_name)
                if wb_client is None:
                    wb_client = CardsWBAPI(session=session, account_name=client_name)
                    clients[client_name] = wb_client

                try:
                    nm_id = self._extract_nm_id(card.wild)
                    wb_card = await wb_client.get_card(nm_id=nm_id)
                    if wb_card is None:
                        logger.error(
                            "Не найдена карточка WB для products: row=%s wild=%s client=%s",
                            card.row_number,
                            card.wild,
                            client_name,
                        )
                        continue

                    photo_link = ""
                    if wb_card.photos:
                        first_photo = wb_card.photos[0]
                        photo_link = (
                            first_photo.get("tm")
                            or first_photo.get("big")
                            or first_photo.get("c246x328")
                            or ""
                        )

                    results.append(
                        ProductRecord(
                            wild=card.wild,
                            name=card.item_name,
                            photo_link=photo_link,
                        )
                    )
                except Exception:
                    logger.exception(
                        "Ошибка при подготовке products для строки: row=%s wild=%s client=%s",
                        card.row_number,
                        card.wild,
                        client_name,
                    )
                    continue

            return results

    def update_input_status_flags(
        self,
        *,
        status_by_row: dict[int, dict[str, str]],
    ) -> None:
        if not status_by_row:
            return

        worksheet = self.get_worksheet(SHEETS.new_items)
        headers = self._worksheet_row_values(worksheet, SHEETS.new_items.header_row)
        resolved_headers = self._resolve_status_headers(headers, worksheet.title)
        header_index = self._build_header_index(headers)
        status_columns = {
            "unit_main": header_index[resolved_headers["added_to_unit_main"]] + 1,
            "autopilot": header_index[resolved_headers["added_to_autopilot"]] + 1,
            "products": header_index[resolved_headers["added_to_products"]] + 1,
        }
        cells_to_update: list[Cell] = []

        for row_number in sorted(status_by_row):
            row_status = status_by_row[row_number]
            for status_key, column_index in status_columns.items():
                cells_to_update.append(
                    Cell(
                        row=row_number,
                        col=column_index,
                        value=row_status[status_key],
                    )
                )

        self._worksheet_update_cells(
            worksheet,
            cells_to_update,
            value_input_option="USER_ENTERED",
        )
        logger.info("Обновлены статусы добавления во вкладке 'Для юнит': %s строк", len(status_by_row))

    def upsert_products(self, product_records: Sequence[ProductRecord]) -> int:
        if not product_records:
            return 0

        statement = text(
            """
            INSERT INTO products (
                id,
                name,
                is_kit,
                share_of_kit,
                photo_link,
                kit_components
            )
            VALUES (
                :id,
                :name,
                :is_kit,
                :share_of_kit,
                :photo_link,
                :kit_components
            )
            ON CONFLICT (id) DO NOTHING
            """
        )
        payload = [
            {
                "id": record.wild,
                "name": record.name,
                "is_kit": record.is_kit,
                "share_of_kit": record.share_of_kit,
                "photo_link": record.photo_link,
                "kit_components": record.kit_components,
            }
            for record in product_records
        ]

        inserted = 0
        with self.database_cls.get_engine().begin() as connection:
            for item in payload:
                result = connection.execute(statement, item)
                inserted += result.rowcount or 0

        logger.info("Записаны новые строки в products: %s", inserted)
        return inserted

    def get_worksheet(self, sheet_config: WorksheetConfig) -> gspread.Worksheet:
        cache_key = (sheet_config.table_title, sheet_config.sheet_title)
        worksheet = self._worksheet_cache.get(cache_key)
        if worksheet is not None:
            return worksheet

        table = self._execute_gspread_call(
            action_name=f"open spreadsheet '{sheet_config.table_title}'",
            func=lambda: self._google_client.open(sheet_config.table_title),
        )
        worksheet = self._execute_gspread_call(
            action_name=(
                f"open worksheet '{sheet_config.sheet_title}' "
                f"in spreadsheet '{sheet_config.table_title}'"
            ),
            func=lambda: table.worksheet(sheet_config.sheet_title),
        )
        self._worksheet_cache[cache_key] = worksheet
        return worksheet

    @staticmethod
    def _build_header_index(headers: Sequence[str]) -> dict[str, int]:
        return {header: index for index, header in enumerate(headers)}

    @staticmethod
    def _get_row_value(row: Sequence[str], header_index: dict[str, int], header_name: str) -> str:
        index = header_index[header_name]
        return row[index] if index < len(row) else ""

    @staticmethod
    def _validate_headers(
        headers: Sequence[str],
        required_headers: Sequence[str],
        worksheet_title: str,
    ) -> None:
        missing_headers = [header for header in required_headers if header not in headers]
        if missing_headers:
            raise ValueError(
                f"Во вкладке '{worksheet_title}' отсутствуют обязательные колонки: "
                f"{', '.join(missing_headers)}"
            )

    def _resolve_input_headers(
        self,
        headers: Sequence[str],
        worksheet_title: str,
    ) -> dict[str, str]:
        return {
            "supplier_name": self._resolve_optional_header_alias(headers, INPUT_COLUMNS.supplier_name),
            "sku": self._resolve_header_alias(headers, INPUT_COLUMNS.sku, worksheet_title),
            "client": self._resolve_header_alias(headers, INPUT_COLUMNS.client, worksheet_title),
            "supplier_code_duplicates": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.supplier_code_duplicates,
                worksheet_title,
            ),
            "status": self._resolve_header_alias(headers, INPUT_COLUMNS.status, worksheet_title),
            "item_name": self._resolve_header_alias(headers, INPUT_COLUMNS.item_name, worksheet_title),
            "category": self._resolve_header_alias(headers, INPUT_COLUMNS.category, worksheet_title),
            "supplier_code_unique": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.supplier_code_unique,
                worksheet_title,
            ),
            "purchase_price": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.purchase_price,
                worksheet_title,
            ),
            "manager": self._resolve_header_alias(headers, INPUT_COLUMNS.manager, worksheet_title),
            "added_to_unit_main": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.added_to_unit_main,
                worksheet_title,
            ),
            "added_to_autopilot": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.added_to_autopilot,
                worksheet_title,
            ),
            "added_to_products": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.added_to_products,
                worksheet_title,
            ),
        }

    def _resolve_status_headers(
        self,
        headers: Sequence[str],
        worksheet_title: str,
    ) -> dict[str, str]:
        return {
            "added_to_unit_main": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.added_to_unit_main,
                worksheet_title,
            ),
            "added_to_autopilot": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.added_to_autopilot,
                worksheet_title,
            ),
            "added_to_products": self._resolve_header_alias(
                headers,
                INPUT_COLUMNS.added_to_products,
                worksheet_title,
            ),
        }

    @staticmethod
    def _resolve_header_alias(
        headers: Sequence[str],
        header_alias: ColumnAlias,
        worksheet_title: str,
    ) -> str:
        candidates = (header_alias,) if isinstance(header_alias, str) else header_alias
        for candidate in candidates:
            if candidate in headers:
                return candidate

        expected = ", ".join(candidates)
        raise ValueError(
            f"Во вкладке '{worksheet_title}' отсутствует обязательная колонка. "
            f"Ожидался один из вариантов: {expected}"
        )

    @staticmethod
    def _resolve_optional_header_alias(
        headers: Sequence[str],
        header_alias: ColumnAlias,
    ) -> str:
        candidates = (header_alias,) if isinstance(header_alias, str) else header_alias
        for candidate in candidates:
            if candidate in headers:
                return candidate
        return ""

    def _fetch_existing_string_values(
        self,
        sheet_config: WorksheetConfig,
        column_name: str,
    ) -> set[str]:
        worksheet = self.get_worksheet(sheet_config)
        column_values = self._get_column_values(
            worksheet=worksheet,
            header_row=sheet_config.header_row,
            column_name=column_name,
        )
        values = {value.strip() for value in column_values if value and value.strip()}
        logger.info(
            "Загружены значения из %s -> %s: %s",
            sheet_config.table_title,
            sheet_config.sheet_title,
            len(values),
        )
        return values

    def _fetch_existing_int_values(
        self,
        sheet_config: WorksheetConfig,
        column_name: str,
    ) -> set[int]:
        worksheet = self.get_worksheet(sheet_config)
        column_values = self._get_column_values(
            worksheet=worksheet,
            header_row=sheet_config.header_row,
            column_name=column_name,
        )
        values = {
            int(value.strip())
            for value in column_values
            if value and value.strip().isdigit()
        }
        logger.info(
            "Загружены числовые значения из %s -> %s: %s",
            sheet_config.table_title,
            sheet_config.sheet_title,
            len(values),
        )
        return values

    def _get_column_values(
        self,
        *,
        worksheet: gspread.Worksheet,
        header_row: int,
        column_name: str,
    ) -> list[str]:
        headers = self._worksheet_row_values(worksheet, header_row)
        if column_name not in headers:
            raise ValueError(
                f"Во вкладке '{worksheet.title}' не найдена колонка '{column_name}'."
            )

        column_index = headers.index(column_name) + 1
        raw_values = self._worksheet_col_values(worksheet, column_index)
        data_start_index = header_row
        return raw_values[data_start_index:]

    def _resolve_header_indexes(
        self,
        *,
        worksheet: gspread.Worksheet,
        header_row: int,
        target_headers: Sequence[str],
    ) -> list[int]:
        headers = self._worksheet_row_values(worksheet, header_row)
        self._validate_headers(headers, target_headers, worksheet.title)
        return [headers.index(header_name) for header_name in target_headers]

    def _append_rows_with_template(
        self,
        worksheet: gspread.Worksheet,
        rows_data: Sequence[Sequence[object]],
        target_indexes: Sequence[int],
        header_row: int,
    ) -> None:
        if not rows_data:
            return

        last_data_row = self._find_last_data_row(worksheet)
        prototype = self._load_last_row_with_formula_placeholders(
            worksheet=worksheet,
            row_number=last_data_row,
            header_row=header_row,
        )
        rows_to_append: list[list[object]] = []
        first_new_row = last_data_row + 1

        for offset, row_data in enumerate(rows_data):
            row_number = first_new_row + offset
            row = prototype.copy()
            for index, value in zip(target_indexes, row_data):
                row[index] = value

            rows_to_append.append(
                [
                    cell.replace("{cell_num}", str(row_number))
                    if isinstance(cell, str) and "{cell_num}" in cell
                    else cell
                    for cell in row
                ]
            )

        self._worksheet_append_rows(
            worksheet,
            rows_to_append,
            value_input_option="USER_ENTERED",
        )
        self._copy_last_row_format(
            worksheet=worksheet,
            source_row=last_data_row,
            destination_start_row=first_new_row,
            destination_end_row=first_new_row + len(rows_to_append) - 1,
        )

    def _find_last_data_row(self, worksheet: gspread.Worksheet) -> int:
        all_values = self._worksheet_get_all_values(worksheet)
        if not all_values:
            raise ValueError(f"Во вкладке '{worksheet.title}' нет строк, от которых можно тянуть формулы.")
        return len(all_values)

    def _load_last_row_with_formula_placeholders(
        self,
        worksheet: gspread.Worksheet,
        row_number: int,
        header_row: int,
    ) -> list[object]:
        headers = self._worksheet_row_values(worksheet, header_row)
        last_row = self._execute_gspread_call(
            action_name=f"read formula row {row_number} from '{worksheet.title}'",
            func=lambda: worksheet.row_values(row_number, value_render_option="FORMULA"),
        )
        columns_count = max(len(headers), len(last_row))
        formulas = self._extract_formulas_with_placeholders(last_row, row_number)
        row_prototype: list[object] = [""] * columns_count

        for index, formula in formulas:
            if index < columns_count:
                row_prototype[index] = formula

        return row_prototype

    @staticmethod
    def _extract_formulas_with_placeholders(
        row_values: Sequence[object],
        row_number: int,
    ) -> list[tuple[int, str]]:
        result: list[tuple[int, str]] = []
        for index, cell in enumerate(row_values):
            if isinstance(cell, str) and cell.startswith("="):
                result.append((index, cell.replace(str(row_number), "{cell_num}")))
        return result

    def _copy_last_row_format(
        self,
        *,
        worksheet: gspread.Worksheet,
        source_row: int,
        destination_start_row: int,
        destination_end_row: int,
    ) -> None:
        if destination_end_row < destination_start_row:
            return

        spreadsheet = worksheet.spreadsheet
        source_range = a1_range_to_grid_range(f"{source_row}:{source_row}", worksheet.id)
        destination_range = a1_range_to_grid_range(
            f"{destination_start_row}:{destination_end_row}",
            worksheet.id,
        )
        self._execute_gspread_call(
            action_name=(
                f"copy format in '{worksheet.spreadsheet.title}' -> "
                f"'{worksheet.title}'"
            ),
            func=lambda: spreadsheet.batch_update(
                {
                    "requests": [
                        {
                            "copyPaste": {
                                "source": source_range,
                                "destination": destination_range,
                                "pasteType": "PASTE_FORMAT",
                                "pasteOrientation": "NORMAL",
                            }
                        }
                    ]
                }
            ),
        )

    @staticmethod
    def _extract_nm_id(wild: str) -> int:
        match = _WILD_NUMERIC_PATTERN.match(wild.strip())
        if match is None:
            raise ValueError(
                f"Не удалось извлечь nm_id из wild '{wild}'. Ожидался формат 'wild123456'."
            )
        return int(match.group("nm_id"))

    def _worksheet_row_values(
        self,
        worksheet: gspread.Worksheet,
        row_number: int,
    ) -> list[str]:
        return self._execute_gspread_call(
            action_name=(
                f"read row {row_number} from '{worksheet.spreadsheet.title}' -> "
                f"'{worksheet.title}'"
            ),
            func=lambda: worksheet.row_values(row_number),
        )

    def _worksheet_get_all_values(
        self,
        worksheet: gspread.Worksheet,
    ) -> list[list[str]]:
        return self._execute_gspread_call(
            action_name=(
                f"read all values from '{worksheet.spreadsheet.title}' -> "
                f"'{worksheet.title}'"
            ),
            func=worksheet.get_all_values,
        )

    def _worksheet_col_values(
        self,
        worksheet: gspread.Worksheet,
        column_index: int,
    ) -> list[str]:
        return self._execute_gspread_call(
            action_name=(
                f"read column {column_index} from '{worksheet.spreadsheet.title}' -> "
                f"'{worksheet.title}'"
            ),
            func=lambda: worksheet.col_values(column_index),
        )

    def _worksheet_append_rows(
        self,
        worksheet: gspread.Worksheet,
        rows_to_append: Sequence[Sequence[object]],
        *,
        value_input_option: str,
    ) -> None:
        self._execute_gspread_call(
            action_name=(
                f"append rows to '{worksheet.spreadsheet.title}' -> "
                f"'{worksheet.title}'"
            ),
            func=lambda: worksheet.append_rows(
                rows_to_append,
                value_input_option=value_input_option,
            ),
        )

    def _worksheet_update_cells(
        self,
        worksheet: gspread.Worksheet,
        cells_to_update: Sequence[Cell],
        *,
        value_input_option: str,
    ) -> None:
        self._execute_gspread_call(
            action_name=(
                f"update cells in '{worksheet.spreadsheet.title}' -> "
                f"'{worksheet.title}'"
            ),
            func=lambda: worksheet.update_cells(
                cells_to_update,
                value_input_option=value_input_option,
            ),
        )

    def _execute_gspread_call(
        self,
        *,
        action_name: str,
        func,
    ):
        last_error: APIError | None = None
        for attempt in range(1, _GSHEETS_RETRY_ATTEMPTS + 1):
            try:
                return func()
            except APIError as error:
                if "[503]" not in str(error):
                    raise

                last_error = error
                if attempt == _GSHEETS_RETRY_ATTEMPTS:
                    break

                logger.warning(
                    "Google Sheets временно недоступен, повторяем попытку: action=%s attempt=%s/%s",
                    action_name,
                    attempt,
                    _GSHEETS_RETRY_ATTEMPTS,
                )
                time.sleep(_GSHEETS_RETRY_DELAY_SECONDS * attempt)

        if last_error is not None:
            logger.error(
                "Google Sheets остался недоступен после повторов: action=%s attempts=%s",
                action_name,
                _GSHEETS_RETRY_ATTEMPTS,
            )
            raise last_error
