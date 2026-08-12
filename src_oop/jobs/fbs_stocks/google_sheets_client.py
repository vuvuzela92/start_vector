from __future__ import annotations

import logging
from dataclasses import dataclass

import gspread
from gspread.utils import rowcol_to_a1

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.fbs_stocks.config import (
    ACCOUNT_COLUMN,
    ARTICLE_COLUMN,
    DATA_START_ROW,
    HEADER_ROW_INDEX,
    INSERT_AFTER_COLUMN,
    STOCK_MANAGEMENT_COLUMNS,
    TARGET_WAREHOUSES,
    UNIT_SHEET_TITLE,
    UNIT_TABLE_TITLE,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class UnitStocksRow:
    """Строка UNIT, по которой нужно обновить текущий FBS-остаток."""

    row_number: int
    article_id: int
    account: str


@dataclass(slots=True)
class UnitNewStockRow:
    """Строка UNIT с явно заданным новым остатком для отправки в WB."""

    row_number: int
    article_id: int
    account: str
    warehouse_id: int
    warehouse_alias: str
    amount: int


class FBSStocksGoogleSheetsClient:
    """Читает строки UNIT и записывает FBS-остатки в заданные колонки складов."""

    def __init__(
        self,
        table_title: str = UNIT_TABLE_TITLE,
        sheet_title: str = UNIT_SHEET_TITLE,
    ) -> None:
        """Подключается к тестовой UNIT-таблице для сценария управления остатками."""
        if table_title != UNIT_TABLE_TITLE:
            raise ValueError(
                "FBS-остатки разрешено обновлять только в тестовой таблице "
                f"'{UNIT_TABLE_TITLE}', получена таблица '{table_title}'."
            )
        self.connector = GoogleTabs(table_title, sheet_title)
        self.worksheet: gspread.Worksheet = self.connector.sheet_title
        if self.worksheet.spreadsheet.title != UNIT_TABLE_TITLE:
            raise ValueError(
                "FBS-остатки разрешено обновлять только в тестовой таблице "
                f"'{UNIT_TABLE_TITLE}', подключена таблица '{self.worksheet.spreadsheet.title}'."
            )

    def read_unit_rows(self) -> tuple[list[UnitStocksRow], list[str]]:
        """Читает `Артикул` и `ЛК`, сохраняя номера строк для точечной записи FBS-остатков."""
        headers = self.worksheet.row_values(HEADER_ROW_INDEX)
        self._validate_headers(headers, (ARTICLE_COLUMN, ACCOUNT_COLUMN))

        article_column_index = headers.index(ARTICLE_COLUMN) + 1
        account_column_index = headers.index(ACCOUNT_COLUMN) + 1
        article_values = self.worksheet.col_values(article_column_index)[DATA_START_ROW - 1 :]
        account_values = self.worksheet.col_values(account_column_index)[DATA_START_ROW - 1 :]
        max_rows = max(len(article_values), len(account_values))

        rows: list[UnitStocksRow] = []
        for offset in range(max_rows):
            article_value = article_values[offset] if offset < len(article_values) else ""
            account_value = account_values[offset] if offset < len(account_values) else ""
            article_id = self._coerce_article_id(article_value)
            if article_id is None or not account_value.strip():
                continue
            rows.append(
                UnitStocksRow(
                    row_number=DATA_START_ROW + offset,
                    article_id=article_id,
                    account=account_value.strip(),
                )
            )

        logger.info(
            "Строки UNIT для обновления FBS-остатков прочитаны | sheet=%s | rows=%s",
            self.worksheet.title,
            len(rows),
        )
        return rows, headers

    def read_new_stock_rows(self) -> list[UnitNewStockRow]:
        """Читает заполненные `Новый остаток ...`, чтобы подготовить точечную отправку остатков в WB."""
        unit_rows, headers = self.read_unit_rows()
        self._validate_headers(
            headers,
            tuple(target.new_stock_column for target in TARGET_WAREHOUSES),
        )

        new_values_by_warehouse: dict[int, list[str]] = {}
        for target_warehouse in TARGET_WAREHOUSES:
            column_index = headers.index(target_warehouse.new_stock_column) + 1
            new_values_by_warehouse[target_warehouse.warehouse_id] = self.worksheet.col_values(
                column_index
            )[DATA_START_ROW - 1 :]

        new_stock_rows: list[UnitNewStockRow] = []
        for unit_row in unit_rows:
            row_offset = unit_row.row_number - DATA_START_ROW
            for target_warehouse in TARGET_WAREHOUSES:
                column_values = new_values_by_warehouse[target_warehouse.warehouse_id]
                raw_value = column_values[row_offset] if row_offset < len(column_values) else ""
                amount = self._coerce_new_stock_amount(
                    value=raw_value,
                    row_number=unit_row.row_number,
                    column_name=target_warehouse.new_stock_column,
                )
                if amount is None:
                    continue
                new_stock_rows.append(
                    UnitNewStockRow(
                        row_number=unit_row.row_number,
                        article_id=unit_row.article_id,
                        account=unit_row.account,
                        warehouse_id=target_warehouse.warehouse_id,
                        warehouse_alias=target_warehouse.warehouse_alias,
                        amount=amount,
                    )
                )

        logger.info(
            "Новые FBS-остатки прочитаны из UNIT | sheet=%s | rows=%s",
            self.worksheet.title,
            len(new_stock_rows),
        )
        return new_stock_rows

    def ensure_stock_management_columns(self) -> list[str]:
        """Создает недостающие складские колонки UNIT для управления FBS-остатками.

        Бизнес-правило: блок складов должен быть единым и идти после старой
        колонки `Новый остаток`, чтобы текущий FBS-контур не смешивался с
        будущими складовыми остатками Вешки/Казань.
        """
        headers = self.worksheet.row_values(HEADER_ROW_INDEX)
        missing_columns = [
            column_name
            for column_name in STOCK_MANAGEMENT_COLUMNS
            if column_name not in headers
        ]
        if not missing_columns:
            return headers

        if INSERT_AFTER_COLUMN not in headers:
            raise ValueError(
                f"В MAIN (tested) нет колонки '{INSERT_AFTER_COLUMN}', после которой нужно добавить блок FBS-складов."
            )

        insert_after_index = headers.index(INSERT_AFTER_COLUMN) + 1
        insert_at_index = insert_after_index + 1
        self.worksheet.insert_cols(
            [[column_name] for column_name in missing_columns],
            col=insert_at_index,
            value_input_option="USER_ENTERED",
        )
        logger.info(
            "В UNIT добавлены недостающие колонки управления FBS-остатками | sheet=%s | columns=%s | insert_after=%s",
            self.worksheet.title,
            missing_columns,
            INSERT_AFTER_COLUMN,
        )
        return self.worksheet.row_values(HEADER_ROW_INDEX)

    def write_stock_columns(
        self,
        headers: list[str],
        values_by_column: dict[str, list[list[int | str]]],
    ) -> None:
        """Записывает FBS-остатки только в целевые колонки складов, не трогая остальные данные UNIT."""
        self._validate_headers(headers, tuple(values_by_column.keys()))

        for column_name, values in values_by_column.items():
            column_index = headers.index(column_name) + 1
            start_cell = rowcol_to_a1(DATA_START_ROW, column_index)
            end_cell = rowcol_to_a1(DATA_START_ROW + len(values) - 1, column_index)
            range_label = f"{start_cell}:{end_cell}"
            self.worksheet.update(
                range_label,
                values,
                value_input_option="USER_ENTERED",
            )
            logger.info(
                "FBS-остатки записаны в UNIT | sheet=%s | column=%s | range=%s | rows=%s",
                self.worksheet.title,
                column_name,
                range_label,
                len(values),
            )

    def _validate_headers(self, headers: list[str], required_columns: tuple[str, ...]) -> None:
        """Проверяет наличие колонок UNIT, чтобы не записать остатки в неверный диапазон."""
        missing_columns = [column for column in required_columns if column not in headers]
        if missing_columns:
            raise ValueError(
                "В MAIN (tested) отсутствуют обязательные колонки для FBS-остатков: "
                f"{missing_columns}"
            )

    def _coerce_article_id(self, value: str) -> int | None:
        """Приводит артикул UNIT к int, пропуская пустые и служебные строки."""
        prepared_value = str(value).strip().replace(" ", "")
        if not prepared_value.isdigit():
            return None
        return int(prepared_value)

    def _coerce_new_stock_amount(
        self,
        value: str,
        row_number: int,
        column_name: str,
    ) -> int | None:
        """Приводит новый остаток UNIT к неотрицательному int для безопасной отправки в WB."""
        prepared_value = str(value).strip().replace(" ", "").replace(",", ".")
        if prepared_value == "":
            return None
        try:
            amount = float(prepared_value)
        except ValueError as error:
            raise ValueError(
                f"Некорректный новый остаток в UNIT: row={row_number} column={column_name} value={value!r}"
            ) from error
        if amount < 0 or not amount.is_integer():
            raise ValueError(
                f"Новый остаток должен быть целым неотрицательным числом: row={row_number} column={column_name} value={value!r}"
            )
        return int(amount)
