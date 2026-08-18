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
    MIN_STOCK_COLUMN,
    NEW_STOCK_ALL_WAREHOUSES_COLUMN,
    NEW_STOCK_VESHKI_COLUMN,
    SOPOST_ADD_COLUMN,
    SOPOST_SHEET_TITLE,
    STOCK_MANAGEMENT_COLUMNS,
    TARGET_WAREHOUSES,
    TOTAL_STOCK_COLUMN,
    UNIT_SHEET_TITLE,
    UNIT_TABLE_TITLE,
    VESHKI_WAREHOUSE_ID,
    WILD_COLUMN,
)

logger = logging.getLogger(__name__)
NBSP_CHARACTER = "\u00a0"


@dataclass(slots=True)
class UnitStocksRow:
    """Строка UNIT, по которой нужно обновить текущий FBS-остаток."""

    row_number: int
    article_id: int
    account: str


@dataclass(slots=True)
class UnitNewStockRow:
    """Команда UNIT на изменение остатка конкретного внутреннего склада WB."""

    row_number: int
    article_id: int
    account: str
    warehouse_id: int
    warehouse_alias: str
    amount: int
    source_column: str


@dataclass(slots=True)
class UnitAutoRefillRow:
    """Строка UNIT для проверки необходимости автопополнения FBS-остатков.

    Бизнес-сценарий: cron смотрит минимальный остаток на один внутренний склад, текущий общий
    FBS-остаток и `wild`, чтобы найти величину пополнения во вкладке `Сопост`.
    """

    row_number: int
    article_id: int
    account: str
    wild: str
    minimum_stock: int
    sheet_total_stock: int


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
        self._validate_headers(headers, (ACCOUNT_COLUMN,))

        article_column_index = self._resolve_article_column_index(headers)
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

    def read_auto_refill_rows(self) -> list[UnitAutoRefillRow]:
        """Читает строки MAIN (tested), по которым cron проверяет автопополнение остатков.

        Бизнес-правила: строка участвует в проверке только если заполнены `Артикул`, `ЛК`, `wild`
        и положительный `Минимальный остаток`. `ФБС общий остаток` читается для диагностики, а
        фактическое решение сервис дополнительно сверяет с текущими остатками WB.
        """
        headers = self.worksheet.row_values(HEADER_ROW_INDEX)
        self._validate_headers(
            headers,
            (ACCOUNT_COLUMN, WILD_COLUMN, MIN_STOCK_COLUMN, TOTAL_STOCK_COLUMN),
        )

        article_column_index = self._resolve_article_column_index(headers)
        account_column_index = headers.index(ACCOUNT_COLUMN) + 1
        wild_column_index = headers.index(WILD_COLUMN) + 1
        minimum_stock_column_index = headers.index(MIN_STOCK_COLUMN) + 1
        total_stock_column_index = headers.index(TOTAL_STOCK_COLUMN) + 1

        article_values = self.worksheet.col_values(article_column_index)[DATA_START_ROW - 1 :]
        account_values = self.worksheet.col_values(account_column_index)[DATA_START_ROW - 1 :]
        wild_values = self.worksheet.col_values(wild_column_index)[DATA_START_ROW - 1 :]
        minimum_values = self.worksheet.col_values(minimum_stock_column_index)[DATA_START_ROW - 1 :]
        total_values = self.worksheet.col_values(total_stock_column_index)[DATA_START_ROW - 1 :]
        max_rows = max(
            len(article_values),
            len(account_values),
            len(wild_values),
            len(minimum_values),
            len(total_values),
        )

        rows: list[UnitAutoRefillRow] = []
        for offset in range(max_rows):
            article_value = article_values[offset] if offset < len(article_values) else ""
            account_value = account_values[offset] if offset < len(account_values) else ""
            wild_value = wild_values[offset] if offset < len(wild_values) else ""
            minimum_value = minimum_values[offset] if offset < len(minimum_values) else ""
            total_value = total_values[offset] if offset < len(total_values) else ""

            article_id = self._coerce_article_id(article_value)
            minimum_stock = self._coerce_optional_non_negative_int(minimum_value)
            if (
                article_id is None
                or not account_value.strip()
                or not wild_value.strip()
                or minimum_stock is None
                or minimum_stock <= 0
            ):
                continue

            rows.append(
                UnitAutoRefillRow(
                    row_number=DATA_START_ROW + offset,
                    article_id=article_id,
                    account=account_value.strip(),
                    wild=wild_value.strip(),
                    minimum_stock=minimum_stock,
                    sheet_total_stock=self._coerce_optional_non_negative_int(total_value) or 0,
                )
            )

        logger.info(
            "Строки UNIT для автопополнения FBS-остатков прочитаны | sheet=%s | rows=%s",
            self.worksheet.title,
            len(rows),
        )
        return rows

    def read_sopost_add_amounts_by_wild(self) -> dict[str, int]:
        """Читает из `Сопост` значение `Добавляем` по каждому `wild`.

        Бизнес-правило: автопополнение не придумывает величину пополнения само, а использует
        управляемое значение из строки `Сопост`, чтобы логика cron совпадала с ручным планированием.
        """
        if self.connector.table is None:
            raise RuntimeError("Google Sheets подключение не содержит объект таблицы для чтения Сопост.")

        worksheet = self.connector.table.worksheet(SOPOST_SHEET_TITLE)
        headers = worksheet.row_values(HEADER_ROW_INDEX)
        self._validate_headers(headers, (WILD_COLUMN, SOPOST_ADD_COLUMN))
        wild_column_index = headers.index(WILD_COLUMN) + 1
        add_column_index = headers.index(SOPOST_ADD_COLUMN) + 1
        wild_values = worksheet.col_values(wild_column_index)[DATA_START_ROW - 1 :]
        add_values = worksheet.col_values(add_column_index)[DATA_START_ROW - 1 :]

        add_amounts_by_wild: dict[str, int] = {}
        for offset, wild_value in enumerate(wild_values):
            prepared_wild = wild_value.strip().casefold()
            if not prepared_wild:
                continue
            raw_add_value = add_values[offset] if offset < len(add_values) else ""
            add_amount = self._coerce_optional_non_negative_int(raw_add_value)
            if add_amount is None or add_amount <= 0:
                continue
            add_amounts_by_wild[prepared_wild] = add_amount

        logger.info(
            "Значения автопополнения FBS-остатков прочитаны из Сопост | sheet=%s | wilds=%s",
            worksheet.title,
            len(add_amounts_by_wild),
        )
        return add_amounts_by_wild

    def _resolve_article_column_index(self, headers: list[str]) -> int:
        """Находит колонку артикула UNIT, защищая сценарий от пустого заголовка в первом столбце."""
        if ARTICLE_COLUMN in headers:
            return headers.index(ARTICLE_COLUMN) + 1
        logger.warning(
            "В MAIN (tested) не найден заголовок '%s', используется первая колонка как колонка артикула.",
            ARTICLE_COLUMN,
        )
        return 1

    def has_pending_new_stock_commands(self) -> bool:
        """Проверяет, есть ли в UNIT хотя бы одна ручная команда на изменение FBS-остатка.

        Бизнес-сценарий: cron может запускать `apply_new_fbs_stocks_from_unit` регулярно. Если
        пользователь не заполнил `Новый остаток для всех складов` или `Новый остаток Вешки`,
        сценарий не должен готовить пустую отправку в WB и может сразу перейти к актуализации
        `ФБС общий остаток`.
        """
        headers = self.worksheet.row_values(HEADER_ROW_INDEX)
        self._validate_headers(
            headers,
            (NEW_STOCK_ALL_WAREHOUSES_COLUMN, NEW_STOCK_VESHKI_COLUMN),
        )

        all_column_index = headers.index(NEW_STOCK_ALL_WAREHOUSES_COLUMN) + 1
        veshki_column_index = headers.index(NEW_STOCK_VESHKI_COLUMN) + 1
        all_values = self.worksheet.col_values(all_column_index)[DATA_START_ROW - 1 :]
        veshki_values = self.worksheet.col_values(veshki_column_index)[DATA_START_ROW - 1 :]

        for raw_value in [*all_values, *veshki_values]:
            if self._normalize_numeric_text(raw_value) != "":
                logger.info(
                    "В UNIT найдены ручные команды новых FBS-остатков | sheet=%s",
                    self.worksheet.title,
                )
                return True

        logger.info(
            "В UNIT не найдены ручные команды новых FBS-остатков | sheet=%s",
            self.worksheet.title,
        )
        return False

    def read_new_stock_rows(self) -> list[UnitNewStockRow]:
        """Читает управляющие колонки новых остатков и разворачивает их в команды по складам.

        Бизнес-правила: `Новый остаток для всех складов` задает одинаковый остаток на каждом
        внутреннем складе. `Новый остаток Вешки` задает остаток на целевом складе Вешки, а остальные
        внутренние склады обнуляет. Одновременно заполнять оба поля в одной строке нельзя, чтобы
        пользовательская команда не была двусмысленной.
        """
        unit_rows, headers = self.read_unit_rows()
        self._validate_headers(
            headers,
            (NEW_STOCK_ALL_WAREHOUSES_COLUMN, NEW_STOCK_VESHKI_COLUMN),
        )

        all_column_index = headers.index(NEW_STOCK_ALL_WAREHOUSES_COLUMN) + 1
        veshki_column_index = headers.index(NEW_STOCK_VESHKI_COLUMN) + 1
        all_values = self.worksheet.col_values(all_column_index)[DATA_START_ROW - 1 :]
        veshki_values = self.worksheet.col_values(veshki_column_index)[DATA_START_ROW - 1 :]

        new_stock_rows: list[UnitNewStockRow] = []
        for unit_row in unit_rows:
            row_offset = unit_row.row_number - DATA_START_ROW
            raw_all_value = all_values[row_offset] if row_offset < len(all_values) else ""
            raw_veshki_value = veshki_values[row_offset] if row_offset < len(veshki_values) else ""
            all_amount = self._coerce_new_stock_amount(
                value=raw_all_value,
                row_number=unit_row.row_number,
                column_name=NEW_STOCK_ALL_WAREHOUSES_COLUMN,
            )
            veshki_amount = self._coerce_new_stock_amount(
                value=raw_veshki_value,
                row_number=unit_row.row_number,
                column_name=NEW_STOCK_VESHKI_COLUMN,
            )
            if all_amount is not None and veshki_amount is not None:
                logger.warning(
                    "В UNIT одновременно заполнены два сценария нового FBS-остатка: применяем только Вешки | row=%s | ignored_column=%s | priority_column=%s",
                    unit_row.row_number,
                    NEW_STOCK_ALL_WAREHOUSES_COLUMN,
                    NEW_STOCK_VESHKI_COLUMN,
                )
                all_amount = None
            if all_amount is not None:
                new_stock_rows.extend(
                    self._build_all_warehouses_rows(
                        unit_row=unit_row,
                        amount=all_amount,
                    )
                )
                continue
            if veshki_amount is None:
                continue
            for target_warehouse in TARGET_WAREHOUSES:
                new_stock_rows.append(
                    UnitNewStockRow(
                        row_number=unit_row.row_number,
                        article_id=unit_row.article_id,
                        account=unit_row.account,
                        warehouse_id=target_warehouse.warehouse_id,
                        warehouse_alias=target_warehouse.warehouse_alias,
                        amount=veshki_amount
                        if target_warehouse.warehouse_id == VESHKI_WAREHOUSE_ID
                        else 0,
                        source_column=NEW_STOCK_VESHKI_COLUMN,
                    )
                )

        logger.info(
            "Новые FBS-остатки прочитаны из UNIT | sheet=%s | rows=%s",
            self.worksheet.title,
            len(new_stock_rows),
        )
        return new_stock_rows

    def _build_all_warehouses_rows(
        self,
        unit_row: UnitStocksRow,
        amount: int,
    ) -> list[UnitNewStockRow]:
        """Создает команды одинакового остатка для всех внутренних складов строки UNIT.

        Бизнес-правило: значение из `Новый остаток для всех складов` применяется к каждому складу
        из внутреннего справочника. Позже склады без активной привязки в БД будут пропущены с логом.
        """
        return [
            UnitNewStockRow(
                row_number=unit_row.row_number,
                article_id=unit_row.article_id,
                account=unit_row.account,
                warehouse_id=target_warehouse.warehouse_id,
                warehouse_alias=target_warehouse.warehouse_alias,
                amount=amount,
                source_column=NEW_STOCK_ALL_WAREHOUSES_COLUMN,
            )
            for target_warehouse in TARGET_WAREHOUSES
        ]

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
        """Записывает рассчитанные FBS-остатки в целевые колонки, не трогая остальные данные UNIT."""
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

    def clear_new_stock_cells(self, rows: list[UnitNewStockRow]) -> int:
        """Очищает успешно примененные команды `Новый остаток ...`, чтобы UNIT не отправил их повторно.

        Бизнес-сценарий: ячейка нового остатка является одноразовым поручением на изменение FBS-остатка.
        После подтвержденной отправки в WB значение нужно удалить только в этой ячейке, не затрагивая
        соседние склады, текущие FBS-остатки и строки, которые не были успешно отправлены.
        """
        if not rows:
            logger.info("Очистка новых FBS-остатков в UNIT пропущена: нет успешно отправленных строк.")
            return 0

        headers = self.worksheet.row_values(HEADER_ROW_INDEX)
        self._validate_headers(
            headers,
            (NEW_STOCK_ALL_WAREHOUSES_COLUMN, NEW_STOCK_VESHKI_COLUMN),
        )

        ranges: list[str] = []
        for row in rows:
            column_index = headers.index(row.source_column) + 1
            ranges.append(rowcol_to_a1(row.row_number, column_index))

        unique_ranges = sorted(set(ranges))
        if not unique_ranges:
            logger.info("Очистка новых FBS-остатков в UNIT пропущена: нет подходящих ячеек.")
            return 0

        self.worksheet.batch_clear(unique_ranges)
        logger.info(
            "Успешно примененные новые FBS-остатки очищены в UNIT | sheet=%s | cells=%s",
            self.worksheet.title,
            len(unique_ranges),
        )
        return len(unique_ranges)

    def clear_excluded_article_controls(self, row_numbers: list[int]) -> int:
        """Исключает строки удаленных товаров из FBS-сценариев, очищая их управляющие ячейки.

        Бизнес-сценарий: если WB отвечает `NotFound`, товар мог быть удален или перемещен в корзину
        через сайт. Чтобы cron и ручная отправка не пытались бесконечно обновлять такой артикул,
        мы очищаем одноразовые команды новых остатков и `Минимальный остаток` в этой строке.
        """
        if not row_numbers:
            logger.info("Исключение строк удаленных товаров из UNIT пропущено: нет подходящих строк.")
            return 0

        headers = self.worksheet.row_values(HEADER_ROW_INDEX)
        self._validate_headers(
            headers,
            (
                NEW_STOCK_ALL_WAREHOUSES_COLUMN,
                NEW_STOCK_VESHKI_COLUMN,
                MIN_STOCK_COLUMN,
            ),
        )

        ranges: list[str] = []
        control_columns = (
            NEW_STOCK_ALL_WAREHOUSES_COLUMN,
            NEW_STOCK_VESHKI_COLUMN,
            MIN_STOCK_COLUMN,
        )
        for row_number in sorted(set(row_numbers)):
            for column_name in control_columns:
                column_index = headers.index(column_name) + 1
                ranges.append(rowcol_to_a1(row_number, column_index))

        unique_ranges = sorted(set(ranges))
        self.worksheet.batch_clear(unique_ranges)
        logger.warning(
            "Строки удаленных товаров исключены из FBS-сценариев в UNIT | sheet=%s | rows=%s | cells=%s",
            self.worksheet.title,
            len(set(row_numbers)),
            len(unique_ranges),
        )
        return len(set(row_numbers))

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
        prepared_value = self._normalize_numeric_text(value)
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
        prepared_value = self._normalize_numeric_text(value).replace(",", ".")
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

    def _coerce_optional_non_negative_int(self, value: str) -> int | None:
        """Приводит числовые настройки UNIT/Сопост к int для сценария автопополнения.

        Бизнес-правило: пустое значение означает, что строка не участвует в автопополнении. Дробные
        и отрицательные значения не используются, чтобы cron не отправил в WB неоднозначный остаток.
        """
        prepared_value = self._normalize_numeric_text(value).replace(",", ".")
        if prepared_value == "":
            return None
        try:
            amount = float(prepared_value)
        except ValueError:
            return None
        if amount < 0 or not amount.is_integer():
            return None
        return int(amount)

    def _normalize_numeric_text(self, value: str) -> str:
        """Нормализует число из Google Sheets перед разбором остатков и лимитов.

        Бизнес-сценарий: пользователи могут вводить значения с разделителями тысяч, и Google
        Sheets сохраняет их как обычные или неразрывные пробелы. Сценарий должен одинаково
        понимать `1000`, `1 000` и `1 000`, чтобы не останавливать запуск из-за формата ячейки.
        """
        return str(value).strip().replace(" ", "").replace(NBSP_CHARACTER, "")
