from __future__ import annotations

"""Прямая синхронизация данных из таблицы закупщиков в таблицу логистов.

Бизнес-логика процесса:
1. Источником данных считается таблица закупщиков ``Заказы белые ТЕСТ``.
2. В работу логистов попадают только строки, которые прошли фильтр готовности:
   есть номер трака и статус ``товар готов к вывозу``.
3. Сопоставление между таблицами всегда выполняется по ``ORDER_LINE_ID``.
4. Если строка уже существует в ``ОТЧЁТ_2.0``, обновляются только управляемые поля
   закупщиков, кроме статуса: логистический статус после первого попадания строки
   в ``ОТЧЁТ_2.0`` считается зоной ответственности логистов и не должен
   перезаписываться обратно из таблицы закупщиков.
5. Если строка новая, она добавляется в конец таблицы логистов как новая запись.
6. Для новых строк фиксируется ``created_at``, а для всех обновленных строк
   актуализируется ``updatet_at``.

Техническая логика процесса:
1. Скрипт читает оба листа целиком, чтобы построить локальный снимок данных.
2. Заголовки и позиции колонок берутся из самого ``ОТЧЁТ_2.0``, поэтому порядок
   колонок в листе может меняться без переписывания кода.
3. Для существующих записей формируется список точечных изменений по ячейкам.
4. Для новых записей формируется отдельный набор строк на добавление.
5. Верхняя часть листа, строка заголовков и любые формулы вне тела таблицы не
   перезаписываются: скрипт меняет только нужные ячейки и добавляет новые строки.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from time import sleep
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.logger import setup_logger
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.logistic_ved.config import (
    CHINA_COLS,
    CHINA_FILTER_COLS,
    delivery_calculation_china,
    ved_logistics_2026,
)

logger = logging.getLogger(__name__)

# Названия ключевых бизнес-полей, с которыми работает синхронизация.
ORDER_LINE_ID_COLUMN = "ORDER_LINE_ID"
STATUS_COLUMN = "Статус"
TRUCK_NUMBER_COLUMN = "Номер Трака"
ORDER_SUM_RMB_COLUMN = "Сумма заказа, RMB"
CREATED_AT_COLUMN = "created_at"
UPDATED_AT_COLUMN = "updatet_at"
# Служебная колонка в локальном DataFrame, где хранится реальный номер строки из Google Sheets.
SHEET_ROW_NUMBER_COLUMN = "__sheet_row_number"
# Ячейка в ОТЧЁТ_2.0, куда пишется дата и время последнего успешного обновления.
LAST_SYNC_CELL = "A2"
READY_FOR_PICKUP_STATUS = "товар готов к вывозу"
# Количество попыток повторного запроса в Google Sheets при временных ошибках API.
GOOGLE_WRITE_RETRY_ATTEMPTS = 5
# Базовая пауза между повторными попытками, если Google Sheets не вернул отдельное время ожидания.
GOOGLE_WRITE_RETRY_DELAY_SECONDS = 2
# Коды ошибок Google Sheets, при которых запрос безопасно повторить.
GOOGLE_WRITE_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
# Максимальный размер одного пакета изменений при batch_update.
BATCH_UPDATE_CHUNK_SIZE = 500
# Список исходных колонок, которые можно исключать из прямой синхронизации без изменения CHINA_COLS.
OPTIONAL_COLUMNS: set[str] = set()
# Рабочий набор колонок закупщиков, которые реально участвуют в прямой синхронизации.
ACTIVE_CHINA_COLS = [column for column in CHINA_COLS if column not in OPTIONAL_COLUMNS]
# Набор колонок, которые должны присутствовать в целевой таблице логистов.
MANAGED_TARGET_COLUMNS = [*ACTIVE_CHINA_COLS, CREATED_AT_COLUMN, UPDATED_AT_COLUMN]


@dataclass(frozen=True, slots=True)
class GoogleSheetConfig:
    """Описание расположения данных в конкретном листе Google Sheets."""

    table_title: str
    sheet_title: str
    header_row_index: int
    data_row_index: int


@dataclass(frozen=True, slots=True)
class TargetSyncPlan:
    """Набор точечных изменений и новых строк для записи в целевой лист."""

    cell_updates: list[dict[str, object]]
    append_rows: list[list[object]]
    updated_rows_count: int
    new_rows_count: int


SOURCE_SHEET_CONFIG = GoogleSheetConfig(
    table_title=delivery_calculation_china["title"],
    sheet_title=delivery_calculation_china["white_orders_sheet"],
    header_row_index=3,
    data_row_index=4,
)

TARGET_SHEET_CONFIG = GoogleSheetConfig(
    table_title=ved_logistics_2026["title"],
    sheet_title=ved_logistics_2026["report_sheet"],
    header_row_index=3,
    data_row_index=4,
)


@dataclass(slots=True)
class LogisticVedUpdater:
    """Синхронизирует таблицу закупщиков с таблицей логистов по ORDER_LINE_ID.

    Бизнес-правило:
    значения из управляемых колонок актуализируются из таблицы закупщиков,
    если ORDER_LINE_ID уже существует в ОТЧЁТ_2.0. При этом колонки, которые
    не входят в рабочий набор синхронизации, сохраняются без изменений. Колонка
    ``Статус`` у уже существующих строк не обновляется, потому что после передачи
    строки логистам статусом управляет уже таблица ``ОТЧЁТ_2.0``.
    """

    source_config: GoogleSheetConfig = SOURCE_SHEET_CONFIG
    target_config: GoogleSheetConfig = TARGET_SHEET_CONFIG
    source_connector: GoogleTabs = field(init=False, repr=False)
    target_connector: GoogleTabs = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Инициализирует подключения к исходному и целевому листу."""
        self.source_connector = GoogleTabs(
            table_title=self.source_config.table_title,
            sheet_title=self.source_config.sheet_title,
        )
        self.target_connector = GoogleTabs(
            table_title=self.target_config.table_title,
            sheet_title=self.target_config.sheet_title,
        )

    def run(self) -> None:
        """Выполняет полный цикл прямой синхронизации.

        Метод последовательно:
        - читает исходную и целевую таблицы;
        - подготавливает отфильтрованный набор строк закупщиков;
        - определяет, какие строки нужно обновить, а какие добавить;
        - применяет к ``ОТЧЁТ_2.0`` только точечные изменения.
        """
        logger.info("Старт задачи logistic_ved_run.")

        # Сначала читаем обе таблицы полностью, чтобы работать уже с локальным снимком данных.
        source_values = self._execute_with_retry(
            operation_name="read source values logistic_ved",
            func=self.source_connector.sheet_title.get_all_values,
        )
        target_values = self._execute_with_retry(
            operation_name="read target values logistic_ved",
            func=self.target_connector.sheet_title.get_all_values,
        )

        source_dataframe = self._build_dataframe_from_sheet(
            values=source_values,
            sheet_config=self.source_config,
        )
        prepared_source = self._prepare_source_dataframe(source_dataframe)
        updated_at_value = self._build_updated_at_value()

        # Заголовки и существующие строки целевой таблицы читаем отдельно,
        # потому что порядок колонок в листе может меняться.
        target_headers = self._prepare_target_layout(target_values)
        existing_target_dataframe = self._build_existing_target_dataframe(
            values=target_values,
            target_headers=target_headers,
        )
        sync_plan = self._build_target_sync_plan(
            existing_target_dataframe=existing_target_dataframe,
            source_dataframe=prepared_source,
            target_headers=target_headers,
            updated_at_value=updated_at_value,
        )

        logger.info(
            "Подготовлено точечное обновление целевого листа по ORDER_LINE_ID: source_rows=%s existing_rows=%s updated_rows=%s new_rows=%s",
            len(prepared_source.index),
            len(existing_target_dataframe.index),
            sync_plan.updated_rows_count,
            sync_plan.new_rows_count,
        )

        self._apply_target_sync_plan(
            worksheet=self.target_connector.sheet_title,
            headers=target_headers,
            sync_plan=sync_plan,
        )
        self._update_last_sync_marker(
            worksheet=self.target_connector.sheet_title,
            updated_at_value=updated_at_value,
        )
        logger.info("Задача logistic_ved_run завершена успешно.")

    def _build_dataframe_from_sheet(
        self,
        values: list[list[str]],
        sheet_config: GoogleSheetConfig,
    ) -> pd.DataFrame:
        """Преобразует лист Google Sheets в DataFrame по настройкам листа."""
        headers = self._extract_headers(
            values=values,
            header_row_index=sheet_config.header_row_index,
        )
        if not headers:
            raise ValueError(
                f"В листе {sheet_config.table_title} / {sheet_config.sheet_title} не найдены заголовки."
            )

        normalized_rows = [
            self._normalize_row_length(row, len(headers))
            for row in values[sheet_config.data_row_index:]
        ]
        dataframe = pd.DataFrame(normalized_rows, columns=headers, dtype=object).fillna("")

        logger.info(
            "Прочитан лист %s / %s: headers=%s data_rows=%s",
            sheet_config.table_title,
            sheet_config.sheet_title,
            len(headers),
            len(dataframe.index),
        )
        return dataframe

    def _extract_headers(self, values: list[list[str]], header_row_index: int) -> list[str]:
        """Достает строку заголовков и удаляет пустые хвостовые ячейки."""
        if header_row_index >= len(values):
            raise ValueError(
                f"В листе отсутствует строка заголовков с индексом {header_row_index}."
            )

        headers = [str(cell).strip() for cell in values[header_row_index]]
        while headers and headers[-1] == "":
            headers.pop()
        return headers

    def _prepare_target_layout(self, values: list[list[str]]) -> list[str]:
        """Подготавливает заголовки целевого листа без изменения верхней части таблицы."""
        if len(values) > self.target_config.header_row_index:
            target_headers = self._extract_headers(
                values=values,
                header_row_index=self.target_config.header_row_index,
            )
        else:
            target_headers = []

        if not target_headers:
            raise ValueError(
                "В целевом листе не найдена строка заголовков. Частичное обновление невозможно."
            )

        return target_headers

    def _prepare_source_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Оставляет только нужные колонки и фильтрует строки для передачи логистам.

        На этом этапе происходит бизнес-отбор:
        - берем только управляемые колонки;
        - нормализуем ключевые поля;
        - исключаем строки без ``ORDER_LINE_ID``;
        - оставляем только строки, которые закупщики действительно передают логистам.
        """
        missing_columns = [column for column in ACTIVE_CHINA_COLS if column not in dataframe.columns]
        if missing_columns:
            raise ValueError(
                "В исходной таблице отсутствуют обязательные колонки: "
                + ", ".join(missing_columns)
            )

        missing_filter_columns = [
            column for column in CHINA_FILTER_COLS if column not in dataframe.columns
        ]
        if missing_filter_columns:
            raise ValueError(
                "В исходной таблице отсутствуют колонки фильтрации: "
                + ", ".join(missing_filter_columns)
            )

        source_dataframe = dataframe.loc[:, ACTIVE_CHINA_COLS].copy().fillna("")
        source_dataframe[TRUCK_NUMBER_COLUMN] = source_dataframe[TRUCK_NUMBER_COLUMN].map(
            self._normalize_string
        )
        source_dataframe[STATUS_COLUMN] = source_dataframe[STATUS_COLUMN].map(
            self._normalize_string
        )
        source_dataframe[ORDER_LINE_ID_COLUMN] = source_dataframe[ORDER_LINE_ID_COLUMN].map(
            self._normalize_string
        )
        if ORDER_SUM_RMB_COLUMN in source_dataframe.columns:
            # В таблице закупщиков сумма может храниться как строка с валютным символом,
            # пробелами и локальным форматированием. Для логистов переводим ее в число.
            source_dataframe[ORDER_SUM_RMB_COLUMN] = source_dataframe[ORDER_SUM_RMB_COLUMN].map(
                self._parse_rmb_value
            )

        # В исходной таблице ORDER_LINE_ID не должен быть пустым:
        # без него строку нельзя корректно сопоставить между закупщиками и логистами.
        empty_key_rows = source_dataframe[source_dataframe[ORDER_LINE_ID_COLUMN] == ""]
        if not empty_key_rows.empty:
            logger.warning(
                "Пропущены строки без ORDER_LINE_ID в таблице Заказы белые ТЕСТ: count=%s",
                len(empty_key_rows.index),
            )
            source_dataframe = source_dataframe[
                source_dataframe[ORDER_LINE_ID_COLUMN] != ""
            ].copy()

        # В прямую выгрузку попадают только строки, которые закупщики реально передают логистам.
        filtered_dataframe = source_dataframe[
            (source_dataframe[TRUCK_NUMBER_COLUMN] != "")
            & (source_dataframe[STATUS_COLUMN].str.lower() == READY_FOR_PICKUP_STATUS)
        ].copy()

        # Если в источнике есть дубли, забираем последнюю строку по ключу,
        # считая ее самой актуальной версией у закупщиков.
        duplicate_mask = filtered_dataframe[ORDER_LINE_ID_COLUMN].duplicated(keep="last")
        duplicate_count = int(duplicate_mask.sum())
        if duplicate_count:
            logger.warning(
                "В источнике найдены дубли ORDER_LINE_ID. Будет использована последняя строка по каждому ключу: duplicates=%s",
                duplicate_count,
            )
            filtered_dataframe = filtered_dataframe.loc[~duplicate_mask].copy()

        logger.info(
            "Источник подготовлен: raw_rows=%s filtered_rows=%s",
            len(dataframe.index),
            len(filtered_dataframe.index),
        )
        return filtered_dataframe

    def _build_existing_target_dataframe(
        self,
        values: list[list[str]],
        target_headers: list[str],
    ) -> pd.DataFrame:
        """Собирает DataFrame из уже существующих строк таблицы логистов."""
        normalized_rows: list[list[object]] = []
        sheet_row_numbers: list[int] = []
        for row_offset, row in enumerate(values[self.target_config.data_row_index:]):
            normalized_rows.append(self._normalize_row_length(row, len(target_headers)))
            sheet_row_numbers.append(self.target_config.data_row_index + 1 + row_offset)

        dataframe = pd.DataFrame(normalized_rows, columns=target_headers, dtype=object).fillna("")
        dataframe.insert(0, SHEET_ROW_NUMBER_COLUMN, sheet_row_numbers)
        logger.info(
            "Прочитаны уже существующие строки целевого листа: rows=%s",
            len(dataframe.index),
        )
        return dataframe

    def _build_target_sync_plan(
        self,
        existing_target_dataframe: pd.DataFrame,
        source_dataframe: pd.DataFrame,
        target_headers: list[str],
        updated_at_value: str,
    ) -> TargetSyncPlan:
        """Формирует точечные обновления и новые строки для целевого листа.

        Технически это ключевой этап прямой синхронизации:
        - существующие ``ORDER_LINE_ID`` превращаются в набор адресных обновлений ячеек;
        - новые ``ORDER_LINE_ID`` превращаются в полноценные строки на добавление;
        - при обновлении существующих строк колонка ``Статус`` сознательно
          пропускается, чтобы ``logistic_ved_full_run`` не откатывал ручные
          логистические статусы обратно к значениям закупщиков;
        - сама запись в Google Sheets на этом шаге еще не выполняется.
        """
        if ORDER_LINE_ID_COLUMN not in target_headers:
            raise ValueError(
                f"В целевой таблице отсутствует обязательная колонка {ORDER_LINE_ID_COLUMN}."
            )

        required_target_columns = [*ACTIVE_CHINA_COLS, CREATED_AT_COLUMN, UPDATED_AT_COLUMN]
        missing_target_columns = [
            column for column in required_target_columns if column not in target_headers
        ]
        if missing_target_columns:
            raise ValueError(
                "В целевой таблице отсутствуют колонки из рабочего набора: "
                + ", ".join(missing_target_columns)
            )

        target_header_map = {header: index + 1 for index, header in enumerate(target_headers)}
        update_map: dict[str, dict[str, object]] = {}
        existing_result = existing_target_dataframe.reindex(
            columns=[SHEET_ROW_NUMBER_COLUMN, *target_headers],
            fill_value="",
        ).copy()
        existing_result[ORDER_LINE_ID_COLUMN] = existing_result[ORDER_LINE_ID_COLUMN].map(
            self._normalize_string
        )
        source_result = source_dataframe.copy()
        source_result[ORDER_LINE_ID_COLUMN] = source_result[ORDER_LINE_ID_COLUMN].map(
            self._normalize_string
        )

        existing_result = existing_result.drop_duplicates(
            subset=ORDER_LINE_ID_COLUMN,
            keep="last",
        ).copy()
        source_result = source_result.drop_duplicates(
            subset=ORDER_LINE_ID_COLUMN,
            keep="last",
        ).copy()

        existing_result = existing_result.set_index(ORDER_LINE_ID_COLUMN, drop=False)
        source_result = source_result.set_index(ORDER_LINE_ID_COLUMN, drop=False)

        matched_keys = existing_result.index.intersection(source_result.index)
        new_keys = source_result.index.difference(existing_result.index)

        # Для уже существующих строк меняем только управляемые колонки и updated_at.
        for order_line_id in matched_keys:
            row_number = int(existing_result.at[order_line_id, SHEET_ROW_NUMBER_COLUMN])
            for column in ACTIVE_CHINA_COLS:
                if column == STATUS_COLUMN:
                    continue

                self._add_update(
                    update_map=update_map,
                    row_number=row_number,
                    column_number=target_header_map[column],
                    value=self._sheet_cell_value(source_result.at[order_line_id, column]),
                )
            self._add_update(
                update_map=update_map,
                row_number=row_number,
                column_number=target_header_map[UPDATED_AT_COLUMN],
                value=updated_at_value,
            )

        # Для новых ORDER_LINE_ID формируем полноценные новые строки на добавление.
        append_rows: list[list[object]] = []
        new_rows = source_result.loc[new_keys].copy()
        for order_line_id in new_rows.index:
            row_values = {header: "" for header in target_headers}
            for column in ACTIVE_CHINA_COLS:
                row_values[column] = self._sheet_cell_value(new_rows.at[order_line_id, column])
            row_values[CREATED_AT_COLUMN] = updated_at_value
            row_values[UPDATED_AT_COLUMN] = updated_at_value
            append_rows.append([row_values.get(header, "") for header in target_headers])

        logger.info(
            "Подготовлен план точечной записи в целевой лист: updated_rows=%s new_rows=%s cell_updates=%s",
            len(matched_keys),
            len(append_rows),
            len(update_map),
        )
        return TargetSyncPlan(
            cell_updates=list(update_map.values()),
            append_rows=append_rows,
            updated_rows_count=len(matched_keys),
            new_rows_count=len(append_rows),
        )

    def _build_updated_at_value(self) -> str:
        """Возвращает текущее московское время в строковом формате."""
        return datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M:%S")

    def _apply_target_sync_plan(
        self,
        worksheet,
        headers: list[str],
        sync_plan: TargetSyncPlan,
    ) -> None:
        """Применяет к целевому листу только точечные обновления и новые строки.

        Бизнес-смысл:
        скрипт не пересобирает заново весь ``ОТЧЁТ_2.0``, а бережно обновляет
        только нужные поля и добавляет только новые записи.
        """
        if sync_plan.cell_updates:
            self._apply_cell_updates(
                operation_name="batch_update ved_logistics_2026",
                worksheet=worksheet,
                updates=sync_plan.cell_updates,
            )

        if sync_plan.append_rows:
            self._execute_with_retry(
                operation_name="append_rows ved_logistics_2026",
                func=worksheet.append_rows,
                values=sync_plan.append_rows,
                value_input_option="USER_ENTERED",
                table_range=f"A{self.target_config.header_row_index + 1}:{rowcol_to_a1(self.target_config.header_row_index + 1, len(headers))}",
            )

    def _update_last_sync_marker(self, worksheet, updated_at_value: str) -> None:
        """Записывает в A2 дату и время последнего успешного обновления данных."""
        self._execute_with_retry(
            operation_name="update ved_logistics_2026 last sync marker",
            func=worksheet.update,
            range_name=LAST_SYNC_CELL,
            values=[[updated_at_value]],
            value_input_option="USER_ENTERED",
        )

    def _apply_cell_updates(
        self,
        operation_name: str,
        worksheet,
        updates: list[dict[str, object]],
    ) -> None:
        """Отправляет точечные обновления в Google Sheets пакетами."""
        if not updates:
            return

        for batch_start in range(0, len(updates), BATCH_UPDATE_CHUNK_SIZE):
            batch = updates[batch_start: batch_start + BATCH_UPDATE_CHUNK_SIZE]
            self._execute_with_retry(
                operation_name=operation_name,
                func=worksheet.batch_update,
                data=batch,
                value_input_option="USER_ENTERED",
            )

    @staticmethod
    def _add_update(
        update_map: dict[str, dict[str, object]],
        row_number: int,
        column_number: int,
        value: object,
    ) -> None:
        """Добавляет или заменяет изменение в карте обновлений по адресу ячейки."""
        cell_range = rowcol_to_a1(row_number, column_number)
        update_map[cell_range] = {
            "range": cell_range,
            "values": [[value]],
        }

    def _normalize_row_length(self, row: list[str], width: int) -> list[str]:
        """Обрезает или дополняет строку до заданной ширины."""
        normalized_row = list(row[:width])
        if len(normalized_row) < width:
            normalized_row.extend([""] * (width - len(normalized_row)))
        return [self._sheet_cell_value(cell) for cell in normalized_row]

    @staticmethod
    def _normalize_string(value: object) -> str:
        """Приводит значение к строке без внешних пробелов."""
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _sheet_cell_value(value: object) -> object:
        """Подготавливает значение к записи в ячейку Google Sheets."""
        if value is None:
            return ""
        if pd.isna(value):
            return ""
        if isinstance(value, (bool, int, float)):
            return value
        return str(value)

    def _parse_rmb_value(self, value: object) -> float | str:
        """Пытается преобразовать сумму в RMB из текстового формата в число."""
        if value is None:
            return ""
        if isinstance(value, (int, float)) and not pd.isna(value):
            return float(value)

        normalized_value = self._normalize_string(value)
        if normalized_value == "":
            return ""

        cleaned_value = normalized_value.replace("\xa0", " ").strip()
        cleaned_value = re.sub(r"^[^\d\-]+", "", cleaned_value)
        cleaned_value = (
            cleaned_value.replace("¥", "")
            .replace("ВҐ", "")
            .replace("пїҐ", "")
            .replace("в‚Ѕ", "")
            .replace("р.", "")
            .replace("руб.", "")
            .replace("руб", "")
            .replace(" ", "")
            .replace(",", ".")
        )
        cleaned_value = re.sub(r"[^0-9.\-]", "", cleaned_value)

        try:
            return float(cleaned_value)
        except ValueError:
            logger.warning(
                "Не удалось преобразовать значение в число для колонки '%s': value=%s",
                ORDER_SUM_RMB_COLUMN,
                normalized_value,
            )
            return normalized_value

    def _execute_with_retry(self, operation_name: str, func, *args, **kwargs):
        """Выполняет запрос к Google Sheets с повторными попытками при временных ошибках."""
        for attempt in range(1, GOOGLE_WRITE_RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as error:
                if not self._is_retryable_google_error(error):
                    logger.exception(
                        "Операция Google Sheets завершилась неретрайбл-ошибкой: operation=%s attempt=%s",
                        operation_name,
                        attempt,
                    )
                    raise

                if attempt == GOOGLE_WRITE_RETRY_ATTEMPTS:
                    logger.exception(
                        "Операция Google Sheets исчерпала все попытки retry: operation=%s attempts=%s",
                        operation_name,
                        GOOGLE_WRITE_RETRY_ATTEMPTS,
                    )
                    raise

                logger.warning(
                    "Временная ошибка Google Sheets, повторяем попытку: operation=%s attempt=%s/%s delay_seconds=%s error=%s",
                    operation_name,
                    attempt,
                    GOOGLE_WRITE_RETRY_ATTEMPTS,
                    GOOGLE_WRITE_RETRY_DELAY_SECONDS,
                    error,
                )
                retry_delay_seconds = self._get_retry_delay_seconds(error, attempt)
                logger.warning(
                    "Google Sheets retry delay adjusted: operation=%s attempt=%s delay_seconds=%s",
                    operation_name,
                    attempt,
                    retry_delay_seconds,
                )
                sleep(retry_delay_seconds)

        raise RuntimeError(f"Не удалось завершить операцию {operation_name}.")

    @staticmethod
    def _is_retryable_google_error(error: gspread.exceptions.APIError) -> bool:
        """Проверяет, относится ли ошибка Google Sheets к временным."""
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            return status_code in GOOGLE_WRITE_RETRY_STATUS_CODES

        error_text = str(error)
        return any(f"[{code}]" in error_text for code in GOOGLE_WRITE_RETRY_STATUS_CODES)

    @staticmethod
    def _get_retry_delay_seconds(
        error: gspread.exceptions.APIError,
        attempt: int,
    ) -> int:
        """Возвращает задержку перед следующей попыткой запроса."""
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code == 429:
            retry_delays = (15, 30, 45, 60)
            return retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        return GOOGLE_WRITE_RETRY_DELAY_SECONDS


def logistic_ved_run() -> None:
    """Точка входа для синхронизации данных закупщиков с таблицей логистов."""

    setup_logger()
    LogisticVedUpdater().run()


def logistic_ved_full_run() -> None:
    """Точка входа для полного цикла: сначала обратная, затем прямая синхронизация."""

    from src_oop.jobs.logistic_ved.reverse_run import logistic_ved_reverse_run

    logistic_ved_reverse_run()
    logistic_ved_run()


if __name__ == "__main__":
    logistic_ved_run()
