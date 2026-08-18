from __future__ import annotations

"""Обратная синхронизация логистических данных из таблицы логистов в таблицу закупщиков.

Бизнес-логика процесса:
1. После передачи строки логистам таблица ``ОТЧЁТ_2.0`` становится рабочим
   источником по логистическим статусам, датам и движениям груза.
2. Закупщикам нужно обратно получать только те поля, которые относятся к
   логистическому этапу процесса.
3. Статусы между таблицами не обязаны совпадать дословно, поэтому для них
   действует отдельная таблица соответствий.
4. Дополнительно выполняется сверка ``wild``, количества и статуса, чтобы
   вовремя подсвечивать расхождения между закупщиками и логистами.

Техническая логика процесса:
1. Обе таблицы читаются и сопоставляются по ``ORDER_LINE_ID``.
2. Для каждой найденной пары строк рассчитываются точечные изменения:
   в таблицу закупщиков, а при необходимости и обратно в таблицу логистов.
3. Все изменения копятся в память и затем отправляются пакетами через
   ``batch_update``.
4. Если авто-пометка о сверке больше не нужна, скрипт сам очищает только те
   сообщения, которые ранее поставил он сам.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from time import sleep
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.database import Database
from src_oop.core.logger import setup_logger
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.logistic_ved.config import (
    LOGISTIC_TO_CHINA_SYNC_COLS,
    SUPPLY_ACCEPTANCE_STATUS_QUERY,
    delivery_calculation_china,
    ved_logistics_2026,
)

logger = logging.getLogger(__name__)

# Названия колонок, участвующих в сопоставлении и сверке.
ORDER_LINE_ID_COLUMN = "ORDER_LINE_ID"
STATUS_COLUMN = "Статус"
WILD_COLUMN = "wild"
ORDER_QTY_COLUMN = "Кол-во к заказу"
TRUCK_NUMBER_COLUMN = "Номер Трака"
TRANSPORT_NUMBER_COLUMN = "Номер ТС"
DATA_CHECK_COLUMN = "Сверка данных"
DATA_CHECK_MESSAGE = "Сверьте wild и количество"
DATA_CHECK_STATUS_MESSAGE = "Сверьте wild, количество и статус"
# Список авто-сообщений, которые скрипт сам ставит и сам же может очистить,
# если расхождение между таблицами исчезло.
AUTO_DATA_CHECK_MESSAGES = {
    DATA_CHECK_MESSAGE,
    DATA_CHECK_STATUS_MESSAGE,
}

IN_TRANSIT_STATUS = "в пути"
AFGHAN_CAULDRONS_IN_TRANSIT_STATUS = "афганские казаны в пути"
UZBEK_CAULDRONS_IN_TRANSIT_STATUS = "узбекские казаны в пути"
ACCEPTED_BY_WAREHOUSE_STATUS = "принят складом"
ARRIVED_STATUS = "прибыло"
READY_FOR_PICKUP_STATUS = "товар готов к вывозу"
CUSTOMS_CLEARED_TO_WAREHOUSE_STATUS = "растаможен, в пути на склад"
WAITING_FOR_LOADING_STATUS = "Ждём загрузку".lower()
CUSTOMS_STATUS = "Таможня".lower()
UNDERLOAD_STATUS = "недозагрузка"
CUSTOMS_ZBK_STATUS = "Таможня ЗБК".lower()
AT_SUBMISSION_STATUS = "На подаче".lower()
INSPECTION_PROBLEM_STATUS = "осмотр/ досмотр/ проблема"
ACCEPTANCE_DISCREPANCY_STATUS = "расхождения при приемке"

# Группа исходных статусов закупщиков, которые считаются эквивалентами общего состояния "в пути".
SOURCE_STATUSES_IN_TRANSIT_GROUP = {
    IN_TRANSIT_STATUS,
    AFGHAN_CAULDRONS_IN_TRANSIT_STATUS,
    UZBEK_CAULDRONS_IN_TRANSIT_STATUS,
}

# Допустимые сочетания статусов между таблицей логистов и таблицей закупщиков.
# Ключ словаря — статус в ОТЧЁТ_2.0, значение — допустимые статусы в Заказы белые ТЕСТ.
ALLOWED_SOURCE_STATUSES_BY_TARGET_STATUS = {
    # ОТЧЁТ_2.0: "принят складом" -> Заказы белые ТЕСТ: "прибыло"
    ACCEPTED_BY_WAREHOUSE_STATUS: {ARRIVED_STATUS},

    # ОТЧЁТ_2.0: "растаможен, в пути на склад" -> Заказы белые ТЕСТ: один из статусов группы "в пути"
    CUSTOMS_CLEARED_TO_WAREHOUSE_STATUS: SOURCE_STATUSES_IN_TRANSIT_GROUP,

    # ОТЧЁТ_2.0: "в пути" -> Заказы белые ТЕСТ: один из статусов группы "в пути"
    IN_TRANSIT_STATUS: SOURCE_STATUSES_IN_TRANSIT_GROUP,

    # ОТЧЁТ_2.0: "Ждём загрузку" -> Заказы белые ТЕСТ: "товар готов к вывозу"
    WAITING_FOR_LOADING_STATUS: {READY_FOR_PICKUP_STATUS},

    # ОТЧЁТ_2.0: "Таможня" -> Заказы белые ТЕСТ: один из статусов группы "в пути"
    CUSTOMS_STATUS: SOURCE_STATUSES_IN_TRANSIT_GROUP,

    # ОТЧЁТ_2.0: "недозагрузка" -> Заказы белые ТЕСТ: допустимых статусов нет, всегда нужна сверка
    UNDERLOAD_STATUS: set(),

    # ОТЧЁТ_2.0: "Таможня ЗБК" -> Заказы белые ТЕСТ: один из статусов группы "в пути"
    CUSTOMS_ZBK_STATUS: SOURCE_STATUSES_IN_TRANSIT_GROUP,

    # ОТЧЁТ_2.0: "На подаче" -> Заказы белые ТЕСТ: один из статусов группы "в пути"
    AT_SUBMISSION_STATUS: SOURCE_STATUSES_IN_TRANSIT_GROUP,

    # ОТЧЁТ_2.0: "товар готов к вывозу" -> Заказы белые ТЕСТ: "товар готов к вывозу"
    READY_FOR_PICKUP_STATUS: {READY_FOR_PICKUP_STATUS},

    # ОТЧЁТ_2.0: "осмотр/ досмотр/ проблема" -> Заказы белые ТЕСТ: один из статусов группы "в пути"
    INSPECTION_PROBLEM_STATUS: SOURCE_STATUSES_IN_TRANSIT_GROUP,
}

# Соответствие статусов из ОТЧЁТ_2.0, которые нужно принудительно вернуть обратно закупщикам.
# Ключ — статус в ОТЧЁТ_2.0, значение — какой статус нужно записать в Заказы белые ТЕСТ.
TARGET_STATUS_BY_SOURCE_STATUS = {
    IN_TRANSIT_STATUS: IN_TRANSIT_STATUS,
    CUSTOMS_ZBK_STATUS: IN_TRANSIT_STATUS,
    INSPECTION_PROBLEM_STATUS: IN_TRANSIT_STATUS,
    CUSTOMS_STATUS: IN_TRANSIT_STATUS,
    CUSTOMS_CLEARED_TO_WAREHOUSE_STATUS: IN_TRANSIT_STATUS,
    ACCEPTED_BY_WAREHOUSE_STATUS: ARRIVED_STATUS,
}

# Количество попыток повторного запроса в Google Sheets при временных ошибках API.
GOOGLE_WRITE_RETRY_ATTEMPTS = 5
# Базовая пауза между повторными попытками, если сервер не вернул отдельное правило ожидания.
GOOGLE_WRITE_RETRY_DELAY_SECONDS = 2
# Коды ошибок Google Sheets, при которых запрос безопасно повторить.
GOOGLE_WRITE_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
# Индекс строки заголовков в листе Заказы белые ТЕСТ, считая с нуля.
SOURCE_HEADER_ROW_INDEX = 3
# Индекс первой строки данных в листе Заказы белые ТЕСТ, считая с нуля.
SOURCE_DATA_ROW_INDEX = 4
# Индекс строки заголовков в листе ОТЧЁТ_2.0, считая с нуля.
TARGET_HEADER_ROW_INDEX = 3
# Индекс первой строки данных в листе ОТЧЁТ_2.0, считая с нуля.
TARGET_DATA_ROW_INDEX = 4
# Максимальный размер одного пакета изменений при batch_update в Google Sheets.
BATCH_UPDATE_CHUNK_SIZE = 500
# Ячейка с датой и временем последней успешной обратной синхронизации в таблице закупщиков.
LAST_SYNC_CELL = "B1"
# Ключ автоматической приемки: номер трака, номер ТС и wild.
AcceptanceKey = tuple[str, str, str]


@dataclass(frozen=True, slots=True)
class TargetRowSnapshot:
    """Снимок строки целевой таблицы с номером строки и ее значениями."""

    row_number: int
    values: dict[str, object]


@dataclass(frozen=True, slots=True)
class ReverseSyncUpdates:
    """Набор подготовленных изменений для обеих таблиц."""

    source_updates: list[dict[str, object]]
    target_updates: list[dict[str, object]]

    @property
    def total_updates(self) -> int:
        """Возвращает общее количество ячеек, которые будут обновлены."""
        return len(self.source_updates) + len(self.target_updates)


@dataclass(frozen=True, slots=True)
class AcceptanceStatusResult:
    """Результат финальной автопроверки приемки для одной строки ОТЧЁТ_2.0.

    Бизнес-логика:
    - строка получает статус ``принят складом`` только при полном совпадении
      трака, автомобиля, wild и количества с данными БД;
    - если ключ совпал, но количество нет, строка получает статус
      ``расхождения при приемке``;
    - если ключ не найден или найден неоднозначно, статус не меняется.
    """

    status: str | None
    matched: bool
    quantity_mismatch: bool
    duplicate_matches: bool


@dataclass(frozen=True, slots=True)
class AcceptanceStatusSummary:
    """Сводка по финальной автопроверке приемки для логов сценария."""

    checked_rows: int
    matched_rows: int
    accepted_rows: int
    discrepancy_rows: int
    duplicate_rows: int


@dataclass(slots=True)
class LogisticVedReverseUpdater:
    """Возвращает логистические данные и результаты сверки обратно закупщикам.

    Этот класс реализует обратный поток процесса:
    логисты продолжают вести свою операционную таблицу, а закупщики получают
    обратно только согласованные логистические поля и сигналы о расхождениях.
    """

    source_connector: GoogleTabs = field(init=False, repr=False)
    target_connector: GoogleTabs = field(init=False, repr=False)
    database_cls: type[Database] = field(default=Database, repr=False)

    def __post_init__(self) -> None:
        """Инициализирует подключения к таблице закупщиков и таблице логистов."""
        self.source_connector = GoogleTabs(
            table_title=delivery_calculation_china["title"],
            sheet_title=delivery_calculation_china["white_orders_sheet"],
        )
        self.target_connector = GoogleTabs(
            table_title=ved_logistics_2026["title"],
            sheet_title=ved_logistics_2026["report_sheet"],
        )

    def run(self) -> None:
        """Выполняет полный цикл обратной синхронизации.

        Метод:
        - читает обе таблицы;
        - строит сопоставление строк по ``ORDER_LINE_ID``;
        - рассчитывает список изменений;
        - применяет их пакетно в обе стороны, если изменения действительно есть.
        """
        logger.info("Старт задачи logistic_ved_reverse_run.")

        source_values = self._execute_with_retry(
            operation_name="read source values logistic_ved_reverse",
            func=self.source_connector.sheet_title.get_all_values,
        )
        target_values = self._execute_with_retry(
            operation_name="read target values logistic_ved_reverse",
            func=self.target_connector.sheet_title.get_all_values,
        )
        updated_at_value = self._build_updated_at_value()

        source_headers = self._extract_headers(source_values, SOURCE_HEADER_ROW_INDEX)
        target_headers = self._extract_headers(target_values, TARGET_HEADER_ROW_INDEX)
        self._validate_headers(source_headers, target_headers)

        source_row_positions = self._build_source_row_positions(
            values=source_values,
            headers=source_headers,
        )
        target_rows_by_key = self._build_target_rows_by_key(
            values=target_values,
            headers=target_headers,
        )

        # На этом шаге только рассчитываем изменения.
        # Запись в таблицы выполняется отдельно, если действительно есть что обновлять.
        updates = self._build_updates(
            source_values=source_values,
            source_headers=source_headers,
            target_headers=target_headers,
            source_row_positions=source_row_positions,
            target_rows_by_key=target_rows_by_key,
        )
        updates = self._append_final_acceptance_updates(
            updates=updates,
            target_headers=target_headers,
            target_rows_by_key=target_rows_by_key,
        )

        if updates.total_updates == 0:
            self._update_last_sync_marker(
                worksheet=self.source_connector.sheet_title,
                updated_at_value=updated_at_value,
            )
            logger.info("Изменений для обратной записи не найдено.")
            return

        self._apply_updates(updates)
        self._update_last_sync_marker(
            worksheet=self.source_connector.sheet_title,
            updated_at_value=updated_at_value,
        )
        logger.info(
            "Задача logistic_ved_reverse_run завершена успешно: source_updates=%s target_updates=%s total_updates=%s",
            len(updates.source_updates),
            len(updates.target_updates),
            updates.total_updates,
        )

    def _extract_headers(self, values: list[list[str]], header_row_index: int) -> list[str]:
        """Достает строку заголовков и обрезает пустые хвостовые ячейки."""
        if header_row_index >= len(values):
            raise ValueError(
                f"В листе отсутствует строка заголовков с индексом {header_row_index}."
            )

        headers = [str(cell).strip() for cell in values[header_row_index]]
        while headers and headers[-1] == "":
            headers.pop()
        return headers

    def _validate_headers(
        self,
        source_headers: list[str],
        target_headers: list[str],
    ) -> None:
        """Проверяет, что обе таблицы содержат обязательные для синхронизации колонки."""
        required_columns = [
            ORDER_LINE_ID_COLUMN,
            STATUS_COLUMN,
            WILD_COLUMN,
            ORDER_QTY_COLUMN,
            DATA_CHECK_COLUMN,
            *LOGISTIC_TO_CHINA_SYNC_COLS,
        ]

        missing_source_columns = [
            column for column in required_columns if column not in source_headers
        ]
        if missing_source_columns:
            raise ValueError(
                "В таблице Заказы белые ТЕСТ отсутствуют обязательные колонки: "
                + ", ".join(missing_source_columns)
            )

        missing_target_columns = [
            column for column in required_columns if column not in target_headers
        ]
        if missing_target_columns:
            raise ValueError(
                "В таблице ОТЧЁТ_2.0 отсутствуют обязательные колонки: "
                + ", ".join(missing_target_columns)
            )

    def _build_source_row_positions(
        self,
        values: list[list[str]],
        headers: list[str],
    ) -> dict[str, list[int]]:
        """Строит карту ORDER_LINE_ID -> список номеров строк в таблице закупщиков."""
        order_line_id_index = headers.index(ORDER_LINE_ID_COLUMN)
        positions: dict[str, list[int]] = {}
        empty_order_line_id_count = 0

        for row_offset, row in enumerate(values[SOURCE_DATA_ROW_INDEX:]):
            normalized_row = self._normalize_row_length(row, len(headers))
            order_line_id = self._normalize_string(normalized_row[order_line_id_index])
            if order_line_id == "":
                empty_order_line_id_count += 1
                continue

            sheet_row_number = SOURCE_DATA_ROW_INDEX + 1 + row_offset
            positions.setdefault(order_line_id, []).append(sheet_row_number)

        if empty_order_line_id_count:
            logger.warning(
                "В таблице Заказы белые ТЕСТ пропущены строки без ORDER_LINE_ID: count=%s",
                empty_order_line_id_count,
            )

        duplicate_source_keys = sum(
            1 for row_numbers in positions.values() if len(row_numbers) > 1
        )
        if duplicate_source_keys:
            logger.warning(
                "В таблице Заказы белые ТЕСТ найдены дубли ORDER_LINE_ID. Обновлены будут все строки с одинаковым ключом: duplicates=%s",
                duplicate_source_keys,
            )

        return positions

    def _build_target_rows_by_key(
        self,
        values: list[list[str]],
        headers: list[str],
    ) -> dict[str, TargetRowSnapshot]:
        """Строит карту ORDER_LINE_ID -> строка из таблицы логистов."""
        order_line_id_index = headers.index(ORDER_LINE_ID_COLUMN)
        rows_by_key: dict[str, TargetRowSnapshot] = {}
        duplicate_count = 0

        for row_offset, row in enumerate(values[TARGET_DATA_ROW_INDEX:]):
            normalized_row = self._normalize_row_length(row, len(headers))
            order_line_id = self._normalize_string(normalized_row[order_line_id_index])
            if order_line_id == "":
                continue

            if order_line_id in rows_by_key:
                duplicate_count += 1

            row_number = TARGET_DATA_ROW_INDEX + 1 + row_offset
            rows_by_key[order_line_id] = TargetRowSnapshot(
                row_number=row_number,
                values={
                    header: normalized_row[index]
                    for index, header in enumerate(headers)
                },
            )

        if duplicate_count:
            logger.warning(
                "В таблице ОТЧЁТ_2.0 найдены дубли ORDER_LINE_ID. Для обратной выгрузки будет использована последняя строка по ключу: duplicates=%s",
                duplicate_count,
            )

        return rows_by_key

    def _build_updates(
        self,
        source_values: list[list[str]],
        source_headers: list[str],
        target_headers: list[str],
        source_row_positions: dict[str, list[int]],
        target_rows_by_key: dict[str, TargetRowSnapshot],
    ) -> ReverseSyncUpdates:
        """Рассчитывает все изменения, которые нужно отправить в обе таблицы.

        Бизнес-смысл метода:
        - вернуть закупщикам логистические поля;
        - при необходимости синхронизировать статус по правилам соответствия;
        - проставить или снять пометку ``Сверка данных``.
        """
        source_update_map: dict[str, dict[str, object]] = {}
        target_update_map: dict[str, dict[str, object]] = {}
        source_header_map = {header: index + 1 for index, header in enumerate(source_headers)}
        target_header_map = {header: index + 1 for index, header in enumerate(target_headers)}
        source_rows_map = self._build_source_rows_map(source_values, source_headers)

        matched_keys = 0
        data_check_hits = 0
        for order_line_id, target_snapshot in target_rows_by_key.items():
            # В обратную синхронизацию попадают только те строки,
            # для которых удалось найти исходную запись закупщиков.
            source_row_numbers = source_row_positions.get(order_line_id)
            if not source_row_numbers:
                continue

            matched_keys += 1
            target_row = target_snapshot.values
            for row_number in source_row_numbers:
                source_row = source_rows_map.get(row_number, {})

                # Возвращаем в таблицу закупщиков только поля логистического блока.
                for column_name in LOGISTIC_TO_CHINA_SYNC_COLS:
                    new_value = self._sheet_cell_value(target_row.get(column_name, ""))
                    current_value = self._sheet_cell_value(source_row.get(column_name, ""))
                    if self._values_equal(current_value, new_value):
                        continue

                    self._add_update(
                        update_map=source_update_map,
                        row_number=row_number,
                        column_number=source_header_map[column_name],
                        value=new_value,
                    )

                # Отдельно обрабатываем статусы, где логисты управляют движением груза,
                # а закупщикам нужно видеть согласованное целевое значение.
                target_status = self._normalize_string(target_row.get(STATUS_COLUMN, "")).lower()
                source_status = self._normalize_string(source_row.get(STATUS_COLUMN, "")).lower()
                mapped_source_status = TARGET_STATUS_BY_SOURCE_STATUS.get(target_status)
                if mapped_source_status and source_status != mapped_source_status:
                    self._add_update(
                        update_map=source_update_map,
                        row_number=row_number,
                        column_number=source_header_map[STATUS_COLUMN],
                        value=mapped_source_status,
                    )

                # Проверка расхождений работает в обе стороны:
                # если проблема есть, ставим пометку в обе таблицы;
                # если проблема исчезла, очищаем только авто-сгенерированные сообщения.
                data_check_message = self._get_data_check_message(
                    source_row=source_row,
                    target_row=target_row,
                )
                if data_check_message is not None:
                    data_check_hits += 1
                    if self._normalize_string(source_row.get(DATA_CHECK_COLUMN, "")) != data_check_message:
                        self._add_update(
                            update_map=source_update_map,
                            row_number=row_number,
                            column_number=source_header_map[DATA_CHECK_COLUMN],
                            value=data_check_message,
                        )
                    if self._normalize_string(target_row.get(DATA_CHECK_COLUMN, "")) != data_check_message:
                        self._add_update(
                            update_map=target_update_map,
                            row_number=target_snapshot.row_number,
                            column_number=target_header_map[DATA_CHECK_COLUMN],
                            value=data_check_message,
                        )
                else:
                    source_check_value = self._normalize_string(source_row.get(DATA_CHECK_COLUMN, ""))
                    target_check_value = self._normalize_string(target_row.get(DATA_CHECK_COLUMN, ""))
                    if source_check_value in AUTO_DATA_CHECK_MESSAGES:
                        self._add_update(
                            update_map=source_update_map,
                            row_number=row_number,
                            column_number=source_header_map[DATA_CHECK_COLUMN],
                            value="",
                        )
                    if target_check_value in AUTO_DATA_CHECK_MESSAGES:
                        self._add_update(
                            update_map=target_update_map,
                            row_number=target_snapshot.row_number,
                            column_number=target_header_map[DATA_CHECK_COLUMN],
                            value="",
                        )

        source_updates = list(source_update_map.values())
        target_updates = list(target_update_map.values())
        logger.info(
            "Подготовлена обратная синхронизация логистических данных: matched_order_line_ids=%s source_updates=%s target_updates=%s data_check_hits=%s",
            matched_keys,
            len(source_updates),
            len(target_updates),
            data_check_hits,
        )
        return ReverseSyncUpdates(
            source_updates=source_updates,
            target_updates=target_updates,
        )

    def _append_final_acceptance_updates(
        self,
        updates: ReverseSyncUpdates,
        target_headers: list[str],
        target_rows_by_key: dict[str, TargetRowSnapshot],
    ) -> ReverseSyncUpdates:
        """Добавляет в конец сценария финальную автопроверку приемки по всем строкам ОТЧЁТ_2.0.

        Бизнес-логика:
        эта проверка должна выполняться после всей основной обратной синхронизации,
        потому что она относится не к обмену полями с закупщиками, а к финальному
        определению статуса приемки внутри рабочей таблицы логистов. Проверяются
        все строки данных ОТЧЁТ_2.0, а не только строки, участвовавшие в обмене
        с Заказы белые ТЕСТ в текущем прогоне.
        """
        acceptance_lookup = self._load_supply_acceptance_lookup()
        acceptance_updates, summary = self._build_final_acceptance_updates(
            target_headers=target_headers,
            target_rows_by_key=target_rows_by_key,
            acceptance_lookup=acceptance_lookup,
        )

        if not acceptance_updates:
            logger.info(
                "Финальная автопроверка приемки завершена без изменений: checked_rows=%s matched_rows=%s duplicate_rows=%s",
                summary.checked_rows,
                summary.matched_rows,
                summary.duplicate_rows,
            )
            return updates

        target_update_map = {
            str(update["range"]): update for update in updates.target_updates
        }
        for update in acceptance_updates:
            target_update_map[str(update["range"])] = update

        logger.info(
            "Финальная автопроверка приемки подготовила изменения: checked_rows=%s matched_rows=%s accepted_rows=%s discrepancy_rows=%s duplicate_rows=%s target_updates_added=%s",
            summary.checked_rows,
            summary.matched_rows,
            summary.accepted_rows,
            summary.discrepancy_rows,
            summary.duplicate_rows,
            len(acceptance_updates),
        )
        return ReverseSyncUpdates(
            source_updates=updates.source_updates,
            target_updates=list(target_update_map.values()),
        )

    def _build_source_rows_map(
        self,
        source_values: list[list[str]],
        source_headers: list[str],
    ) -> dict[int, dict[str, object]]:
        """Строит карту номера строки к словарю значений по заголовкам."""
        rows_map: dict[int, dict[str, object]] = {}

        for row_offset, row in enumerate(source_values[SOURCE_DATA_ROW_INDEX:]):
            row_number = SOURCE_DATA_ROW_INDEX + 1 + row_offset
            normalized_row = self._normalize_row_length(row, len(source_headers))
            rows_map[row_number] = {
                header: normalized_row[index]
                for index, header in enumerate(source_headers)
            }

        return rows_map

    def _load_supply_acceptance_lookup(self) -> dict[AcceptanceKey, list[Decimal | None]]:
        """Читает из БД эталонные связки приемки и строит индекс для финальной проверки.

        Бизнес-логика:
        автоматическая приемка у логистов должна опираться на фактические данные
        прихода из БД. Для надежного совпадения используется связка из номера
        трака, номера автомобиля и wild. Количество сравнивается отдельно уже
        после совпадения ключа.
        """
        dataframe = self.database_cls.read_sql_to_dataframe(SUPPLY_ACCEPTANCE_STATUS_QUERY)
        logger.info(
            "Из БД прочитаны строки для финальной автопроверки приемки: rows=%s",
            len(dataframe.index),
        )

        acceptance_lookup: dict[AcceptanceKey, list[Decimal | None]] = {}
        for row in dataframe.to_dict(orient="records"):
            key = self._build_acceptance_key(
                truck_number=row.get("truck_number", ""),
                transport_number=row.get("transport_number", ""),
                wild=row.get("local_vendor_code", ""),
            )
            if not all(key):
                continue

            acceptance_lookup.setdefault(key, []).append(
                self._normalize_quantity_for_match(row.get("quantity", ""))
            )

        duplicate_keys = sum(1 for quantities in acceptance_lookup.values() if len(quantities) > 1)
        logger.info(
            "Подготовлен индекс приемки из БД для логистов: unique_keys=%s duplicate_keys=%s",
            len(acceptance_lookup),
            duplicate_keys,
        )
        return acceptance_lookup

    def _build_final_acceptance_updates(
        self,
        target_headers: list[str],
        target_rows_by_key: dict[str, TargetRowSnapshot],
        acceptance_lookup: dict[AcceptanceKey, list[Decimal | None]],
    ) -> tuple[list[dict[str, object]], AcceptanceStatusSummary]:
        """Готовит точечные обновления статуса приемки для всех строк ОТЧЁТ_2.0.

        Бизнес-правило:
        - если совпали ``Номер Трака + Номер ТС + wild`` и количество, ставим
          ``принят складом``;
        - если совпал ключ, но количество не совпало, ставим
          ``расхождения при приемке``;
        - если ключ не совпал или совпадение неоднозначно, текущий статус
          логистов не меняем.
        """
        target_header_map = {header: index + 1 for index, header in enumerate(target_headers)}
        update_map: dict[str, dict[str, object]] = {}
        summary = self._build_empty_acceptance_summary()

        for order_line_id, target_snapshot in target_rows_by_key.items():
            summary = AcceptanceStatusSummary(
                checked_rows=summary.checked_rows + 1,
                matched_rows=summary.matched_rows,
                accepted_rows=summary.accepted_rows,
                discrepancy_rows=summary.discrepancy_rows,
                duplicate_rows=summary.duplicate_rows,
            )
            result = self._resolve_acceptance_status(
                target_row=target_snapshot.values,
                order_line_id=order_line_id,
                acceptance_lookup=acceptance_lookup,
            )
            summary = self._accumulate_acceptance_summary(summary, result)
            if result.status is None:
                continue

            current_status = self._normalize_string(target_snapshot.values.get(STATUS_COLUMN, ""))
            if current_status == result.status:
                continue

            self._add_update(
                update_map=update_map,
                row_number=target_snapshot.row_number,
                column_number=target_header_map[STATUS_COLUMN],
                value=result.status,
            )

        return list(update_map.values()), summary

    def _resolve_acceptance_status(
        self,
        target_row: dict[str, object],
        order_line_id: str,
        acceptance_lookup: dict[AcceptanceKey, list[Decimal | None]],
    ) -> AcceptanceStatusResult:
        """Определяет итоговый статус приемки для строки логистов по данным БД.

        Метод обслуживает финальный этап логистического сценария: после всех
        обменов с закупщиками он проверяет, подтверждена ли приемка фактическими
        данными из БД. Статус меняется только при полном совпадении ключа и,
        при необходимости, количества.
        """
        acceptance_key = self._build_acceptance_key(
            truck_number=target_row.get(TRUCK_NUMBER_COLUMN, ""),
            transport_number=target_row.get(TRANSPORT_NUMBER_COLUMN, ""),
            wild=target_row.get(WILD_COLUMN, ""),
        )
        if not all(acceptance_key):
            return AcceptanceStatusResult(
                status=None,
                matched=False,
                quantity_mismatch=False,
                duplicate_matches=False,
            )

        matched_quantities = acceptance_lookup.get(acceptance_key)
        if not matched_quantities:
            return AcceptanceStatusResult(
                status=None,
                matched=False,
                quantity_mismatch=False,
                duplicate_matches=False,
            )

        if len(matched_quantities) > 1:
            logger.warning(
                "Финальная автопроверка приемки пропущена: по связке найдено несколько строк в БД. Нужна ручная проверка: truck=%s transport=%s wild=%s order_line_id=%s matches=%s",
                acceptance_key[0],
                acceptance_key[1],
                acceptance_key[2],
                order_line_id,
                len(matched_quantities),
            )
            return AcceptanceStatusResult(
                status=None,
                matched=True,
                quantity_mismatch=False,
                duplicate_matches=True,
            )

        target_quantity = self._normalize_quantity_for_match(target_row.get(ORDER_QTY_COLUMN, ""))
        database_quantity = matched_quantities[0]
        if target_quantity is None or database_quantity is None:
            logger.warning(
                "Финальная автопроверка приемки переведена в статус расхождения из-за некорректного количества: truck=%s transport=%s wild=%s order_line_id=%s target_quantity=%s database_quantity=%s",
                acceptance_key[0],
                acceptance_key[1],
                acceptance_key[2],
                order_line_id,
                target_row.get(ORDER_QTY_COLUMN, ""),
                database_quantity,
            )
            return AcceptanceStatusResult(
                status=ACCEPTANCE_DISCREPANCY_STATUS,
                matched=True,
                quantity_mismatch=True,
                duplicate_matches=False,
            )

        if target_quantity != database_quantity:
            return AcceptanceStatusResult(
                status=ACCEPTANCE_DISCREPANCY_STATUS,
                matched=True,
                quantity_mismatch=True,
                duplicate_matches=False,
            )

        return AcceptanceStatusResult(
            status=ACCEPTED_BY_WAREHOUSE_STATUS,
            matched=True,
            quantity_mismatch=False,
            duplicate_matches=False,
        )

    def _apply_updates(self, updates: ReverseSyncUpdates) -> None:
        """Применяет подготовленные изменения сначала к закупщикам, затем к логистам."""
        self._apply_sheet_updates(
            operation_name="batch_update logistic_ved_reverse source",
            worksheet=self.source_connector.sheet_title,
            updates=updates.source_updates,
        )
        self._apply_sheet_updates(
            operation_name="batch_update logistic_ved_reverse target",
            worksheet=self.target_connector.sheet_title,
            updates=updates.target_updates,
        )

    def _build_updated_at_value(self) -> str:
        """Возвращает текущее московское время в строковом формате."""
        return datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M:%S")

    def _update_last_sync_marker(self, worksheet, updated_at_value: str) -> None:
        """Записывает в B1 дату и время последней успешной обратной синхронизации."""
        self._execute_with_retry(
            operation_name="update delivery_calculation_china last sync marker",
            func=worksheet.update,
            range_name=LAST_SYNC_CELL,
            values=[[updated_at_value]],
            value_input_option="USER_ENTERED",
        )

    def _apply_sheet_updates(
        self,
        operation_name: str,
        worksheet,
        updates: list[dict[str, object]],
    ) -> None:
        """Отправляет изменения в Google Sheets пакетами фиксированного размера."""
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
    def _normalize_row_length(row: list[object], width: int) -> list[object]:
        """Обрезает или дополняет строку до нужного количества колонок."""
        normalized_row = list(row[:width])
        if len(normalized_row) < width:
            normalized_row.extend([""] * (width - len(normalized_row)))
        return normalized_row

    @staticmethod
    def _normalize_string(value: object) -> str:
        """Приводит значение к строке без внешних пробелов и скрытых переносов."""
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    @staticmethod
    def _sheet_cell_value(value: object) -> object:
        """Подготавливает значение к записи в ячейку Google Sheets."""
        return "" if value is None else value

    @staticmethod
    def _values_equal(left: object, right: object) -> bool:
        """Сравнивает значения как строки после базовой нормализации."""
        return (
            LogisticVedReverseUpdater._normalize_string(left)
            == LogisticVedReverseUpdater._normalize_string(right)
        )

    @staticmethod
    def _normalize_quantity_for_compare(value: object) -> str:
        """Нормализует количество для сверки, убирая форматные различия."""
        normalized_value = LogisticVedReverseUpdater._normalize_string(value)
        if normalized_value == "":
            return ""

        compact_value = "".join(normalized_value.split()).replace(",", ".")
        try:
            numeric_value = float(compact_value)
        except ValueError:
            return compact_value.lower()

        if numeric_value.is_integer():
            return str(int(numeric_value))
        return str(numeric_value)

    def _build_acceptance_key(
        self,
        truck_number: object,
        transport_number: object,
        wild: object,
    ) -> AcceptanceKey:
        """Собирает нормализованный ключ автоприемки из трака, автомобиля и wild."""
        return (
            self._normalize_string(truck_number),
            self._normalize_string(transport_number),
            self._normalize_string(wild),
        )

    def _normalize_quantity_for_match(self, value: object) -> Decimal | None:
        """Приводит количество к единому числовому виду для сверки с БД.

        Бизнес-логика:
        количество в ОТЧЁТ_2.0 и PostgreSQL может визуально отличаться по
        формату записи. Для решения о приемке сравнивается именно числовое
        значение, а не исходная строка.
        """
        if value is None or pd.isna(value):
            return None
        if isinstance(value, Decimal):
            return value
        if isinstance(value, int):
            return Decimal(value)
        if isinstance(value, float):
            return Decimal(str(value))

        normalized_value = self._normalize_string(value).replace("\xa0", "").replace(" ", "")
        if normalized_value == "":
            return None

        normalized_value = normalized_value.replace(",", ".")
        normalized_value = re.sub(r"[^0-9.\-]", "", normalized_value)
        if normalized_value in {"", "-", ".", "-."}:
            return None

        try:
            return Decimal(normalized_value)
        except InvalidOperation:
            logger.warning(
                "Не удалось привести количество к числу для финальной автопроверки приемки: raw_value=%s",
                value,
            )
            return None

    def _get_data_check_message(
        self,
        source_row: dict[str, object],
        target_row: dict[str, object],
    ) -> str | None:
        """Возвращает текст пометки о расхождении или None, если расхождений нет.

        Возвращаемое значение интерпретируется так:
        - ``None``: таблицы согласованы по контролируемым полям;
        - ``Сверьте wild и количество``: числовые или товарные данные расходятся;
        - ``Сверьте wild, количество и статус``: нарушено и статусное соответствие.
        """
        source_wild = self._normalize_string(source_row.get(WILD_COLUMN, "")).lower()
        target_wild = self._normalize_string(target_row.get(WILD_COLUMN, "")).lower()
        source_quantity = self._normalize_quantity_for_compare(source_row.get(ORDER_QTY_COLUMN, ""))
        target_quantity = self._normalize_quantity_for_compare(target_row.get(ORDER_QTY_COLUMN, ""))
        has_data_mismatch = source_wild != target_wild or source_quantity != target_quantity

        # Статусы сверяем не на точное равенство, а по таблице допустимых соответствий.
        source_status = self._normalize_string(source_row.get(STATUS_COLUMN, "")).lower()
        target_status = self._normalize_string(target_row.get(STATUS_COLUMN, "")).lower()
        allowed_source_statuses = ALLOWED_SOURCE_STATUSES_BY_TARGET_STATUS.get(target_status)
        has_status_mismatch = (
            allowed_source_statuses is not None
            and source_status not in allowed_source_statuses
        )

        if has_status_mismatch:
            return DATA_CHECK_STATUS_MESSAGE
        if has_data_mismatch:
            return DATA_CHECK_MESSAGE
        return None

    @staticmethod
    def _build_empty_acceptance_summary() -> AcceptanceStatusSummary:
        """Создает нулевую сводку по финальной автопроверке приемки."""
        return AcceptanceStatusSummary(
            checked_rows=0,
            matched_rows=0,
            accepted_rows=0,
            discrepancy_rows=0,
            duplicate_rows=0,
        )

    @staticmethod
    def _accumulate_acceptance_summary(
        summary: AcceptanceStatusSummary,
        result: AcceptanceStatusResult,
    ) -> AcceptanceStatusSummary:
        """Добавляет результат одной строки в общую сводку автопроверки приемки."""
        return AcceptanceStatusSummary(
            checked_rows=summary.checked_rows,
            matched_rows=summary.matched_rows + int(result.matched),
            accepted_rows=summary.accepted_rows + int(result.status == ACCEPTED_BY_WAREHOUSE_STATUS),
            discrepancy_rows=summary.discrepancy_rows
            + int(result.status == ACCEPTANCE_DISCREPANCY_STATUS),
            duplicate_rows=summary.duplicate_rows + int(result.duplicate_matches),
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

                retry_delay_seconds = self._get_retry_delay_seconds(error, attempt)
                logger.warning(
                    "Временная ошибка Google Sheets, повторяем попытку: operation=%s attempt=%s/%s delay_seconds=%s error=%s",
                    operation_name,
                    attempt,
                    GOOGLE_WRITE_RETRY_ATTEMPTS,
                    retry_delay_seconds,
                    error,
                )
                sleep(retry_delay_seconds)

        raise RuntimeError(f"Не удалось завершить операцию {operation_name}.")

    @staticmethod
    def _is_retryable_google_error(error: gspread.exceptions.APIError) -> bool:
        """Проверяет, можно ли безопасно повторить запрос после ошибки Google Sheets."""
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
        """Возвращает паузу перед повторной попыткой запроса."""
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code == 429:
            retry_delays = (15, 30, 45, 60)
            return retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        return GOOGLE_WRITE_RETRY_DELAY_SECONDS


def logistic_ved_reverse_run() -> None:
    """Точка входа для обратной синхронизации логистических данных."""

    setup_logger()
    LogisticVedReverseUpdater().run()


if __name__ == "__main__":
    logistic_ved_reverse_run()
