from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from time import sleep

import gspread
import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.purchase_price_update.config import (
    ARTICLE_COLUMN,
    DEFAULT_BATCH_SIZE,
    DEFAULT_LOOKBACK_DAYS,
    GOOGLE_WRITE_RETRY_ATTEMPTS,
    GOOGLE_WRITE_RETRY_DELAY_SECONDS,
    GOOGLE_WRITE_RETRY_STATUS_CODES,
    LOCAL_REPORT_PATH,
    PURCHASE_PRICE_COLUMN,
    REPORT_SHEET_CONFIG,
    UNIT_SHEET_CONFIG,
)
from src_oop.jobs.purchase_price_update.processor import (
    DuplicateBusinessKeyError,
    ProcessingResult,
    build_report_dataframe,
    build_unit_sheet_dataframe,
    prepare_purchase_price_updates,
    prepare_unit_state,
)
from src_oop.jobs.purchase_price_update.repository import PurchasePriceUpdateRepository

logger = logging.getLogger(__name__)


class NoPurchasePriceChangesError(ValueError):
    """Нет строк, которые требуется обновить."""


class RowAlignmentError(RuntimeError):
    """Строка в Google Sheets не совпала с рассчитанным business key."""


@dataclass(frozen=True, slots=True)
class PurchasePriceUpdateSummary:
    """
    Краткая сводка по результату выполнения job.

    Назначение:
    вернуть наружу компактный итог выполнения без необходимости разбирать логи.

    Поля:
    `db_rows` — сколько строк пришло из БД.
    `prepared_rows` — сколько строк осталось после подготовки и merge с UNIT.
    `updated_rows` — сколько строк реально отправлено на обновление.
    `suspicious_rows` — сколько строк попало в предупреждение по аномальному изменению цены.
    `locked_rows` — сколько SKU исключено как неизменяемые.
    `absent_in_unit_rows` — сколько SKU не найдено в UNIT.
    `missing_price_rows` — сколько SKU пропущено из-за отсутствующей цены.
    """

    db_rows: int
    prepared_rows: int
    updated_rows: int
    suspicious_rows: int
    locked_rows: int
    absent_in_unit_rows: int
    missing_price_rows: int


@dataclass(slots=True)
class PurchasePriceUpdateService:
    """
    Оркестратор боевой задачи обновления закупочных цен.

    Сервис отвечает за:
    1. чтение данных из PostgreSQL и Google Sheets;
    2. вызов processor для расчета списка изменений;
    3. безопасную запись обновлений в Google Sheets;
    4. формирование локального и удаленного отчета;
    5. подробное логирование всего жизненного цикла задачи.
    """

    repository: PurchasePriceUpdateRepository
    local_report_path: Path = LOCAL_REPORT_PATH
    batch_size: int = DEFAULT_BATCH_SIZE

    def run(
        self,
        round_price: bool = True,
        days_count: int = DEFAULT_LOOKBACK_DAYS,
    ) -> PurchasePriceUpdateSummary:
        """
        Выполняет полный боевой сценарий обновления закупочных цен.

        Назначение:
        координирует чтение данных, подготовку изменений, запись в `Сопост`,
        формирование отчета и выпуск итоговой сводки.

        Параметры:
        `round_price` — применять ли legacy-совместимое округление новой цены.
        `days_count` — размер окна выборки по данным БД в днях.

        Возвращаемый результат:
        `PurchasePriceUpdateSummary` с итоговой статистикой выполнения.

        Возможные исключения:
        `NoPurchasePriceChangesError`, если обновлять нечего.
        `DuplicateBusinessKeyError`, если структура данных делает обновление небезопасным.
        `RowAlignmentError`, если перед записью не сошелся `wild` в целевой строке.
        Исключения Google API или SQLAlchemy, если внешние сервисы недоступны.

        Особенности поведения:
        задача сначала полностью готовит итоговый набор изменений и только потом
        переходит к записи в Google Sheets. Это снижает риск частично рассчитанного результата.
        """

        logger.info(
            "Старт задачи purchase_price_update: round_price=%s days_count=%s",
            round_price,
            days_count,
        )
        db_dataframe = self.repository.fetch_latest_purchase_prices(days_count=days_count)

        logger.info(
            "Подключаемся к листу-источнику UNIT: table=%s sheet=%s",
            UNIT_SHEET_CONFIG.table_title,
            UNIT_SHEET_CONFIG.sheet_title,
        )
        unit_connector = GoogleTabs(
            table_title=UNIT_SHEET_CONFIG.table_title,
            sheet_title=UNIT_SHEET_CONFIG.sheet_title,
        )
        unit_values = unit_connector.sheet_title.get_all_values()
        logger.info(
            "Лист Сопост успешно прочитан: total_rows=%s",
            len(unit_values),
        )

        unit_dataframe = build_unit_sheet_dataframe(
            values=unit_values,
            header_row_index=UNIT_SHEET_CONFIG.header_row_index,
            data_row_index=UNIT_SHEET_CONFIG.data_row_index,
        )
        unit_state = prepare_unit_state(unit_dataframe)
        processing_result = prepare_purchase_price_updates(
            db_dataframe=db_dataframe,
            unit_state=unit_state,
            round_price=round_price,
        )

        self._log_processing_diagnostics(processing_result)
        if processing_result.changed_rows.empty:
            self._update_report_status(
                message="Не найдены SKU с изменённой ценой",
            )
            raise NoPurchasePriceChangesError(
                "Не найдены SKU с изменённой ценой."
            )

        self._apply_purchase_price_updates(
            connector=unit_connector,
            processing_result=processing_result,
            cached_values=unit_values,
        )

        report_dataframe = build_report_dataframe(processing_result.changed_rows)
        self._save_local_report(report_dataframe)
        self._append_report_dataframe(report_dataframe)

        summary = PurchasePriceUpdateSummary(
            db_rows=len(db_dataframe.index),
            prepared_rows=len(processing_result.prepared_rows.index),
            updated_rows=len(processing_result.changed_rows.index),
            suspicious_rows=len(processing_result.suspicious_rows.index),
            locked_rows=len(processing_result.excluded_locked_codes),
            absent_in_unit_rows=len(processing_result.absent_in_unit_codes),
            missing_price_rows=len(processing_result.missing_price_codes),
        )
        logger.info(
            "Задача purchase_price_update завершена успешно: db_rows=%s prepared_rows=%s updated_rows=%s suspicious_rows=%s locked_rows=%s absent_in_unit_rows=%s missing_price_rows=%s",
            summary.db_rows,
            summary.prepared_rows,
            summary.updated_rows,
            summary.suspicious_rows,
            summary.locked_rows,
            summary.absent_in_unit_rows,
            summary.missing_price_rows,
        )
        return summary

    def _apply_purchase_price_updates(
        self,
        connector: GoogleTabs,
        processing_result: ProcessingResult,
        cached_values: list[list[str]],
    ) -> None:
        """
        Выполняет пакетное обновление закупочных цен в листе `Сопост`.

        Перед записью повторно сверяем `wild` в целевой строке.
        Это защищает от сценария, когда строки в таблице были сдвинуты вручную.
        """

        worksheet = connector.sheet_title
        headers = cached_values[UNIT_SHEET_CONFIG.header_row_index]
        if ARTICLE_COLUMN not in headers or PURCHASE_PRICE_COLUMN not in headers:
            raise DuplicateBusinessKeyError(
                "В листе Сопост не найдены колонки для обновления закупочной цены."
            )

        article_column_index = headers.index(ARTICLE_COLUMN) + 1
        price_column_index = headers.index(PURCHASE_PRICE_COLUMN) + 1

        updates: list[dict[str, object]] = []
        for row in processing_result.changed_rows.itertuples(index=False):
            # Используем безопасное имя служебной колонки без двойного подчёркивания.
            # Иначе Python внутри метода класса применяет name mangling, и доступ
            # к полю namedtuple ломается уже на боевом обновлении строк.
            row_number = int(row.sheet_row_number)
            cached_row = cached_values[row_number - 1] if row_number - 1 < len(cached_values) else []
            actual_article = ""
            if article_column_index - 1 < len(cached_row):
                actual_article = str(cached_row[article_column_index - 1]).strip()

            if actual_article != row.local_vendor_code:
                raise RowAlignmentError(
                    "Проверка business key перед записью не прошла: "
                    f"ожидался {row.local_vendor_code}, найден {actual_article or '<empty>'} "
                    f"в строке {row_number}."
                )

            target_cell = rowcol_to_a1(row_number, price_column_index)
            updates.append(
                {
                    "range": target_cell,
                    "values": [[row.price_per_item]],
                }
            )

        logger.info(
            "Подготовлены изменения для записи в Сопост: rows_to_update=%s batch_size=%s",
            len(updates),
            self.batch_size,
        )
        for start_index in range(0, len(updates), self.batch_size):
            chunk = updates[start_index:start_index + self.batch_size]
            self._execute_with_retry(
                operation_name="batch_update закупочных цен",
                func=worksheet.batch_update,
                data=chunk,
                value_input_option="USER_ENTERED",
            )
            logger.info(
                "Пакет обновления закупочных цен записан в Google Sheets: batch_start=%s batch_size=%s",
                start_index,
                len(chunk),
            )

    def _append_report_dataframe(self, dataframe: pd.DataFrame) -> None:
        """Добавляет строки отчета в лист истории изменений закупочной цены."""

        logger.info(
            "Подключаемся к листу отчета: table=%s sheet=%s",
            REPORT_SHEET_CONFIG.table_title,
            REPORT_SHEET_CONFIG.sheet_title,
        )
        report_connector = GoogleTabs(
            table_title=REPORT_SHEET_CONFIG.table_title,
            sheet_title=REPORT_SHEET_CONFIG.sheet_title,
        )
        worksheet = report_connector.sheet_title
        rows = dataframe.values.tolist()
        if rows:
            self._execute_with_retry(
                operation_name="append_rows отчета об изменении цен",
                func=worksheet.append_rows,
                values=rows,
                value_input_option="USER_ENTERED",
            )
        self._update_report_status(
            message="Обновлено",
            worksheet=worksheet,
        )
        logger.info(
            "Отчет об изменении закупочных цен записан в Google Sheets: rows=%s",
            len(rows),
        )

    def _update_report_status(self, message: str, worksheet=None) -> None:
        """Обновляет статус в ячейке A1 отчетного листа."""

        report_worksheet = worksheet
        if report_worksheet is None:
            report_connector = GoogleTabs(
                table_title=REPORT_SHEET_CONFIG.table_title,
                sheet_title=REPORT_SHEET_CONFIG.sheet_title,
            )
            report_worksheet = report_connector.sheet_title

        self._execute_with_retry(
            operation_name="update статуса отчета",
            func=report_worksheet.update,
            range_name="A1",
            values=[[f"{message}: {pd.Timestamp.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
            value_input_option="USER_ENTERED",
        )
        logger.info("Статус отчетного листа обновлен: message=%s", message)

    def _save_local_report(self, dataframe: pd.DataFrame) -> None:
        """Сохраняет локальный CSV snapshot для последующего разбора инцидентов."""

        self.local_report_path.parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_csv(self.local_report_path, index=False, encoding="utf-8")
        logger.info(
            "Локальный snapshot изменений закупочных цен сохранен: path=%s rows=%s",
            self.local_report_path,
            len(dataframe.index),
        )

    @staticmethod
    def _log_processing_diagnostics(processing_result: ProcessingResult) -> None:
        """Пишет в лог ключевую диагностику после подготовки данных."""

        logger.info(
            "Подготовка данных завершена: prepared_rows=%s changed_rows=%s suspicious_rows=%s",
            len(processing_result.prepared_rows.index),
            len(processing_result.changed_rows.index),
            len(processing_result.suspicious_rows.index),
        )
        if processing_result.excluded_locked_codes:
            logger.info(
                "Исключены SKU с неизменяемой ценой: count=%s codes=%s",
                len(processing_result.excluded_locked_codes),
                processing_result.excluded_locked_codes,
            )
        if processing_result.absent_in_unit_codes:
            logger.warning(
                "SKU есть в БД, но отсутствуют в UNIT: count=%s codes=%s",
                len(processing_result.absent_in_unit_codes),
                processing_result.absent_in_unit_codes,
            )
        if processing_result.missing_price_codes:
            logger.warning(
                "SKU пропущены из-за отсутствующей закупочной цены: count=%s codes=%s",
                len(processing_result.missing_price_codes),
                processing_result.missing_price_codes,
            )
        if not processing_result.suspicious_rows.empty:
            logger.warning(
                "Найдены подозрительные изменения цены >= 25%%: count=%s preview=%s",
                len(processing_result.suspicious_rows.index),
                processing_result.suspicious_rows[
                    ["local_vendor_code", "price_per_item", "unit_price", "price_diff_percent"]
                ].head(10).to_dict(orient="records"),
            )

    @staticmethod
    def _execute_with_retry(operation_name: str, func, *args, **kwargs):
        """
        Выполняет запись в Google Sheets с ограниченным количеством повторов.

        Назначение:
        централизовать retry-поведение для всех операций записи, чтобы
        временные ошибки Google API не валили задачу с первой попытки.

        Параметры:
        `operation_name` — человекочитаемое имя операции для логов.
        `func` — вызываемая функция записи в Google Sheets.
        `*args`, `**kwargs` — аргументы, которые будут переданы в `func`.

        Возвращаемый результат:
        результат вызова `func`, если операция завершилась успешно.

        Возможные исключения:
        пробрасывает `gspread.exceptions.APIError`, если ошибка неретрайбл
        или если все попытки исчерпаны.

        Особенности поведения:
        повторяются только временные ошибки API из белого списка статусов.

        Повторяем только временные ошибки API и только небольшое число раз.
        Это делает задачу устойчивее в боевых условиях, но не скрывает реальные ошибки.
        """

        for attempt in range(1, GOOGLE_WRITE_RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as error:
                if not PurchasePriceUpdateService._is_retryable_google_error(error):
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
                sleep(GOOGLE_WRITE_RETRY_DELAY_SECONDS)

    @staticmethod
    def _is_retryable_google_error(error: gspread.exceptions.APIError) -> bool:
        """
        Определяет, можно ли безопасно повторить операцию после ошибки Google API.

        Назначение:
        отделить временные ошибки внешнего сервиса от логических и постоянных ошибок,
        которые нельзя исправить повторной попыткой.

        Параметры:
        `error` — исключение, полученное от `gspread`.

        Возвращаемый результат:
        `True`, если ошибку можно повторить безопасно, иначе `False`.
        """

        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            return status_code in GOOGLE_WRITE_RETRY_STATUS_CODES

        error_text = str(error)
        return any(f"[{code}]" in error_text for code in GOOGLE_WRITE_RETRY_STATUS_CODES)
