"""Запись витрин баланса и начислений продавцов в Google Sheets."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import gspread
import pandas as pd
import requests
from gspread.utils import rowcol_to_a1

from src_oop.jobs.seller_balance.config import (
    COLUMN_RENAME_MAP,
    CREDS_FILE,
    FINANCIAL_REPORTS_COLUMN_COUNT,
    FINANCIAL_REPORTS_COLUMN_RENAME_MAP,
    FINANCIAL_REPORTS_COLUMNS,
    GOOGLE_WRITE_RETRY_ATTEMPTS,
    GOOGLE_WRITE_RETRY_STATUS_CODES,
    PRIORITY_COLUMNS,
    SHEET_CONFIG,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SellerBalanceSaveResult:
    """Хранит итог публикации баланса и начислений в Google Sheets."""

    written_rows: int
    financial_rows: int = 0


class SellerBalanceRepository:
    """Публикует два независимых блока данных во вкладку `Переменные`."""

    def save(
        self,
        rows: list[dict[str, object]],
        financial_dataframe: pd.DataFrame | None = None,
    ) -> SellerBalanceSaveResult:
        """Обновляет баланс WB и начисления одним batch-запросом Google Sheets.

        Бизнес-сценарий:
        job сначала получает оба набора данных, затем публикует их атомарно
        относительно одного чтения листа. Блок баланса занимает `F:J`, а
        агрегированные начисления занимают `L:N`; остальные колонки не меняются.
        """
        dataframe = self._build_dataframe(rows)
        if financial_dataframe is None:
            financial_dataframe = pd.DataFrame(columns=list(FINANCIAL_REPORTS_COLUMNS))
        financial_dataframe = self._build_financial_reports_dataframe(
            financial_dataframe,
        )
        worksheet = self._open_worksheet()
        self._update_data_blocks_in_google(
            balance_dataframe=dataframe,
            financial_dataframe=financial_dataframe,
            worksheet=worksheet,
        )
        logger.info(
            "Витрины баланса и начислений обновлены в Google Sheets | spreadsheet_id=%s | sheet=%s | balance_rows=%s | financial_rows=%s",
            SHEET_CONFIG.spreadsheet_id,
            SHEET_CONFIG.sheet_title,
            len(dataframe.index),
            len(financial_dataframe.index),
        )
        return SellerBalanceSaveResult(
            written_rows=len(dataframe.index),
            financial_rows=len(financial_dataframe.index),
        )

    def _build_dataframe(self, rows: list[dict[str, object]]) -> pd.DataFrame:
        """Преобразует ответы WB API в фиксированный блок `F:J`.

        Бизнес-сценарий:
        бизнесу нужна компактная витрина по кабинетам с русскими заголовками,
        отметкой времени выгрузки и стабильным порядком пяти колонок баланса.
        """
        dataframe = pd.DataFrame(rows)
        if dataframe.empty:
            dataframe = pd.DataFrame(columns=list(COLUMN_RENAME_MAP))

        dataframe = dataframe.copy()
        dataframe["updated_at_export"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S",
        )
        dataframe = dataframe.rename(columns=COLUMN_RENAME_MAP)
        final_dataframe = dataframe.loc[:, list(PRIORITY_COLUMNS)].copy()

        if "Аккаунт" in final_dataframe.columns:
            final_dataframe = final_dataframe.sort_values(
                by="Аккаунт",
                kind="stable",
            ).reset_index(drop=True)

        return final_dataframe

    def _build_financial_reports_dataframe(
        self,
        dataframe: pd.DataFrame,
    ) -> pd.DataFrame:
        """Готовит результат PostgreSQL для фиксированного блока `L:N`.

        Бизнес-сценарий:
        агрегированные начисления должны быть пригодны для формул ДДС:
        дата приводится к ISO-формату, аккаунт нормализуется, а заголовки
        становятся русскими и всегда сохраняются даже при пустом результате.
        """
        missing_columns = set(FINANCIAL_REPORTS_COLUMNS) - set(dataframe.columns)
        if missing_columns:
            missing_columns_text = ", ".join(sorted(missing_columns))
            raise ValueError(
                "В результате PostgreSQL отсутствуют колонки: "
                f"{missing_columns_text}",
            )

        prepared_dataframe = dataframe.loc[:, list(FINANCIAL_REPORTS_COLUMNS)].copy()
        prepared_dataframe = prepared_dataframe.rename(
            columns=FINANCIAL_REPORTS_COLUMN_RENAME_MAP,
        )
        prepared_dataframe["Дата"] = pd.to_datetime(
            prepared_dataframe["Дата"],
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")
        prepared_dataframe["Аккаунт"] = (
            prepared_dataframe["Аккаунт"]
            .astype("string")
            .str.strip()
            .str.upper()
        )
        prepared_dataframe = prepared_dataframe.sort_values(
            by=["Дата", "Аккаунт"],
            kind="stable",
        ).reset_index(drop=True)
        return prepared_dataframe

    def _open_worksheet(self) -> gspread.Worksheet:
        """Открывает вкладку ДДС по идентификатору Google-таблицы.

        Бизнес-сценарий:
        таблица открывается по `spreadsheet_id`, потому что название документа
        может быть неуникальным, а job должна писать в строго заданный документ.
        """
        client = gspread.service_account(filename=str(CREDS_FILE))
        spreadsheet = client.open_by_key(SHEET_CONFIG.spreadsheet_id)
        return spreadsheet.worksheet(SHEET_CONFIG.sheet_title)

    def _update_data_blocks_in_google(
        self,
        balance_dataframe: pd.DataFrame,
        financial_dataframe: pd.DataFrame,
        worksheet: gspread.Worksheet,
    ) -> None:
        """Обновляет `F:J` и `L:N` одним batch-запросом.

        Бизнес-сценарий:
        один вызов `get_all_values` определяет старую высоту каждого блока,
        после чего каждый блок очищается только в собственных колонках. Это
        сохраняет справочные и соседние данные на вкладке `Переменные`.
        """
        old_values = worksheet.get_all_values()
        updates = [
            self._build_block_update(
                dataframe=balance_dataframe,
                old_values=old_values,
                start_column_index=SHEET_CONFIG.start_column_index,
                column_count=len(PRIORITY_COLUMNS),
            ),
            self._build_block_update(
                dataframe=financial_dataframe,
                old_values=old_values,
                start_column_index=SHEET_CONFIG.financial_reports_start_column_index,
                column_count=FINANCIAL_REPORTS_COLUMN_COUNT,
            ),
        ]
        self._execute_google_write_with_retry(
            operation_name=(
                f"batch_update {worksheet.title} "
                f"{updates[0]['range']}, {updates[1]['range']}"
            ),
            func=worksheet.batch_update,
            data=updates,
            value_input_option="USER_ENTERED",
        )

    def _build_block_update(
        self,
        dataframe: pd.DataFrame,
        old_values: list[list[str]],
        start_column_index: int,
        column_count: int,
    ) -> dict[str, Any]:
        """Строит payload одного целевого блока Google Sheets.

        Бизнес-сценарий:
        заголовок должен оставаться на первой строке, а уменьшившийся новый
        набор строк должен затирать только хвост прежнего блока.
        """
        data_values = self._dataframe_to_values(dataframe, column_count)
        old_block_rows = self._get_existing_block_rows(
            old_values=old_values,
            start_column_index=start_column_index,
            column_count=column_count,
        )
        target_rows = max(len(data_values), old_block_rows, 1)
        values = [["" for _ in range(column_count)] for _ in range(target_rows)]
        for row_index, row in enumerate(data_values):
            values[row_index] = [self._sheet_update_cell(value) for value in row]

        return {
            "range": self._build_target_range(
                start_column_index=start_column_index,
                target_rows=target_rows,
                target_cols=column_count,
            ),
            "values": values,
        }

    def _dataframe_to_values(
        self,
        dataframe: pd.DataFrame,
        column_count: int,
    ) -> list[list[object]]:
        """Преобразует DataFrame в заголовок и строки для одного блока.

        Бизнес-сценарий:
        фиксированная ширина блоков защищает соседние колонки листа от
        случайной записи при изменении схемы входных данных.
        """
        if len(dataframe.columns) != column_count:
            raise ValueError(
                "Количество колонок выгрузки не совпадает с конфигурацией блока",
            )

        dataframe_to_upload = dataframe.astype(object)
        dataframe_to_upload = dataframe_to_upload.where(
            pd.notnull(dataframe_to_upload),
            "",
        )
        return [
            dataframe_to_upload.columns.astype(str).tolist(),
            *dataframe_to_upload.values.tolist(),
        ]

    def _get_existing_block_rows(
        self,
        old_values: list[list[str]],
        start_column_index: int,
        column_count: int,
    ) -> int:
        """Определяет высоту старого блока только по его собственным колонкам.

        Бизнес-сценарий:
        данные в `A:E`, `K` и других соседних колонках не должны заставлять job
        очищать или расширять целевой блок.
        """
        block_start = start_column_index - 1
        block_end = block_start + column_count
        last_row = 0
        for row_index, row in enumerate(old_values, start=1):
            block_values = row[block_start:block_end]
            if any(value not in ("", None) for value in block_values):
                last_row = row_index
        return last_row

    def _build_target_range(
        self,
        start_column_index: int,
        target_rows: int,
        target_cols: int,
    ) -> str:
        """Строит A1-диапазон одного целевого блока выгрузки.

        Бизнес-сценарий:
        точный A1-диапазон ограничивает запись только блоком баланса `F:J`
        или начислений `L:N`, не затрагивая справочные колонки листа.
        """
        start_cell = rowcol_to_a1(1, start_column_index)
        end_cell = rowcol_to_a1(target_rows, start_column_index + target_cols - 1)
        return f"{start_cell}:{end_cell}"

    def _execute_google_write_with_retry(
        self,
        operation_name: str,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Выполняет batch-запись в Google Sheets с retry временных ошибок.

        Бизнес-сценарий:
        кратковременный сбой Google или квотный `429` не должен оставлять
        актуальные данные WB и PostgreSQL без публикации.
        """
        for attempt in range(1, GOOGLE_WRITE_RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as error:
                if not self._is_retryable_google_error(error):
                    logger.error(
                        "Операция записи в Google Sheets завершилась постоянной ошибкой | operation=%s | attempt=%s | error_type=%s",
                        operation_name,
                        attempt,
                        type(error).__name__,
                    )
                    raise

                if attempt == GOOGLE_WRITE_RETRY_ATTEMPTS:
                    logger.error(
                        "Операция записи в Google Sheets исчерпала все попытки retry | operation=%s | attempts=%s | error_type=%s",
                        operation_name,
                        GOOGLE_WRITE_RETRY_ATTEMPTS,
                        type(error).__name__,
                    )
                    raise

                wait_seconds = self._get_google_retry_delay_seconds(error, attempt)
                status_code = self._get_google_error_status_code(error)
                logger.warning(
                    "Google Sheets временно не принял запись, повторяем попытку | operation=%s | status_code=%s | attempt=%s/%s | wait_seconds=%s",
                    operation_name,
                    status_code,
                    attempt,
                    GOOGLE_WRITE_RETRY_ATTEMPTS,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
            except requests.exceptions.RequestException as error:
                if attempt == GOOGLE_WRITE_RETRY_ATTEMPTS:
                    logger.error(
                        "Операция записи в Google Sheets исчерпала все попытки после сетевой ошибки | operation=%s | attempts=%s | error_type=%s",
                        operation_name,
                        GOOGLE_WRITE_RETRY_ATTEMPTS,
                        type(error).__name__,
                    )
                    raise

                wait_seconds = self._get_google_network_retry_delay_seconds(attempt)
                logger.warning(
                    "Сетевая ошибка при записи в Google Sheets, повторяем попытку | operation=%s | attempt=%s/%s | wait_seconds=%s | error_type=%s",
                    operation_name,
                    attempt,
                    GOOGLE_WRITE_RETRY_ATTEMPTS,
                    wait_seconds,
                    type(error).__name__,
                )
                time.sleep(wait_seconds)

        raise RuntimeError(
            "Не удалось завершить операцию записи в Google Sheets: "
            f"{operation_name}",
        )

    def _is_retryable_google_error(self, error: gspread.exceptions.APIError) -> bool:
        """Проверяет, относится ли ошибка Google Sheets к временным.

        Бизнес-сценарий:
        повторяются только квотные и временные ошибки, а проблемы прав доступа
        или структуры листа сразу возвращаются оператору без скрытого retry.
        """
        status_code = self._get_google_error_status_code(error)
        if status_code is not None:
            return status_code in GOOGLE_WRITE_RETRY_STATUS_CODES

        error_text = str(error)
        return any(f"[{code}]" in error_text for code in GOOGLE_WRITE_RETRY_STATUS_CODES)

    def _get_google_retry_delay_seconds(
        self,
        error: gspread.exceptions.APIError,
        attempt: int,
    ) -> int:
        """Считает паузу перед повторной записью после API-ошибки Google Sheets.

        Бизнес-сценарий:
        для `429` используется длинный backoff, чтобы запись не усиливала
        ограничение квоты Google Sheets.
        """
        status_code = self._get_google_error_status_code(error)
        if status_code == 429:
            retry_delays = (15, 30, 45, 60)
            return retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        return self._get_google_network_retry_delay_seconds(attempt)

    def _sheet_update_cell(self, value: object) -> object:
        """Нормализует значение ячейки перед записью в Google Sheets.

        Бизнес-сценарий:
        финансовые формулы не должны получать `NaN`, бесконечности или сырые
        объекты дат, поэтому пропуски очищаются, а даты приводятся к строкам.
        """
        if value is None:
            return ""

        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")

        if isinstance(value, date):
            return value.isoformat()

        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return ""
            return value

        return "" if pd.isna(value) else value

    @staticmethod
    def _get_google_error_status_code(
        error: gspread.exceptions.APIError,
    ) -> int | None:
        """Извлекает HTTP-статус ошибки Google Sheets для backoff.

        Бизнес-сценарий:
        статус позволяет отличить `429` от прочих временных ответов и выбрать
        подходящую задержку перед повтором публикации ДДС.
        """
        response = getattr(error, "response", None)
        return getattr(response, "status_code", None)

    @staticmethod
    def _get_google_network_retry_delay_seconds(attempt: int) -> int:
        """Возвращает паузу для повторной записи после сетевого сбоя.

        Бизнес-сценарий:
        короткий нарастающий backoff даёт Google Sheets время восстановить
        соединение и не задерживает job так долго, как квотный backoff.
        """
        retry_delays = (5, 10, 20, 30)
        return retry_delays[min(attempt - 1, len(retry_delays) - 1)]
