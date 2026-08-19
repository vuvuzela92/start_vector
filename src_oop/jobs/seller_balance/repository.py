"""Запись витрины баланса продавцов в Google Sheets."""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime

import gspread
import pandas as pd
import requests
from gspread.utils import rowcol_to_a1

from src_oop.jobs.seller_balance.config import (
    COLUMN_RENAME_MAP,
    CREDS_FILE,
    GOOGLE_WRITE_RETRY_ATTEMPTS,
    GOOGLE_WRITE_RETRY_STATUS_CODES,
    PRIORITY_COLUMNS,
    SHEET_CONFIG,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SellerBalanceSaveResult:
    """Хранит итог публикации витрины баланса в Google Sheets.

    Бизнес-сценарий:
    сервису нужен компактный итог записи, чтобы в финальном логе было видно,
    сколько кабинетов реально попало во вкладку `Переменные.`.
    """

    written_rows: int


class SellerBalanceRepository:
    """Публикует актуальный баланс продавцов во вкладку `Переменные.`.

    Бизнес-сценарий:
    финансовая команда читает таблицу ДДС как витрину текущего состояния, а не
    как журнал событий, поэтому обновляется только целевой блок листа с
    актуальным срезом по всем кабинетам.
    """

    def save(self, rows: list[dict[str, object]]) -> SellerBalanceSaveResult:
        """Подготавливает витрину и публикует её в Google Sheets по `spreadsheet_id`.

        Бизнес-сценарий:
        метод завершает основной сценарий job: превращает ответы WB API в
        понятную финансовую таблицу и обновляет лист для бизнес-пользователей.
        """
        dataframe = self._build_dataframe(rows)
        worksheet = self._open_worksheet()
        self._update_df_in_google(df=dataframe, worksheet=worksheet)
        logger.info(
            "Витрина баланса продавцов обновлена в Google Sheets | spreadsheet_id=%s | sheet=%s | rows=%s | start_column=%s",
            SHEET_CONFIG.spreadsheet_id,
            SHEET_CONFIG.sheet_title,
            len(dataframe.index),
            SHEET_CONFIG.start_column_index,
        )
        return SellerBalanceSaveResult(written_rows=len(dataframe.index))

    def _build_dataframe(self, rows: list[dict[str, object]]) -> pd.DataFrame:
        """Преобразует ответы WB API в DataFrame для листа `Переменные.`.

        Бизнес-сценарий:
        бизнесу нужна компактная витрина по кабинетам с понятными русскими
        заголовками и отметкой времени последнего обновления.
        """
        dataframe = pd.DataFrame(rows)
        if dataframe.empty:
            dataframe = pd.DataFrame(columns=list(COLUMN_RENAME_MAP))

        dataframe = dataframe.copy()
        dataframe["updated_at_export"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dataframe = dataframe.rename(columns=COLUMN_RENAME_MAP)

        ordered_columns = [column for column in PRIORITY_COLUMNS if column in dataframe.columns]
        remaining_columns = [column for column in dataframe.columns if column not in ordered_columns]
        final_dataframe = dataframe.loc[:, [*ordered_columns, *remaining_columns]].copy()

        if "Аккаунт" in final_dataframe.columns:
            final_dataframe = final_dataframe.sort_values(
                by="Аккаунт",
                kind="stable",
            ).reset_index(drop=True)

        return final_dataframe

    def _open_worksheet(self) -> gspread.Worksheet:
        """Открывает рабочую вкладку ДДС по `spreadsheet_id`.

        Бизнес-сценарий:
        job намеренно открывает таблицу по идентификатору, потому что в проекте
        встречаются таблицы с неуникальными названиями, а витрина ДДС должна
        обновляться строго в одном известном документе.
        """
        client = gspread.service_account(filename=str(CREDS_FILE))
        spreadsheet = client.open_by_key(SHEET_CONFIG.spreadsheet_id)
        return spreadsheet.worksheet(SHEET_CONFIG.sheet_title)

    def _update_df_in_google(self, df: pd.DataFrame, worksheet: gspread.Worksheet) -> None:
        """Перезаписывает целевой блок листа, начиная с заданной колонки.

        Бизнес-сценарий:
        выгрузка должна попадать во вкладку `Переменные.` начиная с `F1`, не
        затрагивая данные слева. При этом старые значения внутри блока справа
        нужно очищать, если строк или колонок в новой витрине стало меньше.
        """
        old_values = worksheet.get_all_values()
        old_rows = len(old_values)
        old_cols = max((len(row) for row in old_values), default=0)
        start_column_index = SHEET_CONFIG.start_column_index

        df_to_upload = df.copy()
        df_to_upload = df_to_upload.astype(object)
        df_to_upload = df_to_upload.where(pd.notnull(df_to_upload), "")

        data_values: list[list[object]] = []
        if len(df_to_upload.columns) > 0:
            data_values = [
                df_to_upload.columns.astype(str).tolist(),
                *df_to_upload.values.tolist(),
            ]

        new_rows = len(data_values)
        new_cols = len(data_values[0]) if data_values else 0
        target_rows = max(old_rows, new_rows)
        old_target_cols = max(old_cols - start_column_index + 1, 0)
        target_cols = max(old_target_cols, new_cols)

        if target_rows == 0 or target_cols == 0:
            logger.info("Обновление Google Sheets пропущено: старых и новых данных нет.")
            return

        values = [["" for _ in range(target_cols)] for _ in range(target_rows)]
        for row_idx, row in enumerate(data_values):
            for col_idx, value in enumerate(row):
                values[row_idx][col_idx] = self._sheet_update_cell(value)

        target_range = self._build_target_range(
            start_column_index=start_column_index,
            target_rows=target_rows,
            target_cols=target_cols,
        )
        self._execute_google_write_with_retry(
            operation_name=f"update_range {worksheet.title} {target_range}",
            func=worksheet.update,
            range_name=target_range,
            values=values,
            value_input_option="USER_ENTERED",
        )

    def _build_target_range(
        self,
        start_column_index: int,
        target_rows: int,
        target_cols: int,
    ) -> str:
        """Строит A1-диапазон записи для блока витрины баланса.

        Бизнес-сценарий:
        выгрузка должна начинаться с фиксированной ячейки `F1`, чтобы левая
        часть вкладки `Переменные.` оставалась под другие финансовые данные.
        """
        start_cell = rowcol_to_a1(1, start_column_index)
        end_cell = rowcol_to_a1(target_rows, start_column_index + target_cols - 1)
        return f"{start_cell}:{end_cell}"

    def _execute_google_write_with_retry(self, operation_name: str, func, *args, **kwargs):
        """Выполняет запись в Google Sheets с retry при временных ошибках.

        Бизнес-сценарий:
        финансовая витрина не должна падать на первом временном ответе Google,
        иначе вкладка ДДС останется без обновления даже при кратком сбое API.
        """
        for attempt in range(1, GOOGLE_WRITE_RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as error:
                if not self._is_retryable_google_error(error):
                    logger.exception(
                        "Операция записи в Google Sheets завершилась неретрайбл-ошибкой | operation=%s | attempt=%s",
                        operation_name,
                        attempt,
                    )
                    raise

                if attempt == GOOGLE_WRITE_RETRY_ATTEMPTS:
                    logger.exception(
                        "Операция записи в Google Sheets исчерпала все попытки retry | operation=%s | attempts=%s",
                        operation_name,
                        GOOGLE_WRITE_RETRY_ATTEMPTS,
                    )
                    raise

                wait_seconds = self._get_google_retry_delay_seconds(error=error, attempt=attempt)
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
                    logger.exception(
                        "Операция записи в Google Sheets исчерпала все попытки после сетевой ошибки | operation=%s | attempts=%s",
                        operation_name,
                        GOOGLE_WRITE_RETRY_ATTEMPTS,
                    )
                    raise

                wait_seconds = self._get_google_network_retry_delay_seconds(attempt=attempt)
                logger.warning(
                    "Сетевая ошибка при записи в Google Sheets, повторяем попытку | operation=%s | attempt=%s/%s | wait_seconds=%s | error_type=%s",
                    operation_name,
                    attempt,
                    GOOGLE_WRITE_RETRY_ATTEMPTS,
                    wait_seconds,
                    type(error).__name__,
                )
                time.sleep(wait_seconds)

        raise RuntimeError(f"Не удалось завершить операцию записи в Google Sheets: {operation_name}")

    def _is_retryable_google_error(self, error: gspread.exceptions.APIError) -> bool:
        """Проверяет, относится ли ошибка Google Sheets к временным.

        Бизнес-сценарий:
        повторяем только те ответы Google, которые обычно проходят сами собой,
        чтобы не прятать реальные ошибки структуры листа или прав доступа.
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
        на `429` нужен более длинный нарастающий backoff, чтобы следующая
        запись не врезалась в тот же лимит почти мгновенно.
        """
        status_code = self._get_google_error_status_code(error)
        if status_code == 429:
            retry_delays = (15, 30, 45, 60)
            return retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        return self._get_google_network_retry_delay_seconds(attempt=attempt)

    def _sheet_update_cell(self, value: object) -> object:
        """Нормализует значение ячейки перед записью в Google Sheets.

        Бизнес-сценарий:
        витрина баланса не должна засоряться `NaN`, бесконечностями и сырыми
        объектами дат, иначе лист становится неудобным для финансовой команды.
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
    def _get_google_error_status_code(error: gspread.exceptions.APIError) -> int | None:
        """Извлекает HTTP-статус ошибки Google Sheets для логов и backoff.

        Бизнес-сценарий:
        статус помогает быстро отличать квотный `429` от других проблем записи.
        """
        response = getattr(error, "response", None)
        return getattr(response, "status_code", None)

    @staticmethod
    def _get_google_network_retry_delay_seconds(attempt: int) -> int:
        """Возвращает паузу для повторной записи после сетевого сбоя.

        Бизнес-сценарий:
        сетевые обрывы обычно восстанавливаются быстрее квотных ограничений,
        поэтому для них нужен более короткий backoff без лишней задержки job.
        """
        retry_delays = (5, 10, 20, 30)
        return retry_delays[min(attempt - 1, len(retry_delays) - 1)]
