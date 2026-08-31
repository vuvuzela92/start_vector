import logging
import math
import time
from datetime import date, datetime
from pathlib import Path

import gspread
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from google.auth import exceptions as google_auth_exceptions
from gspread.utils import rowcol_to_a1

load_dotenv()

logger = logging.getLogger(__name__)
GOOGLE_WRITE_RETRY_ATTEMPTS = 4
GOOGLE_WRITE_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
GOOGLE_CONNECT_RETRY_ATTEMPTS = 5


def _json_safe_cell(value):
    """Преобразует значение в безопасный для JSON вид при подготовке к Google Sheets.

    Бизнес-сценарий:
    часть выгрузок подготавливает данные через общий клиент Google Sheets, и
    бесконечности или `NaN` не должны попадать в запросы к API, иначе отчёт
    может оборваться ещё до записи в таблицу.
    """

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, (list, tuple)):
        return [_json_safe_cell(item) for item in value]

    if isinstance(value, dict):
        return {key: _json_safe_cell(item) for key, item in value.items()}

    return value


def _sheet_update_cell(value):
    """Нормализует значение ячейки перед записью в Google Sheets.

    Бизнес-сценарий:
    общая инфраструктура выгрузок должна превращать пропуски, `NaN` и
    бесконечности в пустые ячейки, чтобы отчёты не засорялись служебными
    значениями и не ломали форматирование листов.
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


class GoogleTabs:
    """Класс для работы с Google Таблицами."""

    def __init__(
        self,
        table_title: str,
        sheet_title: str,
        creds_file: str | Path | None = None,
        spreadsheet_id: str | None = None,
    ):
        """Создаёт подключение к Google Таблице и выбранному листу проекта.

        Бизнес-сценарий:
        единая точка доступа к Google Sheets нужна большинству выгрузок
        проекта, чтобы все job-сценарии одинаково открывали таблицы по имени
        или по стабильному `spreadsheet_id` и использовали общие правила записи
        и обработки временных сбоев. Открытие по `spreadsheet_id` защищает
        сценарии от ручного переименования документа.
        """

        self.creds_file = (
            Path(creds_file)
            if creds_file is not None
            else Path(__file__).resolve().parents[2] / "creds/creds.json"
        )
        self.table_title = table_title
        self.spreadsheet_id = spreadsheet_id
        self.table = None
        self.sheet_title = sheet_title
        self._safe_connect()

    def _safe_connect(self, retries: int = GOOGLE_CONNECT_RETRY_ATTEMPTS, delay: int = 2):
        """Подключается к таблице и листу с ограниченными повторами открытия.

        Бизнес-сценарий:
        сценарии выгрузок не должны падать из-за кратковременного `429`,
        `5xx` или сетевого сбоя на этапе открытия Google Sheets, поэтому
        клиент делает несколько попыток подключения перед окончательной
        ошибкой. Это особенно важно для cron-задач, которые могут попасть в
        минутные квоты чтения Google Sheets. Если для документа задан
        `spreadsheet_id`, подключение выполняется по нему, чтобы переименование
        таблицы не ломало бизнес-сценарий.
        """

        self.gc = gspread.service_account(filename=self.creds_file)

        for attempt in range(1, retries + 1):
            try:
                table = self._open_table()
                self.table = table
                self.sheet_title = table.worksheet(self.sheet_title)

                logger.info(
                    "Успешное подключение к Google Sheets | table=%s | sheet=%s | spreadsheet_id=%s",
                    table.title,
                    self.sheet_title.title,
                    self.spreadsheet_id,
                )
                return

            except gspread.exceptions.APIError as error:
                if not self._is_retryable_google_error(error):
                    raise
                if attempt == retries:
                    logger.exception(
                        "Подключение к Google Sheets исчерпало все попытки после API-ошибки | table=%s | sheet=%s | attempts=%s",
                        self.table_title,
                        self.sheet_title,
                        retries,
                    )
                    raise

                wait_seconds = self._get_google_retry_delay_seconds(error=error, attempt=attempt)
                status_code = self._get_google_error_status_code(error)
                logger.warning(
                    "Google Sheets временно недоступен при подключении, повторяем попытку | table=%s | sheet=%s | status_code=%s | attempt=%s/%s | wait_seconds=%s",
                    self.table_title,
                    self.sheet_title,
                    status_code,
                    attempt,
                    retries,
                    wait_seconds,
                )
                time.sleep(max(wait_seconds, delay))
            except gspread.exceptions.WorksheetNotFound:
                raise RuntimeError(
                    f"Ошибка: Лист '{self.sheet_title}' не найден в таблице '{self.table_title}'"
                )
            except requests.exceptions.RequestException as error:
                if attempt == retries:
                    logger.exception(
                        "Подключение к Google Sheets исчерпало все попытки после сетевой ошибки | table=%s | sheet=%s | attempts=%s | error_type=%s",
                        self.table_title,
                        self.sheet_title,
                        retries,
                        type(error).__name__,
                    )
                    raise

                wait_seconds = self._get_google_network_retry_delay_seconds(attempt=attempt)
                logger.warning(
                    "Сетевая ошибка при подключении к Google Sheets, повторяем попытку | table=%s | sheet=%s | attempt=%s/%s | wait_seconds=%s | error_type=%s",
                    self.table_title,
                    self.sheet_title,
                    attempt,
                    retries,
                    wait_seconds,
                    type(error).__name__,
                )
                time.sleep(max(wait_seconds, delay))
            except google_auth_exceptions.TransportError as error:
                if attempt == retries:
                    logger.exception(
                        "Подключение к Google Sheets исчерпало все попытки после ошибки транспортного слоя Google Auth | table=%s | sheet=%s | attempts=%s | error_type=%s",
                        self.table_title,
                        self.sheet_title,
                        retries,
                        type(error).__name__,
                    )
                    raise

                wait_seconds = self._get_google_network_retry_delay_seconds(attempt=attempt)
                logger.warning(
                    "Google Auth временно недоступен при подключении к Google Sheets, повторяем попытку | table=%s | sheet=%s | attempt=%s/%s | wait_seconds=%s | error_type=%s",
                    self.table_title,
                    self.sheet_title,
                    attempt,
                    retries,
                    wait_seconds,
                    type(error).__name__,
                )
                time.sleep(max(wait_seconds, delay))

        raise RuntimeError(
            f"Не удалось открыть таблицу '{self.table_title}' после {retries} попыток."
        )

    def _open_table(self):
        """Открывает Google Таблицу по `spreadsheet_id` или по имени.

        Бизнес-сценарий:
        часть критичных сценариев должна переживать ручное переименование
        документа. Для них приоритетным идентификатором считается
        `spreadsheet_id`. Если одновременно задано и ожидаемое имя таблицы,
        клиент дополнительно проверяет его и пишет предупреждение в лог, если
        название изменилось.
        """
        if self.spreadsheet_id:
            table = self.gc.open_by_key(self.spreadsheet_id)
            if self.table_title and table.title != self.table_title:
                logger.warning(
                    "Название Google Таблицы отличается от ожидаемого, но подключение продолжается по spreadsheet_id | expected_title=%s | actual_title=%s | spreadsheet_id=%s",
                    self.table_title,
                    table.title,
                    self.spreadsheet_id,
                )
            return table

        return self.gc.open(self.table_title)

    def _execute_google_write_with_retry(self, operation_name: str, func, *args, **kwargs):
        """Выполняет запись в Google Sheets с retry при временных ошибках.

        Бизнес-сценарий:
        многие отчёты проекта пишутся одним крупным запросом. Если Google
        Sheets временно отвечает `429` или `5xx`, лучше выдержать паузу и
        повторить запись, чем сразу ронять задачу и оставлять лист без
        обновления.
        """

        for attempt in range(1, GOOGLE_WRITE_RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as error:
                if not self._is_retryable_google_error(error):
                    logger.exception(
                        "Операция записи в Google Sheets завершилась неретраебельной ошибкой | operation=%s | attempt=%s",
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
            except google_auth_exceptions.TransportError as error:
                if attempt == GOOGLE_WRITE_RETRY_ATTEMPTS:
                    logger.exception(
                        "Операция записи в Google Sheets исчерпала все попытки после ошибки транспортного слоя Google Auth | operation=%s | attempts=%s",
                        operation_name,
                        GOOGLE_WRITE_RETRY_ATTEMPTS,
                    )
                    raise

                wait_seconds = self._get_google_network_retry_delay_seconds(attempt=attempt)
                logger.warning(
                    "Google Auth временно недоступен при записи в Google Sheets, повторяем попытку | operation=%s | attempt=%s/%s | wait_seconds=%s | error_type=%s",
                    operation_name,
                    attempt,
                    GOOGLE_WRITE_RETRY_ATTEMPTS,
                    wait_seconds,
                    type(error).__name__,
                )
                time.sleep(wait_seconds)

        raise RuntimeError(f"Не удалось завершить операцию записи в Google Sheets: {operation_name}")

    @staticmethod
    def _get_google_error_status_code(error: gspread.exceptions.APIError) -> int | None:
        """Извлекает HTTP-статус ошибки Google Sheets для логов и backoff.

        Бизнес-сценарий:
        при расследовании падения важно быстро отличать квотный `429` от
        других проблем записи, чтобы понять, помогает ли backoff или нужно
        чинить конфигурацию листа.
        """

        response = getattr(error, "response", None)
        return getattr(response, "status_code", None)

    def _is_retryable_google_error(self, error: gspread.exceptions.APIError) -> bool:
        """Проверяет, относится ли ошибка Google Sheets к временным.

        Бизнес-сценарий:
        ретраим только те ответы Google Sheets, которые обычно проходят сами
        собой, чтобы не прятать реальные ошибки структуры таблицы, прав
        доступа или неверного диапазона записи.
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
        на `429` нужен более длинный нарастающий backoff, иначе следующая
        попытка почти сразу снова попадёт в лимит. Для временных `5xx` хватит
        более короткой паузы, чтобы не тормозить стандартные выгрузки.
        """

        status_code = self._get_google_error_status_code(error)
        if status_code == 429:
            retry_delays = (15, 30, 45, 60)
            return retry_delays[min(attempt - 1, len(retry_delays) - 1)]
        return self._get_google_network_retry_delay_seconds(attempt=attempt)

    @staticmethod
    def _get_google_network_retry_delay_seconds(attempt: int) -> int:
        """Возвращает паузу для повторной записи после сетевого сбоя.

        Бизнес-сценарий:
        сетевые обрывы обычно восстанавливаются быстрее квотных ограничений,
        поэтому для них используется более короткий backoff без лишней задержки
        обычных выгрузок.
        """

        retry_delays = (5, 10, 20, 30)
        return retry_delays[min(attempt - 1, len(retry_delays) - 1)]

    def _update_df_in_google(self, df: pd.DataFrame, sheet):
        """Полностью перезаписывает рабочую область листа одним безопасным update.

        Бизнес-сценарий:
        отчёты вроде аналитики продаж должны обновлять целиком весь лист,
        чтобы в витрине не оставались старые строки после уменьшения набора
        данных. При временных ответах Google запись повторяется с паузой.
        """

        try:
            old_values = sheet.get_all_values()
            old_rows = len(old_values)
            old_cols = max((len(row) for row in old_values), default=0)

            df_to_upload = df.copy()
            df_to_upload = df_to_upload.replace([np.inf, -np.inf], "")
            df_to_upload = df_to_upload.astype(object)
            df_to_upload = df_to_upload.where(pd.notnull(df_to_upload), "")

            data_values = []
            if len(df_to_upload.columns) > 0:
                data_values = [
                    df_to_upload.columns.astype(str).tolist(),
                    *df_to_upload.values.tolist(),
                ]

            new_rows = len(data_values)
            new_cols = len(data_values[0]) if data_values else 0

            target_rows = max(old_rows, new_rows)
            target_cols = max(old_cols, new_cols)

            if target_rows == 0 or target_cols == 0:
                logger.info("Обновление Google Sheets пропущено: старых и новых данных нет.")
                return

            values = [["" for _ in range(target_cols)] for _ in range(target_rows)]

            for row_idx, row in enumerate(data_values):
                for col_idx, value in enumerate(row):
                    values[row_idx][col_idx] = _sheet_update_cell(value)

            target_range = f"A1:{rowcol_to_a1(target_rows, target_cols)}"
            self._execute_google_write_with_retry(
                operation_name=f"update_range {sheet.title} {target_range}",
                func=sheet.update,
                range_name=target_range,
                values=values,
                value_input_option="USER_ENTERED",
            )
            logger.info("Данные Google Sheets полностью перезаписаны в диапазоне %s", target_range)

        except Exception as error:
            logger.exception("Не удалось обновить Google Sheets: %s", error)
            if "APIError: [400]: This action would increase the number of cells in the workbook" in str(error):
                logger.error("Превышен лимит ячеек Google Sheets при полной перезаписи.")
            raise

    def _send_df_to_google(self, df, sheet):
        """Добавляет строки DataFrame в лист Google Sheets с retry на запись.

        Бизнес-сценарий:
        legacy-сценарии проекта местами дописывают строки в существующий лист,
        поэтому даже в режиме append запись должна переживать временные лимиты
        Google Sheets и не падать на первом `429`.
        """

        try:
            df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
            existing_data = sheet.get_all_values()

            if len(existing_data) <= 1:
                print("Добавляем заголовки и данные")
                self._execute_google_write_with_retry(
                    operation_name=f"append_rows {sheet.title} with_headers",
                    func=sheet.append_rows,
                    values=df_data_to_append,
                    value_input_option="USER_ENTERED",
                )
            else:
                print("Добавляем только данные")
                self._execute_google_write_with_retry(
                    operation_name=f"append_rows {sheet.title} data_only",
                    func=sheet.append_rows,
                    values=df_data_to_append[1:],
                    value_input_option="USER_ENTERED",
                )

        except Exception as error:
            print(f"Произошла ошибка при записи DataFrame в Google Sheets: {error}")

    def update_column_by_name(self, column_name: str, data_to_write: list):
        """Обновляет выбранную колонку листа по имени заголовка.

        Бизнес-сценарий:
        служебные задачи проекта точечно перезаписывают отдельные колонки без
        полной очистки листа, и такие обновления тоже должны переживать
        временные лимиты Google Sheets без мгновенного падения.
        """

        try:
            headers = self.sheet_title.row_values(1)

            if column_name not in headers:
                raise ValueError(f"Колонка '{column_name}' не найдена в таблице!")

            col_idx = headers.index(column_name) + 1
            vertical_values = [[val] for val in data_to_write]

            start_cell = rowcol_to_a1(2, col_idx)
            end_cell = rowcol_to_a1(len(data_to_write) + 1, col_idx)
            range_label = f"{start_cell}:{end_cell}"

            self._execute_google_write_with_retry(
                operation_name=f"update_column {self.sheet_title.title} {column_name}",
                func=self.sheet_title.update,
                range_name=range_label,
                values=vertical_values,
            )
            print(f"Данные успешно записаны в колонку '{column_name}' (диапазон {range_label})")

        except Exception as error:
            print(f"Ошибка при динамическом обновлении: {error}")

    def update_column_by_name_at_header_row(
        self,
        column_name: str,
        data_to_write: list,
        header_row_number: int,
        data_start_row_number: int,
    ) -> None:
        """Обновляет колонку по имени заголовка в листе с произвольной строкой шапки.

        Бизнес-сценарий:
        часть управленческих листов проекта хранит заголовки не в первой строке,
        а ниже после служебных блоков. Для таких витрин нужно уметь точечно
        обновлять одну бизнес-колонку без полной перезаписи листа и без
        смещения пользовательской структуры таблицы.
        """

        try:
            headers = self.sheet_title.row_values(header_row_number)

            if column_name not in headers:
                raise ValueError(
                    f"Колонка '{column_name}' не найдена в строке заголовков {header_row_number}!"
                )

            if not data_to_write:
                logger.info(
                    "Точечное обновление колонки Google Sheets пропущено: нет данных для записи | sheet=%s | column=%s | header_row=%s",
                    self.sheet_title.title,
                    column_name,
                    header_row_number,
                )
                return

            col_idx = headers.index(column_name) + 1
            vertical_values = [[_sheet_update_cell(val)] for val in data_to_write]

            start_cell = rowcol_to_a1(data_start_row_number, col_idx)
            end_cell = rowcol_to_a1(
                data_start_row_number + len(data_to_write) - 1,
                col_idx,
            )
            range_label = f"{start_cell}:{end_cell}"

            self._execute_google_write_with_retry(
                operation_name=(
                    f"update_column {self.sheet_title.title} {column_name} "
                    f"header_row={header_row_number}"
                ),
                func=self.sheet_title.update,
                range_name=range_label,
                values=vertical_values,
            )
            logger.info(
                "Колонка Google Sheets обновлена по произвольной строке заголовков | sheet=%s | column=%s | range=%s | rows=%s",
                self.sheet_title.title,
                column_name,
                range_label,
                len(data_to_write),
            )

        except Exception as error:
            logger.exception(
                "Ошибка при точечном обновлении колонки Google Sheets | sheet=%s | column=%s | header_row=%s | error_type=%s",
                getattr(self.sheet_title, "title", self.sheet_title),
                column_name,
                header_row_number,
                type(error).__name__,
            )
            raise

    def set_df_to_google(self, df: pd.DataFrame):
        """Публикует DataFrame в Google Sheets с добавлением служебного времени обновления.

        Бизнес-сценарий:
        большинство витрин проекта должны показывать не только сами данные, но
        и момент последнего обновления. Поэтому перед записью клиент добавляет
        `updated_at` и нормализует все известные колонки с датами.
        """

        df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        date_columns = [
            "date",
            "updated_at",
            "created_at",
            "date_from",
            "date_to",
            "dt",
            "start_date",
            "end_date",
            "month",
            "supply_date",
            "Дата создания документа",
            "Дата поставки",
            "Ожидаемая дата прихода",
        ]

        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)

        try:
            self._update_df_in_google(df=df, sheet=self.sheet_title)
            print("Таблица полностью обновлена")

        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Не найдена таблица {self.table_title}")
            raise
        except gspread.exceptions.WorksheetNotFound:
            print(f"Не найден лист {self.sheet_title.title}")
            raise
        except Exception as error:
            print(f"Ошибка: {error}")
            raise
