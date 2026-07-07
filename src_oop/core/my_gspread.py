import logging
import math
import time
from datetime import datetime
from pathlib import Path

import gspread
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from gspread.utils import rowcol_to_a1

load_dotenv()

logger = logging.getLogger(__name__)


def _json_safe_cell(value):
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
    if value is None:
        return ""

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
    ):
        self.creds_file = (
            Path(creds_file)
            if creds_file is not None
            else Path(__file__).resolve().parents[2] / "creds/creds.json"
        )
        self.table_title = table_title
        self.table = None
        self.sheet_title = sheet_title
        self._safe_connect()

    def _safe_connect(self, retries=5, delay=2):
        """
        Пытается открыть таблицу и лист с повторными попытками.
        """
        self.gc = gspread.service_account(filename=self.creds_file)

        for attempt in range(1, retries + 1):
            try:
                table = self.gc.open(self.table_title)
                self.table = table
                self.sheet_title = table.worksheet(self.sheet_title)

                print(f"Успешное подключение к {self.table_title} -> {self.sheet_title.title}")
                return

            except gspread.exceptions.APIError as error:
                if "503" in str(error):
                    print(f"[Попытка {attempt}/{retries}] APIError 503, повтор через {delay} сек.")
                    time.sleep(delay)
                else:
                    raise
            except gspread.exceptions.WorksheetNotFound:
                raise RuntimeError(
                    f"Ошибка: Лист '{self.sheet_title}' не найден в таблице '{self.table_title}'"
                )

        raise RuntimeError(
            f"Не удалось открыть таблицу '{self.table_title}' после {retries} попыток."
        )

    def _update_df_in_google(self, df: pd.DataFrame, sheet):
        """
        Полностью перезаписывает рабочую область листа одним вызовом update.
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
                logger.info("Google Sheet update skipped: no old data and no new data.")
                return

            values = [["" for _ in range(target_cols)] for _ in range(target_rows)]

            for row_idx, row in enumerate(data_values):
                for col_idx, value in enumerate(row):
                    values[row_idx][col_idx] = _sheet_update_cell(value)

            target_range = f"A1:{rowcol_to_a1(target_rows, target_cols)}"
            sheet.update(
                target_range,
                values,
                value_input_option="USER_ENTERED",
            )
            logger.info("Google Sheet data fully overwritten in range %s", target_range)

        except Exception as error:
            logger.exception("Failed to update Google Sheet: %s", error)
            if "APIError: [400]: This action would increase the number of cells in the workbook" in str(error):
                logger.error("Google Sheets cell limit exceeded during overwrite.")
            raise

    def _send_df_to_google(self, df, sheet):
        """
        Отправляет DataFrame на указанный лист Google Таблицы.
        """
        try:
            df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
            existing_data = sheet.get_all_values()

            if len(existing_data) <= 1:
                print("Добавляем заголовки и данные")
                sheet.append_rows(df_data_to_append, value_input_option="USER_ENTERED")
            else:
                print("Добавляем только данные")
                sheet.append_rows(df_data_to_append[1:], value_input_option="USER_ENTERED")

        except Exception as error:
            print(f"An error occurred: {error}")

    def update_column_by_name(self, column_name: str, data_to_write: list):
        """
        Находит колонку по названию и обновляет её содержимое, начиная со 2-й строки.
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

            self.sheet_title.update(range_label, vertical_values)
            print(f"Данные успешно записаны в колонку '{column_name}' (диапазон {range_label})")

        except Exception as error:
            print(f"Ошибка при динамическом обновлении: {error}")

    def set_df_to_google(self, df: pd.DataFrame):
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
