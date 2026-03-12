import re
import os
import time
import logging
import gspread
import pandas as pd
from datetime import datetime

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class GoogleTabs():
    """Класс для работы с гугл таблицами"""
    def __init__(self, table_title: str, sheet_title: str):
        """ Инициализируем класс для работы с гугл-таблицами.
        spreadsheet_title: Название гугл-таблицы
        worksheet_title: Название страницы"""
        self.creds_file = Path(__file__).resolve().parents[2]/'creds/creds.json' # Доступы к таблицам
        self.table_title = table_title # Названия таблиц
        self.table = None  # ← Открытая таблица
        self.sheet_title = sheet_title
        self._safe_connect()  # ← Подключаемся сразу

    def _safe_connect(self, retries=5, delay=2):        
            """
            Пытается открыть таблицу и лист с повторными попытками.
            """
            self.gc = gspread.service_account(filename=self.creds_file)
            
            for attempt in range(1, retries + 1):
                try:
                    # 1. Открываем саму таблицу
                    table = self.gc.open(self.table_title)
                    self.table = table # Сохраняем в атрибут класса
                    
                    # 2. Открываем конкретный лист 
                    self.sheet_title = table.worksheet(self.sheet_title) 
                    
                    print(f"✅ Успешное подключение к {self.table_title} -> {self.sheet_title.title}")
                    return # Выходим из функции, всё готово
                    
                except gspread.exceptions.APIError as e:
                    if "503" in str(e):
                        print(f"[Попытка {attempt}/{retries}] APIError 503 — повтор через {delay} сек.")
                        time.sleep(delay)
                    else:
                        raise 
                except gspread.exceptions.WorksheetNotFound:
                    raise RuntimeError(f"Ошибка: Лист '{self.sheet_title}' не найден в таблице '{self.table_title}'")

            raise RuntimeError(f"Не удалось открыть таблицу '{self.table_title}' после {retries} попыток.")

    def _update_df_in_google(self, df: pd.DataFrame, sheet):
        """
        Перезаписывает данные DataFrame на указанный лист Google Таблицы.
        Также добавляет дату и время последнего обновления в первую строку последней колонки.
        
        Параметры:
        df (DataFrame): DataFrame, который нужно отправить.
        sheet (gspread.models.Worksheet): Объект листа, на который будут записаны данные.

        Возвращаемое значение:
        None
        """
        try:
            # Обрабатываем NaN значения в DataFrame (заменяем на пустые строки)
            df = df.fillna('')

            # Очищаем лист перед записью новых данных
            sheet.clear()

            # Подготовка данных для записи
            df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()

            # Запись данных на лист
            sheet.append_rows(df_data_to_append, value_input_option='USER_ENTERED')
            print("Данные успешно перезаписаны на лист.")

        except Exception as e:
            print(f"Произошла ошибка: {e}")
            # Проверяем на ошибку, связанную с лимитом ячеек
            if "APIError: [400]: This action would increase the number of cells in the workbook" in str(e):
                print("Превышен лимит ячеек Google Таблицы. Создание резервной копии в Excel...")

    def _send_df_to_google(self, df, sheet):
        """
        Отправляет DataFrame на указанный лист Google Таблицы.

        Параметры:
        df (DataFrame): DataFrame, который нужно отправить.
        sheet (gspread.models.Worksheet): Объект листа, на который будут добавлены данные.

        Возвращаемое значение:
        None
        """
        try:
            # Данные, которые нужно добавить
            df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
            
            # Проверка существующих данных на листе
            existing_data = sheet.get_all_values()
            
            if len(existing_data) <= 1:  # Если данных нет
                print("Добавляем заголовки и данные")
                sheet.append_rows(df_data_to_append, value_input_option='USER_ENTERED')
            else:
                print("Добавляем только данные")
                sheet.append_rows(df_data_to_append[1:], value_input_option='USER_ENTERED')
                
        except Exception as e:
            print(f"An error occurred: {e}")