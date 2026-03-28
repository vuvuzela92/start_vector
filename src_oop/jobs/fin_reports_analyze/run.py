from src_oop.core.database import Database
from src_oop.jobs.fin_reports_analyze.repository import FinReportsAnalyze
from gspread_dataframe import set_with_dataframe
import logging
from datetime import datetime
import gspread
from src_oop.core.my_gspread import GoogleTabs
from src_oop.storage.google_sheets.google_sheets import fin_rep_analyze

logger = logging.getLogger(__name__)

def update_monthly_profit_report():
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о ежемесячных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_monthly_profit_report()
    df['updatet_at'] = datetime.now().strftime('%Y-%m-%d %H-%M-%S')

    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("monthly_rep")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        set_with_dataframe(google_connect.sheet_title, df)
        print("Данные вставлены в гугл таблицу")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {table_name}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except StopIteration:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}") 


def update_weekly_profit_report():
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_weekly_profit_report()
    df['updatet_at'] = datetime.now().strftime('%Y-%m-%d %H-%M-%S')

    
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("weekly_rep")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        set_with_dataframe(google_connect.sheet_title, df)
        print("Данные вставлены в гугл таблицу")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {table_name}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except StopIteration:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}")

def update_outcomes_detalize(table_name: str = "Анализ_фин_отчетов_Вектор", sheet_name: str = "отчет_по_неделям"):
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_outcomes_detalize()
    df['updatet_at'] = datetime.now().strftime('%Y-%m-%d %H-%M-%S')

    
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("outcomes_detalize")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        set_with_dataframe(google_connect.sheet_title, df)
        print("Данные вставлены в гугл таблицу")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {table_name}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except StopIteration:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}") 

def update_fin_deductions_mv(table_name: str = "Анализ_фин_отчетов_Вектор", sheet_name: str = "отчет_по_неделям"):
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_fin_deductions_mv()
    df['updatet_at'] = datetime.now().strftime('%Y-%m-%d %H-%M-%S')

    
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("deducations_detalize")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        set_with_dataframe(google_connect.sheet_title, df)
        print("Данные вставлены в гугл таблицу")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {table_name}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except StopIteration:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}") 


def update_daily_fin_reports_deductions_agg(table_name: str = "Анализ_фин_отчетов_Вектор", sheet_name: str = "отчет_по_неделям"):
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_daily_fin_reports_deductions_agg()
    df['updatet_at'] = datetime.now().strftime('%Y-%m-%d %H-%M-%S')

    
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("export_daily_fin_reports_deductions_agg")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        set_with_dataframe(google_connect.sheet_title, df)
        print("Данные вставлены в гугл таблицу")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {table_name}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except StopIteration:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}")