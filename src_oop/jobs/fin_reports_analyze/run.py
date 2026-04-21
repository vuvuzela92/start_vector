from src_oop.core.database import Database
from src_oop.jobs.fin_reports_analyze.repository import FinReportsAnalyze
from gspread_dataframe import set_with_dataframe
import logging
from datetime import datetime
import gspread
from src_oop.core.my_gspread import GoogleTabs
from src_oop.storage.google_sheets.google_sheets import fin_rep_analyze

from src_oop.jobs.fin_reports_analyze.queries import query_deductions_by_month, query_cash_flow_writeoffs,query_monthly_report, query_stock_analyze

logger = logging.getLogger(__name__)

def update_weekly_profit_report():
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_weekly_profit_report()
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("weekly_rep")
    # Создаем соединение с гугл-таблицей
    google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
    # Вставляем данные в гугл-таблицу
    google_connect.set_df_to_google(df)
    print("Данные вставлены в гугл таблицу")

def update_outcomes_detalize(table_name: str = "Анализ_фин_отчетов_Вектор", sheet_name: str = "отчет_по_неделям"):
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_outcomes_detalize()
    
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("outcomes_detalize")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        google_connect.set_df_to_google(df)
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

    
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("deducations_detalize")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        google_connect.set_df_to_google(df)
        print("Данные вставлены в гугл таблицу")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {table_name}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except StopIteration:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}") 

def update_deductions_by_month():
    """ Обновление данных о ежемесячных удержаниях в таблице Анализ_фин_отчетов_Вектор, лист отчет_по_месяцам"""
    analyze = FinReportsAnalyze()
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("deductions_by_month")
    analyze.set_processed_df_to_google(query_deductions_by_month, table_name=table_name, sheet_name=sheet_name)

def update_cash_flow_writeoffs():
    """ Выгрузка детализированных данных по затратам из 1С Анализ_фин_отчетов_Вектор, лист расходы_по_банку"""
    fin_rep = FinReportsAnalyze()
    df = fin_rep.get_update_cash_flow_writeoffs()
    table_name = fin_rep_analyze.get("title")
    sheet_name: str = fin_rep_analyze.get("query_cash_flow_writeoffs")
    # Создаем соединение с гугл-таблицей
    google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
    # Вставляем данные в гугл-таблицу
    google_connect.set_df_to_google(df)

def update_monthly_report():
    fin_rep = FinReportsAnalyze()
    df = fin_rep.get_monthly_profit_report()
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("monthly_rep")
    # Создаем соединение с гугл-таблицей
    google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
    # Вставляем данные в гугл-таблицу
    google_connect.set_df_to_google(df)

def update_stock_analyze():
    analyze = FinReportsAnalyze()
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("stock_analyze")
    analyze.set_processed_df_to_google(query_stock_analyze, table_name=table_name, sheet_name=sheet_name)

# python -m src_oop.jobs.fin_reports_analyze.run
# if __name__ == "__main__":
#     update_stock_analyze()