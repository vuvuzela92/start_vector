from src_oop.jobs.conditional_calculations.db_client import ConditionalCalculations
from src_oop.jobs.conditional_calculations.tables_scheme import conditional_calculations
import logging
from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from gspread_dataframe import set_with_dataframe
import gspread

logger = logging.getLogger(__name__)

def conditional_calculation_to_db_run(days_ago: int = 30, days_to: int = 1):
    """Функция получает данные по Условному расчету и добавляет их в БД"""
    df = ConditionalCalculations(days_ago, days_to).execute_conditional_calculations()

    if df.empty:
        logger.warning("Нет данных для записи")
        return
    
    scheme = conditional_calculations.get("columns")
    table = conditional_calculations.get("title")
    keys = conditional_calculations.get("unique_keys")

    Database.sync_data_to_postgres(
        table_name=table,
        data=df,
        schema_definition=scheme,
        unique_keys=keys
    )

def update_conditional_calculations_to_gs(table_name: str = "Условный расчет", sheet_name: str = "Справочная информация"):
    df = ConditionalCalculations().get_conditional_calculations()
   
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