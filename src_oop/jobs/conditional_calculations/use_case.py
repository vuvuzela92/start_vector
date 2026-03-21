from src_oop.jobs.conditional_calculations.repository import GetDataFromDB
from src_oop.core.my_gspread import GoogleTabs
from gspread_dataframe import set_with_dataframe
from src_oop.core.database import Database
import gspread

class ConditionalCalculationsToGS:
    def __init__(self, table_name: str = "Условный расчет", sheet_name: str = "Справочная информация"):
        self.table_name = table_name
        self.sheet_name = sheet_name

    def update_conditional_calculations_to_gs(self):
        engine = engine = Database.get_engine()
        df = GetDataFromDB(engine).get_conditional_calculations()
        # Передаем название таблицы и листа
        google_table = self.table_name
        table_sheet = self.sheet_name        
        try:
            # Создаем соединение с гугл-таблицей
            google_connect = GoogleTabs(table_title=google_table, sheet_title=table_sheet)
            # Вставляем данные в гугл-таблицу
            set_with_dataframe(google_connect.sheet_title, df)
            print("Данные вставлены в гугл таблицу")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Не найдена таблица {google_table}")
        except gspread.exceptions.WorksheetNotFound as e:
            print(f"Не найден лист {table_sheet} в таблице {google_table}")
        except StopIteration:
            print(f"Не найден лист {table_sheet} в таблице {google_table}")
        except RuntimeError as e:
            print(f"Ошибка подключения: {e}")            
        
