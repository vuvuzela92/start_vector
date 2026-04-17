from src_oop.core.database import Database
from src_oop.jobs.unit.queries import query_adv_spend
from src_oop.jobs.unit.config import unit_gs, unit_gs_test
from src_oop.core.my_gspread import GoogleTabs

class UnitEconomics:
    def __init__(self):
        self.database = Database()
        # Получаем данные из гугл-таблицы
        self.google_table = unit_gs.get("title")
        self.sheet = unit_gs.get("unit_sheet")
        self.google_connect = GoogleTabs(self.google_table, self.sheet)
        # Тестовые данные для отладки
        self.test_google_table = unit_gs_test.get("title")
        self.test_sheet = unit_gs_test.get("unit_sheet")
        self.google_connect_test = GoogleTabs(self.test_google_table, self.test_sheet)
    
    def get_adv_spend(self):
        """Получаем данные о рекламных расходах по статьям за вчерашний день"""
        return self.database.read_sql_to_dict(query_adv_spend)