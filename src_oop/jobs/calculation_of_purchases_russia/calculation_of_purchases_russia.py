from src_oop.core.my_gspread import GoogleTabs
from src_oop.core.database import Database
from src_oop.jobs.calculation_of_purchases_russia.config import query, google_table

class Calculation_of_purchases_russia:
    def __init__(self):
        # Объект подключения к БД
        self.db = Database()
        # Получаем данные из гугл-таблицы
        self.google_table = google_table.get("title")
        self.sheet = google_table.get("orders_sheet")
        self.google_connect = GoogleTabs(self.google_table, self.sheet)
        # Получаем статусы из гугл-таблицы
        self.statuses_sheet = google_table.get("statuses_sheet")
        self.google_connect_statuses = GoogleTabs(self.google_table, self.statuses_sheet)