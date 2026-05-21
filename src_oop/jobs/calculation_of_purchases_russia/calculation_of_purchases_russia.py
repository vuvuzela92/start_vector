from src_oop.core.my_gspread import GoogleTabs
from src_oop.core.database import Database
from src_oop.jobs.calculation_of_purchases_russia.config import google_table, query_orders_and_supply
import pandas as pd

class Calculation_of_purchases_russia:
    def __init__(self):
        # ===
        # Старые настройки подключения
        # ===

        # Объект подключения к БД
        self.db = Database()
        # Получаем данные из гугл-таблицы
        self.google_table = google_table.get("title")
        self.sheet = google_table.get("orders_sheet")
        self.google_connect = GoogleTabs(self.google_table, self.sheet)
        # Получаем статусы из гугл-таблицы
        self.statuses_sheet = google_table.get("statuses_sheet")
        self.google_connect_statuses = GoogleTabs(self.google_table, self.statuses_sheet)

        # === 
        # Новые настройки подключения
        # ====

        # Подключение к базе данных
        self.engine = Database.get_engine()
        # Расчет закупки Россия
        self._purchsase_russia_table = google_table.get("title")
        self._orders_buyers_sheet = google_table.get("orders_buyers_sheet")
        self._conn_purchase_russia = None

    # Свойства для ленивого подключения к гугл-таблицам
    @property
    def google_connect_to_purchsase_russia_table(self):
        if self._conn_purchase_russia is None:
            self._conn_purchase_russia = GoogleTabs(self._purchsase_russia_table, self._orders_buyers_sheet)
        return self._conn_purchase_russia
    
    @staticmethod
    def set_data(coonector: GoogleTabs, df: pd.DataFrame):
        """Записывает данные в целевую таблицу. Принимает на вход объект GoogleTabs вида self._conn_to и DataFrame с данными для записи."""
        # Подключение создастся только здесь
        coonector.set_df_to_google(df)
    
    def get_orders_and_supplies_data(self):
        """Получает данные из базы данных по запросу parfume_query"""
        return Database.read_sql_to_dataframe(query_orders_and_supply)