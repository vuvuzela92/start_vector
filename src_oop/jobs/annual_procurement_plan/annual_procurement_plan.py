# Импортируем внутренние модули
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.annual_procurement_plan.config import delivery_calculation_china, annual_procurement_plan, unit_gs, supplies_query
from src_oop.core.database import Database
# Импортируем внешние библиотеки
import pandas as pd

class AnnualProcurementPlan:
    """Класс для обслуживания гугл-таблицы Годовой план закупа 2026"""
    
    def __init__(self):
        # Настройки для "Заказы белые ТЕСТ"
        self._table_white_name = delivery_calculation_china.get("title")
        self._sheet_white_name = delivery_calculation_china.get("white_orders_sheet")
        self._conn_white = None

        # Настройки для "Заказы"
        self._table_orders_name = delivery_calculation_china.get("title")
        self._sheet_orders_name = delivery_calculation_china.get("orders_sheet")
        self._conn_orders = None

        # Настройки для "Годовой план закупа 2026"
        self._table_to_name = annual_procurement_plan.get("title")
        self._sheet_to_name = annual_procurement_plan.get("orders_sheet")
        self._unit_sheet = annual_procurement_plan.get("unit_sheet")
        self._supply_sheet = annual_procurement_plan.get("supply_sheet")
        self._conn_to = None

        # Настройки для юнитки
        self._table_unit = unit_gs.get("title")
        self._sheet_unit = unit_gs.get("unit_sheet")
        self._conn_unit = None
        self.unit_cols = ["wild", "ФБО"]

        # Константы для фильтрации
        self.choosen_orders_columns = ["wild", "Модель", "Статус", "Кол-во к заказу", "Сумма заказа, RMB", "нед прибытие"]
        self.cancel_statuses = ["отмена", "в планах", "прибыло"]

        # Подключение к базе данных
        self.engine = Database.get_engine()

    # --- Свойства для ленивого подключения ---

    @property
    def google_connect_white(self):
        if self._conn_white is None:
            self._conn_white = GoogleTabs(self._table_white_name, self._sheet_white_name)
        return self._conn_white

    @property
    def google_connect_orders(self):
        """Ленивое подключение к листу Заказы в таблице Расчет поставки Китай_по обороту"""
        if self._conn_orders is None:
            self._conn_orders = GoogleTabs(self._table_orders_name, self._sheet_orders_name)
        return self._conn_orders

    @property
    def google_connect_to(self):
        """Ленивое подключение к таблице Годовой план закупа 2026 для работы с заказами"""
        if self._conn_to is None:
            self._conn_to = GoogleTabs(self._table_to_name, self._sheet_to_name)
        return self._conn_to

    @property
    def annual_plan_connect_to_unit_sheet(self):
        """Ленивое подключение к листу с данными юнитки в таблице Годовой план закупа 2026"""
        if self._conn_to is None:
            self._conn_to = GoogleTabs(self._table_to_name, self._unit_sheet)
        return self._conn_to
    
    @property
    def annual_plan_connect_to_supply_sheet(self):
        """Ленивое подключение к листу с данными поставок в таблице Годовой план закупа 2026"""
        if self._conn_to is None:
            self._conn_to = GoogleTabs(self._table_to_name, self._supply_sheet)
        return self._conn_to

    @property
    def google_connect_unit(self):
        """Ленивое подключение к юнитке"""
        if self._conn_unit is None:
            self._conn_unit = GoogleTabs(self._table_unit, self._sheet_unit)
        return self._conn_unit

    # --- Методы работы с данными ---

    def get_white_orders_data(self):
        """Получает данные из листа 'Заказы белые' таблицы 'Расчет поставки Китай_по обороту'"""
        data = self.google_connect_white.sheet_title.get_all_values()
        df = pd.DataFrame(data[4:], columns=data[3])
        return df
    
    def get_orders_data(self):
        """Получает данные из листа 'Заказы' таблицы 'Расчет поставки Китай_по обороту'"""
        data = self.google_connect_orders.sheet_title.get_all_values()
        df = pd.DataFrame(data[7:], columns=data[6])
        return df
    
    def get_unit_data(self):
        """Получает данные из юнитки таблицы 'UNIT 2.0 (tested)'"""
        data = self.google_connect_unit.sheet_title.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    
    def get_supplies_data(self):
        """Получает данные из базы данных по запросу supplies_query"""
        return Database.read_sql_to_dataframe(supplies_query)

    @staticmethod
    def set_data(coonector: GoogleTabs, df: pd.DataFrame):
        """Записывает данные в целевую таблицу. Принимает на вход объект GoogleTabs вида self._conn_to и DataFrame с данными для записи."""
        # Подключение создастся только здесь
        coonector.set_df_to_google(df)