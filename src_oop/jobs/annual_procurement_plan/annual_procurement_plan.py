# Импортируем внутренние модули
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.annual_procurement_plan.config import delivery_calculation_china, annual_procurement_plan
# Импортируем внешние библиотеки
import pandas as pd
from datetime import datetime

class Annual_procurement_plan:
    """Класс для обслуживания гугл-таблицы Годовой план закупа 2026"""
    def __init__(self):
        # Получаем данные из Google Sheets Заказы белые ТЕСТ
        self.table_from = delivery_calculation_china.get("title")
        self.sheet_from = delivery_calculation_china.get("white_orders_sheet")
        self.google_connect_from = GoogleTabs(self.table_from, self.sheet_from)
        # Получаем данные из Google Sheets Заказы
        self.table_from_orders = delivery_calculation_china.get("title")
        self.sheet_from_orders = delivery_calculation_china.get("orders_sheet")
        self.google_connect_from_orders = GoogleTabs(self.table_from_orders, self.sheet_from_orders)
        # Данные для вставки в таблицу Годовой план закупа 2026
        self.table_to = annual_procurement_plan.get("title")
        self.sheet_to = annual_procurement_plan.get("orders_sheet")
        self.google_connect_to = GoogleTabs(self.table_to, self.sheet_to)
        # Колонки для фильтрации
        self.choosen_orders_columns = ["wild", "Модель", "Статус", "Кол-во к заказу", "Сумма заказа, RMB", "нед прибытие"]
        # Статусы для фильтрации
        self.cancel_statuses = ["отмена", "в планах", "прибыло"]

    def get_white_orders_data(self):
        # Подключаемся к листу Заказы белые ТЕСТ и получаем данные
        data = self.google_connect_from.sheet_title.get_all_values()
        data_from = data[4:] # Данные начиная с 5-й строки
        columns_from = data[3] # Названия колонок с 4-й строки
        df = pd.DataFrame(data_from, columns=columns_from)
        return df
    
    def get_orders_data(self):
        # Подключаемся к листу Заказы и получаем данные
        data = self.google_connect_from_orders.sheet_title.get_all_values()
        data_from = data[7:] # Данные начиная с 8-й строки
        columns_from = data[6] # Названия колонок с 7-й строки
        df = pd.DataFrame(data_from, columns=columns_from)
        return df

    def set_data(self, df):
        # Вставляем данные в таблицу Годовой план закупа 2026 в лист БД Заказы
        self.google_connect_to.set_df_to_google(df)