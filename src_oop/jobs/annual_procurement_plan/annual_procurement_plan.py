# Импортируем внутренние модули
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.annual_procurement_plan.config import (
    delivery_calculation_china,
    annual_procurement_plan,
    unit_gs,
    supplies_query,
    parfume_query,
    seller_price_query,
    purchase_price_query,
    days_without_stocks_query,
    sales_plan_new_items,
)
from src_oop.core.database import Database
# Импортируем внешние библиотеки
import pandas as pd

class AnnualProcurementPlan:
    """Класс для обслуживания гугл-таблицы Годовой план закупа 2026"""

    def __init__(self):
        """Инициализирует источники и состав колонок для сценария обновления годового плана закупа.

        Метод настраивает подключения к таблицам-источникам и к листам выгрузки,
        которые участвуют в переносе заказов, юнит-данных, поставок и парфюма.
        Отдельно фиксирует список колонок заказа, который должен попадать в
        вкладку ``БД_ЗАКАЗЫ`` как единый бизнес-срез для последующей аналитики.
        """
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
        self._parfume_sheet = annual_procurement_plan.get("parfume_sheet")
        self._days_without_stocks_sheet = annual_procurement_plan.get(
            "days_without_stocks_sheet"
        )
        self._quarter_sheet = annual_procurement_plan.get("quarter_sheet")
        self._conn_to = None
        self._conn_quarter = None
        self._conn_days_without_stocks = None

        # Источник дат запуска новых товаров.
        self._new_items_table_name = sales_plan_new_items.get("title")
        self._new_items_spreadsheet_id = sales_plan_new_items.get("spreadsheet_id")
        self._new_items_sheet_name = sales_plan_new_items.get("sheet")
        self._new_items_header_row = sales_plan_new_items.get("header_row")
        self._conn_new_items = None

        # Настройки для юнитки
        self._table_unit = unit_gs.get("title")
        self._sheet_unit = unit_gs.get("unit_sheet")
        self._conn_unit = None
        self.unit_cols = ["wild", "ФБО"]

        # Константы для фильтрации
        self.choosen_orders_columns = [
            "wild",
            "Модель",
            "Статус",
            "Кол-во к заказу",
            "Сумма заказа, RMB",
            "нед прибытие",
            "Поставщик",
            "Цена WB",
            "Стоимость в закупке (руб.)",
        ]
        self.cancel_statuses = ["отмена", "в планах", "прибыло","РАЗОБРАТЬСЯ", "утеряно карго", "не получили", "излишек", "недостача"]
        self.quarter_price_column = "цена продажная плановая"
        self.quarter_cost_column = "себестоимость 2025-2026"
        self.white_plan_cost_column = "Себестоимость 1 шт. в руб ПЛАН"

        # Подключение к базе данных
        self.engine = Database.get_engine()

    # --- Свойства для ленивого подключения ---

    # Свойства для ленивого подключения к гугл-таблицам
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

    @property
    def annual_plan_connect_to_quarter_sheet(self):
        """Ленивое подключение к листу Поквартально в годовом плане закупа.

        Свойство обслуживает отдельный сценарий точечного обновления ценовых
        колонок в листе ``Поквартально`` без полной перезаписи таблицы.
        """
        if self._conn_quarter is None:
            self._conn_quarter = GoogleTabs(self._table_to_name, self._quarter_sheet)
        return self._conn_quarter

    @property
    def annual_plan_connect_to_days_without_stocks_sheet(self):
        """Возвращает подключение к витрине дней без остатков.

        Свойство обслуживает регламентную выгрузку агрегированных данных из
        `sales_wild_status_daily` в отдельную вкладку годового плана закупа.
        Подключение создается лениво, чтобы остальные сценарии годового плана
        не открывали этот лист без необходимости.
        """
        if self._conn_days_without_stocks is None:
            self._conn_days_without_stocks = GoogleTabs(
                self._table_to_name,
                self._days_without_stocks_sheet,
            )
        return self._conn_days_without_stocks

    @property
    def sales_plan_new_items_connect(self):
        """Возвращает подключение к вкладке `НОВИНКИ` панели продаж.

        Свойство обслуживает перенос даты запуска товара в продажу в
        квартальный план закупа. Таблица открывается по стабильному ID, чтобы
        ручное переименование документа не сломало регламентную выгрузку.
        """
        if self._conn_new_items is None:
            self._conn_new_items = GoogleTabs(
                self._new_items_table_name,
                self._new_items_sheet_name,
                spreadsheet_id=self._new_items_spreadsheet_id,
            )
        return self._conn_new_items

    @property
    def annual_plan_connect_to_parfume_sheet(self):
        """Ленивое подключение к листу с данными парфюма в таблице Годовой план закупа 2026"""
        if self._conn_to is None:
            self._conn_to = GoogleTabs(self._table_to_name, self._parfume_sheet)
        return self._conn_to

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

    def get_quarterly_plan_data(self):
        """Получает данные листа ``Поквартально`` с шапкой в 4-й строке.

        Метод обслуживает сценарий точечного обновления цен в квартальном
        плане закупа. Он сохраняет реальную структуру листа: заголовки берутся
        из 4-й строки, а строки данных начинаются с 5-й, чтобы привязка по
        `wild` совпадала с тем, что видит бизнес в Google Sheets.
        """
        data = self.annual_plan_connect_to_quarter_sheet.get_all_values_with_retry()
        df = pd.DataFrame(data[4:], columns=data[3])
        return df

    def get_days_without_stocks_data(self):
        """Получает число дней без остатков по месяцу и `wild`.

        Метод обслуживает вкладку ``Дней с остатком`` и считает отдельно
        дневные записи, где `sales_wild_status_daily.is_active` равно `FALSE`
        и `TRUE`. Результат уже сгруппирован в PostgreSQL по месяцу и товару
        и отсортирован для стабильной выгрузки в Google Sheets.
        """
        return Database.read_sql_to_dataframe(days_without_stocks_query)

    def get_new_items_data(self):
        """Получает `WILD` и дату запуска из вкладки `НОВИНКИ`.

        Метод обслуживает перенос дат запуска в ``Поквартально``. В источнике
        заголовки находятся во второй строке, поэтому первая строка считается
        служебной и не попадает в DataFrame.
        """
        data = self.sales_plan_new_items_connect.sheet_title.get_all_values()
        header_index = self._new_items_header_row - 1
        if len(data) <= header_index:
            return pd.DataFrame(columns=["WILD", "Запуск"])

        headers = [
            str(header).replace("\u00a0", " ").strip()
            for header in data[header_index]
        ]
        return pd.DataFrame(data[header_index + 1 :], columns=headers)

    def get_supplies_data(self):
        """Получает данные поставок для блока фактических приходов годового плана.

        Метод обслуживает вкладку ``Данные_Поставки``. В выборку попадают
        агрегированные количества по `local_vendor_code` и дате поставки, чтобы
        закупка могла сопоставлять план с уже подтвержденными поступлениями.
        """
        return Database.read_sql_to_dataframe(supplies_query)

    def get_seller_price_data(self):
        """Получает самую свежую цену WB по артикулу продавца без окна по дате.

        Метод обслуживает финальную выгрузку во вкладку ``БД_ЗАКАЗЫ``. Он
        возвращает последнюю доступную `seller_price` из `wb_order_feed` по
        каждому `local_vendor_code`, чтобы в годовом плане закупа рядом с
        заказом была максимально актуальная цена WB даже для товаров, у которых
        давно не было новых продаж.
        """
        return Database.read_sql_to_dataframe(seller_price_query)

    def get_purchase_price_data(self):
        """Получает максимально полную закупочную стоимость для годового плана.

        Метод обслуживает финальную выгрузку во вкладку ``БД_ЗАКАЗЫ``. Для
        расчета используется та же бизнес-логика, что и в боевой задаче
        `purchase_price_update`: по каждому `local_vendor_code` берется
        последняя валидная поставка и выбирается итоговый `price_per_item` с
        учетом `planned_cost` для специальных сценариев валюты и поставщика.
        В отличие от боевого обновления цены в UNIT здесь не используется
        ограничение по дате, чтобы закупочная стоимость подтянулась для
        максимального количества товаров в годовом плане.
        """
        return Database.read_sql_to_dataframe(purchase_price_query)

    def get_parfume_data(self):
        """Получает отзывы по парфюмерным товарам для отдельной вкладки плана.

        Метод обслуживает лист ``Данные_Парфюм`` и возвращает диагностическую
        выгрузку по отзывам, которая используется в смежном бизнес-сценарии
        контроля качества и оценки карточек товаров.
        """
        return Database.read_sql_to_dataframe(parfume_query)

    @staticmethod
    def set_data(coonector: GoogleTabs, df: pd.DataFrame):
        """Записывает данные в целевую таблицу. Принимает на вход объект GoogleTabs вида self._conn_to и DataFrame с данными для записи."""
        # Подключение создастся только здесь
        coonector.set_df_to_google(df)
