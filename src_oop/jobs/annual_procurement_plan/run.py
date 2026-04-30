# Импорт внутренних модулей
from src_oop.jobs.annual_procurement_plan.annual_procurement_plan import AnnualProcurementPlan
from src_oop.core.utils_general import clean_currency_value
# Импорт внешних библиотек
import pandas as pd

def transport_data_to_annual_procurement_plan():
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_white_orders = plan.get_white_orders_data()
    # Выбираем нужные колонки
    choosen_orders_columns = plan.choosen_orders_columns
    df_white_orders_short = df_white_orders[choosen_orders_columns]
    # Получаем датафрейм от листа Заказы в таблице Расчет поставки Китай_по обороту
    df_orders = plan.get_orders_data()
    df_orders_short = df_orders[choosen_orders_columns]
    # Объединяем датафреймы вертикально
    df_merge = pd.concat([
        df_white_orders_short.reset_index(drop=True), 
        df_orders_short.reset_index(drop=True)
    ], ignore_index=True)
    # Убираем знаки валюты из колонки
    df_merge['Сумма заказа, RMB'] = df_merge['Сумма заказа, RMB'].apply(clean_currency_value)
    # Выбираем статусы для фильтрации
    cancel_statuses = plan.cancel_statuses
    # Фильтрация
    df_merge = df_merge.loc[~df_merge['Статус'].isin(cancel_statuses)]
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(plan.google_connect_to, df_merge)


def transport_unit_data_to_annual_procurement_plan():
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_unit = plan.get_unit_data()
    df_unit_short = df_unit[plan.unit_cols]
    # Используем str.replace с регулярным выражением
    df_unit_short['ФБО'] = (
        df_unit_short['ФБО']
        .astype(str)
        .str.replace(r'[\s\xa0]+', '', regex=True)
    )
    # errors='coerce' превратит некорректные строки в NaN
    df_unit_short['ФБО'] = pd.to_numeric(df_unit_short['ФБО'], errors='coerce')
    
    # Заполняем пустоты нулями и приводим к int
    df_unit_short['ФБО'] = df_unit_short['ФБО'].fillna(0).astype(int)
    df_unit_short = df_unit_short.groupby('wild').agg({  
        'ФБО': 'sum'
    }).reset_index()
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(plan.annual_plan_connect_to_unit_sheet, df_unit_short)

def transport_supplies_data_to_annual_procurement_plan():
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_supplies = plan.get_supplies_data()
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(plan.annual_plan_connect_to_supply_sheet, df_supplies)