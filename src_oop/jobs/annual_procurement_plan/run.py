# Импорт внутренних модулей
from src_oop.jobs.annual_procurement_plan.annual_procurement_plan import AnnualProcurementPlan
from src_oop.core.utils_general import clean_currency_value
# Импорт внешних библиотек
import pandas as pd

def _select_orders_columns(df_source: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    """Возвращает набор колонок заказа для выгрузки в годовой план закупа.

    Вспомогательная функция защищает основной сценарий переноса заказов в
    вкладку ``БД_ЗАКАЗЫ``: если одна из ожидаемых колонок временно отсутствует
    в листе-источнике, она создается пустой, чтобы обновление не прерывалось и
    структура выгрузки оставалась стабильной.
    """
    df_result = df_source.copy()
    missing_columns = [column for column in required_columns if column not in df_result.columns]

    for column in missing_columns:
        df_result[column] = ""

    return df_result[required_columns]


def transport_data_to_annual_procurement_plan():
    """Переносит заказы из рабочих листов в вкладку ``БД_ЗАКАЗЫ`` годового плана.

    Сценарий объединяет данные из листов ``Заказы белые ТЕСТ`` и ``Заказы``,
    сохраняет только бизнес-значимые колонки заказа, очищает сумму заказа,
    исключает отмененные и завершенные статусы и формирует итоговую выгрузку
    для дальнейшей работы закупки и аналитики. Поле ``Поставщик`` также
    включается в итоговую структуру выгрузки.
    """
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_white_orders = plan.get_white_orders_data()
    # Выбираем нужные колонки
    choosen_orders_columns = plan.choosen_orders_columns
    df_white_orders_short = _select_orders_columns(df_white_orders, choosen_orders_columns)
    # Получаем датафрейм от листа Заказы в таблице Расчет поставки Китай_по обороту
    df_orders = plan.get_orders_data()
    df_orders_short = _select_orders_columns(df_orders, choosen_orders_columns)
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

def transport_parfume_data_to_annual_procurement_plan():
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_parfume = plan.get_parfume_data()
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(plan.annual_plan_connect_to_parfume_sheet, df_parfume)
