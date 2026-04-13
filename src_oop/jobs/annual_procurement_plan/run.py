# Импорт внутренних модулей
from src_oop.jobs.annual_procurement_plan.annual_procurement_plan import Annual_procurement_plan
from src_oop.core.utils_general import clean_currency_value
# Импорт внешних библиотек
import pandas as pd
from datetime import datetime

def transport_data_to_annual_procurement_plan():
    # Создаем экземпляр класса и получаем данные
    plan = Annual_procurement_plan()
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
    cancel_statuses = ["отмена", "в планах", "прибыло"]
    # Фильтрация
    df_merge = df_merge.loc[~df_merge['Статус'].isin(cancel_statuses)]
    # Добавляем колонку, указывающую на время обновления
    df_merge["updatet_at"] = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(df_merge)