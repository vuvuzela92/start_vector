import pandas as pd

def process_orders_info(orders_info):
    """ Функция обрабатывает полученные данные по отчетам orders"""
    if orders_info:
        # Список для хранения данных о заказах в виде словарей
        orders_list = []
        # Проходим внешним циклом по данным каждого ЛК
        for orders_lk in orders_info:
            # Достаем отдельно данные по каждому заказу и добваляем в общий список
            for orders in orders_lk:
                orders_list.append(orders)


        df = pd.DataFrame(orders_list)
        return df
    else:
        return pd.DataFrame() 