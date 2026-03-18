from src_oop.core.my_gspread import GoogleTabs
from src_oop.storage.google_sheets.google_sheets import delivery_calculation_china, annual_procurement_plan
import pandas as pd
from datetime import datetime
import gspread


def transport_data_to_annual_procurement_plan():
    # Создаем соединение с гугл-таблицей из таблицы из которой забираем данные
    google_table_from = delivery_calculation_china.get("title")
    table_sheet_from = delivery_calculation_china.get("white_orders_sheet")
    google_connect_from = GoogleTabs(table_title=google_table_from, sheet_title=table_sheet_from)

    # Считываем данные из таблицы из которой забираем данные
    table_sheet_from_data = google_connect_from.sheet_title.get_all_values()
    #  Создаем датафрейм
    df = pd.DataFrame(table_sheet_from_data[4:], # Данные начиная с 5-й строки
                    columns=table_sheet_from_data[3]) # Названия колонок с 4-й строки

    # Выбираем нужные колонки
    # choosen_columns = ["wild", "Статус", "Кол-во к заказу"]
    cancel_statuses = ["отмена", "в планах", "прибыло"]
    # Фильтрация
    df_short = df.loc[~df['Статус'].isin(cancel_statuses)]
    df_short["updatet_at"] = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    # Определяем таблицу и лист для вставки данных
    google_table_to = annual_procurement_plan.get("title")
    table_sheet_to = annual_procurement_plan.get("orders_sheet")


    # Создаем соединение с гугл-таблицей из таблицы из которой забираем данные
    try:
        # Создаем соединение с гугл-таблицей
        google_connect_to = GoogleTabs(table_title=google_table_to, sheet_title=table_sheet_to)
        # Вставляем данные в гугл-таблицу
        google_connect_to._update_df_in_google(df_short, google_connect_to.sheet_title)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {google_table_to}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {table_sheet_to} в таблице {google_table_to}")
    except StopIteration:
        print(f"Не найден лист {table_sheet_to} в таблице {google_table_to}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}")