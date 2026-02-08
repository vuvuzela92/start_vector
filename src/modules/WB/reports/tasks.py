# python -m src.modules.WB.report.tasks
import asyncio
import os
from src.core.utils_general import load_api_tokens
from src.core.utils_gspread import safe_open_spreadsheet
from src.modules.WB.reports.api import fetch_orders_info
from src.modules.WB.reports.processing import process_orders_info


def orders_report_today():
    """Полчает данные по отчету orders"""
    tokens = load_api_tokens()

    orders_info = asyncio.run(fetch_orders_info(tokens))

    df = process_orders_info(orders_info)

    orders_df_group = df.groupby('account', as_index=False).agg({'date': 'first',
                                  'priceWithDisc': 'sum',
                                  'finishedPrice': 'count', 
                                    })
    orders_df_group = orders_df_group.rename(columns={'priceWithDisc': 'revenue',
                                    'finishedPrice': 'count'})
    
    # Подключение к гугл-таблице Панель управления продажами Вектор
    table = safe_open_spreadsheet(os.getenv("AUTOPILOT_TABLE_NAME"))
    sheet = table.worksheet(os.getenv("AUTOPILOT_SHEET_ORDERS"))

    # Отправка датафрейма в таблицу
    from gspread_dataframe import set_with_dataframe
    set_with_dataframe(sheet, orders_df_group)

