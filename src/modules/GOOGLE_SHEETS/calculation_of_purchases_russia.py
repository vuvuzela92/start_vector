from pathlib import Path
import os
import pandas as pd
from sqlalchemy import text
from src.core.google_sheets_scheme import calculation_of_purchases_russia_table, unit_table
from src.core.utils_gspread import safe_open_spreadsheet
from src.core.utils_sql import get_db_engine
from gspread.utils import rowcol_to_a1


def update_penalties_in_gs_purchase_russia():
    """ Функция для получения данных о штрафах по основаниям брак, невыполненный заказ и подмены за последние 7 дней из базы данных и обновления этих данных в Google Sheets. 
    
    Функция выполняет следующие шаги:
    1. Получает данные о штрафах из базы данных за последние 7 дней по определенным основаниям.
    2. Загружает данные из таблицы UNIT  для таблицы закупок и таблицы UNIT 2.0 (tested) для получения информации о виртуальных остатках."""
    # Получаем подключение к базе данных
    engine = get_db_engine()
    # Выполняем SQL-запрос для получения данных о штрафах по основаниям брак, невыполненный заказ и подменах за последние 7 дней
    query = text("""SELECT  
        sum(f.penalty), -- 250,935.92
        a.local_vendor_code
    FROM daily_fin_reports_full f
    LEFT JOIN article a 
        USING(nm_id)
    WHERE f.create_dt BETWEEN CURRENT_DATE - INTERVAL '7 days'
    AND CURRENT_DATE
    AND f.penalty != 0
    AND f.bonus_type_name ILIKE ANY (ARRAY[
        '%брак%',
        '%невыполненный%',
        '%подмена%'
    ])
    GROUP BY a.local_vendor_code;""")

    # Загружаем данные в DataFrame
    df = pd.read_sql(query, engine)

    # Открываем таблицу закупок Google Sheets
    table_purchase = safe_open_spreadsheet(calculation_of_purchases_russia_table['title'])
    # Получаем данные из листа
    sheet_purchase = table_purchase.worksheet(calculation_of_purchases_russia_table['sheet_calculation_of_purchases'])
    # Получаем все данные из листа
    sheet_purchase_data = sheet_purchase.get_all_values()
    # Преобразуем данные в DataFrame, пропуская первые две строки (заголовки и описание) и используя вторую строку в качестве заголовков столбцов
    sheet_purchase_df = pd.DataFrame(sheet_purchase_data[2:], columns=sheet_purchase_data[1])
    # Выбираем колонки по индексам
    sheet_purchase_df = sheet_purchase_df.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]

    # Открываем таблицу юнит Google Sheets
    table_unit = safe_open_spreadsheet(unit_table['title'])
    # Получаем данные из листа юнит
    sheet_unit = table_unit.worksheet(unit_table['sheet_unit'])
    # Получаем все данные из листа юнит
    sheet_unit_data = sheet_unit.get_all_values()
    # Преобразуем данные в DataFrame, пропуская первые две строки (заголовки и описание) и используя вторую строку в качестве заголовков столбцов
    sheet_unit_df = pd.DataFrame(sheet_unit_data[1:], columns=sheet_unit_data[0])
    # Выбираем колонки по индексам
    sheet_unit_df = sheet_unit_df[['wild', 'ФБС']]
    # Группируем данные по колонке 'wild', суммируя значения в колонке 'ФБС' для каждой группы
    # Перед группировкой обрабатываем пропуски в колонке 'ФБС', заменяя пустые строки и NaN на 0, а затем приводим значения к целочисленному типу
    sheet_unit_df['ФБС'] = (
    sheet_unit_df['ФБС']
        .replace('', 0)         
        .fillna(0)               
        .astype(int)
        )
    sheet_unit_df = sheet_unit_df.groupby('wild', as_index=False).agg({'ФБС': 'sum'})

    # Удаляем дубликаты по колонке 'wild', оставляя только первую встречающуюся запись для каждого уникального значения в этой колонке
    sheet_unit_df = sheet_unit_df.drop_duplicates(subset=['wild'], keep='first')
    # Объединяем данные из Google Sheets с данными из базы данных по колонке 'local_vendor_code' и 'wild'
    merge_df = (
        sheet_purchase_df
            .merge(df, how='left', left_on='wild', right_on='local_vendor_code')
            .merge(sheet_unit_df, how='left', on='wild')
    )

    # Обрабатываем пропуски
    merge_df = merge_df.fillna("")
    # Переименовываем колонку 'sum' в 'Штраф (по основанию брак, невыполненный заказ и подмена)'
    merge_df['Штраф (по основанию брак, невыполненный заказ и подмена)'] = merge_df['sum']

    merge_df['Кол-во товара по виртуальным остаткам'] = merge_df['ФБС']
    # Удаляем колонку 'sum' и 'local_vendor_code'
    # merge_df = merge_df.drop(columns=['sum', 'local_vendor_code', 'ФБС'])
    merge_df = merge_df[['Штраф (по основанию брак, невыполненный заказ и подмена)', 'Кол-во товара по виртуальным остаткам']]
    # Получаем координаты ячейки, с которой нужно начать обновление данных в Google Sheets
    start_update = sheet_purchase.find("Штраф (по основанию брак, невыполненный заказ и подмена)")
    row_start_update = start_update.row
    col_start_update = start_update.col
    start_cell = rowcol_to_a1(row_start_update+1, col_start_update)

    # Преобразуем DataFrame в список списков для обновления Google Sheets
    values = merge_df.values.tolist()
    # Обновляем данные в Google Sheets, начиная с ячейки A2
    sheet_purchase.update(values, f"{start_cell}")
    print("Данные о штрафах и виртуальных остатках успешно обновлены в Google Sheets.")          