import pandas as pd
from sqlalchemy import text
from src.core.google_sheets_scheme import credit_analyze_vector_table
from src.core.utils_gspread import safe_open_spreadsheet
from src.core.utils_sql import get_db_engine
from gspread.utils import rowcol_to_a1
from gspread_dataframe import set_with_dataframe 


# python -m src.modules.GOOGLE_SHEETS.credit_analyze_vector
def update_credit_data_vector():
    """ Функция для получения данных о кредитах из БД и загрузки их в Google Таблицы для дальнейшего анализа."""
    # Получаем подключение к базе данных
    engine = get_db_engine()
    # Выполняем SQL-запрос для получения данных о штрафах по основаниям брак, невыполненный заказ и подменах за последние 7 дней
    query = text("""SELECT 
            f.account 
            , f.realizationreport_id
            ,f.create_dt
            ,f.bonus_type_name
            ,substring(f.bonus_type_name FROM '([0-9]{10,})') AS doc_number
            ,SUM(CASE WHEN f.bonus_type_name ILIKE '%основного долга%' THEN f.deduction ELSE 0 END) AS credit_body
            ,SUM(CASE WHEN f.bonus_type_name ILIKE '%оплаты процентов%' THEN f.deduction ELSE 0 END) AS credit_percent
            ,SUM(CASE WHEN f.bonus_type_name ILIKE '%комисс%' THEN f.deduction ELSE 0 END) AS comission
            ,f.date_from
            ,f.date_to
        FROM fin_reports_full f
        WHERE f.create_dt >= '2025-01-01'
        AND f.deduction != 0
        AND f.bonus_type_name ILIKE ANY (array['%заём%', '%займ%', '%кредит%', '%комисс%'])
        GROUP BY f.account 
            , f.realizationreport_id
            , f.create_dt
            , f.date_from
            , f.date_to
            , f.bonus_type_name	
            , f.create_dt
        HAVING substring(f.bonus_type_name FROM '([0-9]{10,})') IS NOT NULL;""")    

    # Загружаем данные в DataFrame
    df = pd.read_sql(query, engine)

    # Открываем таблицу 
    credit_table = safe_open_spreadsheet(credit_analyze_vector_table['title'])
    # Получаем данные из листа
    credits_sheet = credit_table.worksheet(credit_analyze_vector_table['sheet_credit_analyze_vector'])
    set_with_dataframe(credits_sheet, df)
