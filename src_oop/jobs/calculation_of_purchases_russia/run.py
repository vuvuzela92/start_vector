from src_oop.jobs.calculation_of_purchases_russia.calculation_of_purchases_russia import Calculation_of_purchases_russia
from src_oop.jobs.calculation_of_purchases_russia.config import query

def set_orders_quantity():
    calc = Calculation_of_purchases_russia()
    df = calc.db.read_sql_to_dataframe(query)
    calc.google_connect.set_df_to_google(df)