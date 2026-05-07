from src_oop.jobs.calculation_of_purchases_russia.calculation_of_purchases_russia import Calculation_of_purchases_russia
from src_oop.jobs.calculation_of_purchases_russia.config import query

def set_orders_quantity():
    calc = Calculation_of_purchases_russia()
    df = calc.db.read_sql_to_dataframe(query)
    df['date'] = df['date'].astype(str)
    calc.google_connect.set_df_to_google(df)

def transport_orders_and_supply():
    # Создаем экземпляр класса и получаем данные
    clc = Calculation_of_purchases_russia()
    df = clc.get_orders_and_supplies_data()
    df = df.fillna(0)
    # Обновляем данные в таблице Годовой план закупа 2026
    clc.set_data(clc.google_connect_to_purchsase_russia_table, df)

# python -m src_oop.jobs.calculation_of_purchases_russia.run
# if __name__ == "__main__":
#     transport_orders_and_supply()