from src_oop.jobs.calculation_of_purchases_russia.calculation_of_purchases_russia import (
    Calculation_of_purchases_russia,
)
from src_oop.jobs.calculation_of_purchases_russia.config import query


def set_orders_quantity() -> None:
    calc = Calculation_of_purchases_russia()
    df = calc.db.read_sql_to_dataframe(query)
    df["date"] = df["date"].astype(str)
    calc.google_connect.set_df_to_google(df)


def transport_orders_and_supply() -> None:
    calc = Calculation_of_purchases_russia()
    df = calc.get_orders_and_supplies_data()
    df = df.fillna(0)
    calc.set_data(calc.google_connect_to_purchsase_russia_table, df)


def update_penalties_in_gs_purchase_russia() -> None:
    calc = Calculation_of_purchases_russia()
    calc.update_penalties_and_virtual_stock()


# python -m src_oop.jobs.calculation_of_purchases_russia.run
