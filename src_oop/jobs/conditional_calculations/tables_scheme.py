from sqlalchemy import Date, String, BigInteger


# Колонки Условного расчета
conditional_calculations ={
        "title": "conditions_calculation",
        "columns": {
        'account': String(255),
        'orders_sum': BigInteger,
        'sales_sum': BigInteger,
        'profit_by_ind_cond_orders': BigInteger,
        'profit_by_ind_cond_sales': BigInteger,
        'sales_count': BigInteger,
        'order_count': BigInteger,
        'adv_spend': BigInteger,
        'bonuses': BigInteger,
        'profit_cond_sales_minus_adv_spend': BigInteger,
        'cost_price_orders': BigInteger,
        'cost_price_sales': BigInteger,
        'general_profit_orders': BigInteger,
        'date': Date
    },
    "unique_keys": ["date", "account"]
}