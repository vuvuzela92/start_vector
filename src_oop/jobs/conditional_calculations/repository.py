import pandas as pd
from sqlalchemy import text

class GetDataFromDB:
    """Класс для получения сложной аналитики из БД."""
    
    def __init__(self, engine):
        self.engine = engine

    def execute_conditional_calculations(self, days_ago: int = 30, days_to: int = 1):
        query = text(f"""
        SELECT
            o.account,
            SUM(o.orders_sum_rub) AS orders_sum,
            ROUND(SUM(o.sales_sum)) AS sales_sum,
            ROUND(SUM(o.profit_by_cond_orders)) AS profit_by_ind_cond_orders,
            ROUND(SUM(o.profit_by_cond_sales)) AS profit_by_ind_cond_sales,
            SUM(o.sales_count) AS sales_count,
            SUM(o.orders_count) AS order_count,
            SUM(o.adv_spend) AS adv_spend,
            SUM(o.bonuses) AS bonuses,
            ROUND(SUM(o.profit_by_cond_sales) - SUM(o.adv_spend)) AS profit_cond_sales_minus_adv_spend,
            ROUND(SUM(o.orders_count * o.purchase_price)) AS cost_price_orders,
            ROUND(SUM(o.sales_count * o.purchase_price)) AS cost_price_sales,
            SUM(o.profit_by_orders) AS general_profit_orders,
            o.date
        FROM orders_articles_analyze o
        WHERE o.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days'
            AND CURRENT_DATE - INTERVAL '{days_to} days'
            AND o.account != '0'
        GROUP BY o.account,
                o.date;
    """)
        
        # Используем контекстный менеджер соединения
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)
        

    def get_conditional_calculations(self, days_ago: int = 30, days_to: int = 1):
            query = text(f"""
                        SELECT * FROM conditions_calculation cc
            WHERE cc.date >= '2025-12-01'
            ORDER BY cc.date ASC,
                cc.account;
            """)
            
            # Используем контекстный менеджер соединения
            with self.engine.connect() as connection:
                return pd.read_sql(query, connection)   