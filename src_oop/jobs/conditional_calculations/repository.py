# src_oop/jobs/analytics/repository.py
from sqlalchemy import text
from src_oop.core.database import Database


class ConditionalCalculationsRepository:
    def __init__(self, days_ago: int = 84, days_to: int = 1):
        self.days_ago = days_ago
        self.days_to = days_to
    
    def execute_conditional_calculations(self):
        """Функция для пересчета данных Условного расчета за указанное количество дней"""
        query = text(f"""
        SELECT
            o.account,
            SUM(o.orders_sum_rub) AS orders_sum,
            ROUND(SUM(o.sales_revenue_rep)) AS sales_sum,
            ROUND(SUM(o.profit_by_cond_orders)) AS profit_by_ind_cond_orders,
            ROUND(SUM(o.sales_profit_cond_rep)) AS profit_by_ind_cond_sales,
            SUM(o.sales_count_rep) AS sales_count,
            SUM(o.orders_count) AS order_count,
            SUM(o.adv_spend) AS adv_spend,
            SUM(o.bonuses) AS bonuses,
            ROUND(SUM(o.profit_by_cond_sales) - SUM(o.adv_spend)) AS profit_cond_sales_minus_adv_spend,
            ROUND(SUM(o.orders_count * o.purchase_price)) AS cost_price_orders,
            ROUND(SUM(o.sales_count_rep * o.purchase_price)) AS cost_price_sales,
            SUM(o.profit_by_orders) AS general_profit_orders,
            o.date
        FROM orders_articles_analyze o
        WHERE o.date BETWEEN CURRENT_DATE - INTERVAL '{self.days_ago} days'
            AND CURRENT_DATE - INTERVAL '{self.days_to} days'
            AND o.account != '0' 
            AND o.account IS NOT NULL
            AND o.account != 'NaN'
        GROUP BY o.account,
                o.date;
    """)

        return Database.read_sql_to_dataframe(query)
    

    def get_conditional_calculations(self):
        """Получаем данные для вставки в гугл-таблицу Условного расчета"""
        query = text(f"""
                    SELECT * FROM conditions_calculation cc
        WHERE cc.date >= '2025-12-01'
        ORDER BY cc.date ASC,
            cc.account;
        """)
        return Database.read_sql_to_dataframe(query) 