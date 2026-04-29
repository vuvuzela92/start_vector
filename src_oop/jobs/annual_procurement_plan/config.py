from sqlalchemy import text

fin_rep_analyze = {
                   "title" : "Анализ_фин_отчетов_Вектор",
                   "weekly_rep" : "отчет_по_неделям",
                   "monthly_rep" : "отчет_по_месяцам",
                   "outcomes_detalize" : "детализация_расходов",
                   "deducations_detalize": "удержания_детализация",
                   "deductions_by_month" : "удержания_детализация_месяц",
                   "query_cash_flow_writeoffs": "расходы_по_банку",
                   "stock_analyze": "анализ_остатков"
                   }

delivery_calculation_china = {
                        "title": "Расчет поставки Китай_по обороту",
                        "white_orders_sheet": "Заказы белые ТЕСТ",
                        "orders_sheet": "Заказы"
                            }

annual_procurement_plan = {
                        "title": "Годовой план закупа 2026",
                        "orders_sheet": "БД_ЗАКАЗЫ",
                        "unit_sheet": "Данные_Юнитки",
                        "supply_sheet": "Данные_Поставки"
                            }

unit_gs = {
    "title": "UNIT 2.0 (tested)",
    "unit_sheet": "MAIN (tested)"
}

supplies_query = text("""
    SELECT DATE(s.supply_date) AS supply_date,
            s.local_vendor_code,
        sum(s.quantity) AS quantity
    FROM supply_to_sellers_warehouse s
    WHERE DATE(s.supply_date) BETWEEN '2026-03-15'
        AND '2026-04-15'
        AND s.is_valid is TRUE
    GROUP BY DATE(s.supply_date),
            s.local_vendor_code;
        """)