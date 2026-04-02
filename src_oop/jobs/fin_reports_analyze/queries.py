"""Запросы к БД для Финансового анализа"""

query_deductions_by_month = """
SELECT * FROM deductions_by_month
    """

query_cash_flow_writeoffs = """
SELECT *
FROM cash_flow_writeoffs c
WHERE c.is_valid IS TRUE
ORDER BY c.date DESC;
"""