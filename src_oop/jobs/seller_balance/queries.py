"""SQL-запросы job выгрузки баланса и финансовых начислений."""

from __future__ import annotations

# Агрегация начислений нужна формулам ДДС на уровне даты и кабинета.
FINANCIAL_REPORTS_QUERY = """
SELECT
    DATE(f.date_from) AS date,
    UPPER(TRIM(f.account)) AS account,
    ROUND(
        SUM(
            CASE
                WHEN f.doc_type_name = 'Продажа'
                    THEN COALESCE(f.ppvz_for_pay, 0)
                ELSE 0
            END
        )
        - SUM(
            CASE
                WHEN f.doc_type_name = 'Возврат'
                    THEN COALESCE(f.ppvz_for_pay, 0)
                ELSE 0
            END
        )
        - SUM(COALESCE(f.delivery_rub, 0))
        - SUM(COALESCE(f.penalty, 0))
        - SUM(COALESCE(f.deduction, 0))
    ) AS total_to_pay
FROM public.daily_fin_reports_full AS f
WHERE DATE(f.date_from) BETWEEN :date_from
    AND CURRENT_DATE - INTERVAL '1 day'
    AND f.account IS NOT NULL
    AND f.account != '0'
    AND f.account != 'NaN'
GROUP BY
    DATE(f.date_from),
    UPPER(TRIM(f.account))
"""
