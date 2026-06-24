from __future__ import annotations

from sqlalchemy import text

WEEK_N_REDEEM_QUERY = text(
    """-- === Для бухгалтерии ===
WITH week_rep AS (
    SELECT
        SUM(CASE WHEN w.title = 'Итого стоимость реализованного товара и услуг' THEN COALESCE(w.sum_rub, 0) ELSE 0 END) AS total_sum,
        ROUND(
            SUM(
                CASE
                    WHEN w.title = 'Итого стоимость реализованного товара и услуг'
                        THEN COALESCE(w.sum_rub, 0) / (1.0 + v.vat)
                    ELSE 0
                END
            ),
            2
        ) AS total_sum_without_vat,
        SUM(CASE WHEN w.title = 'Компенсация ущерба' THEN COALESCE(w.sum_rub, 0) ELSE 0 END) AS damages_comp,
        SUM(CASE WHEN w.title = 'Прочие выплаты' THEN COALESCE(w.sum_rub, 0) ELSE 0 END) AS other_payments,
        SUM(CASE WHEN w.title = 'Компенсация скидки по программе лояльности' THEN COALESCE(w.sum_rub, 0) ELSE 0 END) AS discount_loyalty,
        SUM(
            CASE
                WHEN w.title IN (
                    'Сумма вознаграждения Вайлдберриз за текущий период (ВВ), без НДС',
                    'НДС с вознаграждения Вайлдберриз'
                )
                    THEN COALESCE(w.sum_rub, 0)
                ELSE 0
            END
        ) AS award,
        SUM(
            CASE
                WHEN w.title IN ('Сумма, удержанная в счёт обеспечения организации платежа')
                    THEN COALESCE(w.sum_rub, 0)
                ELSE 0
            END
        ) AS amount_withheld_to_org,
        SUM(
            CASE
                WHEN w.title IN ('Возмещение расходов по перевозке')
                    THEN COALESCE(w.sum_rub, 0)
                ELSE 0
            END
        ) AS reimbursement_of_transp_costs,
        SUM(
            CASE
                WHEN w.title IN ('Возмещение за выдачу и возврат товаров на ПВЗ')
                    THEN COALESCE(w.sum_rub, 0)
                ELSE 0
            END
        ) AS reimbursement_for_delivery_and_return_of_goods_to_pvz,
        SUM(CASE WHEN w.title IN ('Штрафы') THEN COALESCE(w.sum_rub, 0) ELSE 0 END) AS penalties,
        SUM(CASE WHEN w.title IN ('Прочие удержания') THEN COALESCE(w.sum_rub, 0) ELSE 0 END) AS other_deductions,
        SUM(
            CASE
                WHEN w.title IN ('Удержания в пользу третьих лиц')
                    THEN COALESCE(w.sum_rub, 0)
                ELSE 0
            END
        ) AS retentions_in_favor_of_third_parties,
        w.doc_num AS weekly_rep,
        DATE(DATE_TRUNC('week', w."date")) AS week_start,
        DATE(w."date") AS report_date,
        DATE((DATE_TRUNC('week', w."date") + INTERVAL '7 days' - INTERVAL '1 microsecond')) AS week_end,
        w.account
    FROM weekly_implementation_report w
    LEFT JOIN vat_guide v
        ON v.account = UPPER(w.account)
    GROUP BY w.doc_num, w."date", w.account
),
fin_rep AS (
    SELECT
        f.realizationreport_id,
        f.date_from,
        f.date_to,
        f.create_dt,
        SUM(
            CASE
                WHEN f.doc_type_name ILIKE '%возврат%'
                    THEN COALESCE(f.ppvz_for_pay, 0)
                ELSE 0
            END
        ) AS return_pay,
        f.account
    FROM fin_reports_full f
    WHERE f.report_type = 2
    GROUP BY f.realizationreport_id, f.date_from, f.date_to, f.create_dt, f.account
),
redeem_not AS (
    SELECT
        r.account,
        SUM(r.sum_rub_with_vat) AS sum_rub_with_vat,
        ROUND(SUM(r.sum_rub_with_vat) / (1.0 + v.vat), 2) AS sum_rub_without_vat,
        SUBSTRING(r.doc_name FROM '№(\\d+)') AS redeem_notif,
        DATE(SUBSTRING(r.doc_name FROM ' от (\\d{4}-\\d{2}-\\d{2})')) AS redeem_notif_date
    FROM redeem_notification r
    LEFT JOIN vat_guide v
        ON v.account = UPPER(r.account)
    GROUP BY r.account, r.doc_name, v.vat
)
SELECT
    w.account,
    w.weekly_rep AS "Номер_еженедельного_отчета",
    w.week_start AS "Начало_периода",
    w.report_date AS "Конец_периода",
    COALESCE(w.total_sum, 0) AS "Всего_товара",
    COALESCE(w.total_sum_without_vat, 0) AS "Всего_товара_БЕЗ_НДС",
    COALESCE(w.damages_comp, 0) AS "Компенсации_ущерба",
    COALESCE(w.other_payments, 0) AS "Прочие_выплаты",
    COALESCE(w.discount_loyalty, 0) AS "Компенсация_скидки_по_программе_лояльности",
    r.redeem_notif AS "Уведомление_о_выкупе_№",
    COALESCE(r.sum_rub_with_vat, 0) AS "Выкуплено_по_уведомлению",
    COALESCE(r.sum_rub_without_vat, 0) AS "Выкуплено_по_уведомлению_без_НДС",
    COALESCE(f.return_pay, 0) AS "Вовзрат_выкупа",
    CASE WHEN COALESCE(w.award, 0) < 0 THEN w.award * -1 ELSE 0 END AS "Вознагрожденение_в_доход",
    CASE
        WHEN COALESCE(w.award, 0) < 0
            THEN ROUND((w.award * -1) / (1.0 + v.vat), 2)
        ELSE 0
    END AS "Вознагрожденение_в_доход_БЕЗ_НДС",
    COALESCE(w.award, 0) AS "Вознаграждение",
    COALESCE(w.amount_withheld_to_org, 0) AS "Сумма_удержанная_в_счёт_обеспечения_организации_платежа",
    COALESCE(w.reimbursement_of_transp_costs, 0) AS "Возмещение расходов по перевозке",
    COALESCE(w.reimbursement_for_delivery_and_return_of_goods_to_pvz, 0) AS "Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ",
    COALESCE(w.penalties, 0) AS "Штрафы",
    COALESCE(w.other_deductions, 0) AS "Прочие удержания",
    COALESCE(w.retentions_in_favor_of_third_parties, 0) AS "Удержания_в_пользу_третьих_лиц"
FROM week_rep w
LEFT JOIN fin_rep f
    ON UPPER(w.account) = UPPER(f.account)
   AND w.report_date = f.date_to
   AND f.date_to > DATE '2025-01-31'
LEFT JOIN redeem_not r
    ON f.realizationreport_id = r.redeem_notif::INT
   AND UPPER(w.account) = UPPER(r.account)
LEFT JOIN vat_guide v
    ON v.account = UPPER(w.account)
WHERE w.report_date > DATE '2025-01-31'
ORDER BY w.week_start DESC, w.account;
"""
)
