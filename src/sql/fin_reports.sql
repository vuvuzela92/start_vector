SELECT 
    SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) AS "К перечислению за продажу",
    SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) AS "К перечислению за возврат",
    SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) - 
    SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) AS "Итого к перечислению",
    SUM(frf.delivery_rub ) AS "Логистика",
    SUM(frf.penalty) AS "Штрафы" ,
    SUM(frf.deduction) AS "Удержания",
    SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) - 
    SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) - SUM(frf.delivery_rub ) - SUM(frf.penalty) - SUM(frf.deduction) AS "Итого к оплате"
FROM fin_reports_full frf 
WHERE frf.realizationreport_id = 448960367; -- Дашборд по отчету

SELECT 
    SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) - 
    SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) AS "Итого к перечислению",
    SUM(frf.delivery_rub ) AS "Логистика",
    SUM(frf.penalty) AS "Общая сумма штрафов" ,
    SUM(frf.deduction) AS "Удержания",
    SUM(frf.storage_fee) AS "Хранение",
    SUM(frf.acceptance) AS "Платная приёмка",
    SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) - 
    SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) - SUM(frf.delivery_rub ) - SUM(frf.penalty) - SUM(frf.deduction) AS "Итого к оплате",
    account,
    '2025-06-01 - 2025-07-31' AS "Период"
FROM fin_reports_full frf 
WHERE frf.create_dt BETWEEN '2025-06-01' AND '2025-07-31'
GROUP BY account;


----------------------------

SELECT create_dt AS "Дата отчета",
	realizationreport_id AS "Номер отчета",
	SUM(frf.storage_fee) AS "Хранение",
	account AS "ЛК"
FROM fin_reports_full frf
WHERE frf.supplier_oper_name = 'Хранение'
OR frf.supplier_oper_name = 'Коррекция хранения'
GROUP BY create_dt, realizationreport_id, account
ORDER BY create_dt;--Хранение


SELECT create_dt AS "Дата отчета",
	realizationreport_id AS "Номер отчета",
	SUM(frf.acceptance) AS "Платная приёмка",
	account AS "ЛК"
FROM fin_reports_full frf
WHERE frf.supplier_oper_name = 'Платная приемка'
OR frf.supplier_oper_name = 'Пересчет платной приемки'
GROUP BY create_dt, realizationreport_id, account
ORDER BY create_dt;--Платная приемка


SELECT create_dt AS "Дата отчета",
	realizationreport_id AS "Номер отчета",
	SUM(frf.delivery_rub) AS "Логистика",
	account AS "ЛК"
FROM fin_reports_full frf
WHERE frf.delivery_rub != 0 
AND frf.doc_type_name  = 'Продажа'
OR frf.doc_type_name  = ''
AND frf.supplier_oper_name = 'Логистика'
AND frf.bonus_type_name = 'К клиенту при продаже'
GROUP BY create_dt, realizationreport_id, account
ORDER BY create_dt;--Логистика к клиенту


SELECT o.supplier_article, o.date_from, SUM(CASE WHEN o.is_realization THEN 1 ELSE 0 END) AS count_orders,
	a.account 
FROM orders o
LEFT JOIN article a 
ON o.article_id = a.nm_id 
WHERE o."date" = '2025-09-01'
AND supplier_article ILIKE '%wild128%'
AND a.account = 'ХАЧАТРЯН'
GROUP BY  o.date_from, supplier_article, a.account;


SELECT DISTINCT frf.supplier_oper_name 
FROM fin_reports_full frf ;


SELECT DISTINCT frf.create_dt 
FROM fin_reports_full frf 
ORDER BY create_dt DESC;

REFRESH MATERIALIZED VIEW check_act_fbs;


SELECT date_trunc('month', f.date_from) AS month,
	f.supplier_oper_name,
	SUM(f.retail_price_withdisc_rub) 
FROM daily_fin_reports_full f 
WHERE date_trunc('month', f.date_from) > '2025-01-08'
AND f.supplier_oper_name = 'Продажа'
GROUP BY date_trunc('month', f.date_from), f.supplier_oper_name
ORDER BY MONTH DESC
;

SELECT date_trunc('month', f.date_from) AS month,
	f.supplier_oper_name,
	SUM(f.retail_price_withdisc_rub) 
FROM fin_reports_full f 
WHERE date_trunc('month', f.date_from) > '2025-01-08'
AND f.supplier_oper_name = 'Продажа'
GROUP BY date_trunc('month', f.date_from), f.supplier_oper_name
ORDER BY MONTH DESC
;


SELECT 
	date_trunc('month', "date") AS month,
	SUM(s.price_with_disc) AS revenue_sales
FROM sales s
WHERE s."date" > '2025-01-01'
GROUP BY date_trunc('month', "date")
LIMIT 12;

WITH net_profit AS (SELECT date_trunc('month', "date") AS month,
	SUM(npfs.sum_by_sales) AS revenue_net
FROM net_profit_from_sales npfs 
WHERE npfs."date" > '2025-02-01'
GROUP BY date_trunc('month', "date")
LIMIT 12),
sale AS (SELECT 
	date_trunc('month', "date") AS month,
	SUM(s.price_with_disc) AS revenue_sales
FROM sales s
WHERE s."date" > '2025-02-01'
GROUP BY date_trunc('month', "date")
LIMIT 12)
SELECT revenue_net,
	revenue_sales,
	n."month" 
FROM net_profit n
LEFT JOIN sale sa
USING("month");


SELECT ROUND((f.total_to_pay
             - (COALESCE(f.purchase_cost_sales, 0) - COALESCE(f.purchase_cost_returns, 0))
             + f.credit_transfers
            ) / f.revenue,
        4) * 100
FROM daily_fin_reports_agg f
WHERE f.date_from > '2025-12-31'
LIMIT 5;


SELECT SUM(CASE WHEN f.supplier_oper_name = 'Продажа' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Коррекция продаж' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Добровольная компенсация при возврате' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция возвратов' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Возврат' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Компенсация ущерба' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Корректировка эквайринга' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Штраф' THEN f.penalty ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Удержание' THEN f.deduction ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Платная приемка' THEN f.acceptance ELSE 0 END)
     - (SUM(CASE WHEN f.supplier_oper_name = 'Логистика' THEN f.delivery_rub ELSE 0 END)
         + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция логистики' THEN f.delivery_rub ELSE 0 END)
        ) AS total_to_pay
FROM daily_fin_reports_full f
WHERE f.date_from = '2026-01-01'
LIMIT 5;


SELECT SUM(ppvz_for_pay),
	account
FROM daily_fin_reports_full f
WHERE f.date_from = '2026-01-01'
GROUP BY supplier_oper_name;

SELECT SUM(ppvz_for_pay)
FROM fin_reports_full f
WHERE f.date_from > '2025-01-01'
LIMIT 5;

SELECT *
FROM fin_reports_full f
WHERE f.date_from > '2025-01-01'
LIMIT 5;


SELECT SUM(CASE WHEN f.supplier_oper_name = 'Продажа' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Коррекция продаж' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Добровольная компенсация при возврате' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция возвратов' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Возврат' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Компенсация ущерба' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Корректировка эквайринга' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Штраф' THEN f.penalty ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Удержание' THEN f.deduction ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Платная приемка' THEN f.acceptance ELSE 0 END)
     - (SUM(CASE WHEN f.supplier_oper_name = 'Логистика' THEN f.delivery_rub ELSE 0 END)
         + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция логистики' THEN f.delivery_rub ELSE 0 END)
        ) AS total_to_pay
FROM fin_reports_full f
WHERE f.date_from BETWEEN '2025-12-01' AND '2025-12-31'
LIMIT 5;

SELECT SUM(f.retail_price_withdisc_rub) AS total_to_pay
FROM fin_reports_full f
WHERE f.date_from BETWEEN '2025-12-01' AND '2025-12-31'
AND f.supplier_oper_name IN('Продажа','Добровольная компенсация при возврате', 'Коррекция возвратов', 'Компенсация ущерба', 'Корректировка эквайринга', 'Коррекция логистики')
LIMIT 5;

SELECT SUM(f.retail_price_withdisc_rub) AS total_to_pay
FROM daily_fin_reports_full f
WHERE f.date_from BETWEEN '2025-12-01' AND '2025-12-31'
AND f.supplier_oper_name IN('Продажа','Добровольная компенсация при возврате', 'Коррекция возвратов', 'Компенсация ущерба', 'Корректировка эквайринга', 'Коррекция логистики')
LIMIT 5;


SELECT SUM(f.ppvz_for_pay) AS total_to_pay, -- 358 191 247,52
	SUM(f.retail_price_withdisc_rub) AS sales_sum, -- 514 868 241.56
	f.account 
FROM fin_reports_full f
WHERE f.date_from BETWEEN '2025-12-01' AND '2025-12-31'
GROUP BY f.account;

SELECT SUM(f.ppvz_for_pay) AS total_to_pay, -- 396 923 816,07
	SUM(f.retail_price_withdisc_rub) AS sales_sum, -- 570 732 460.77
	f.account 
FROM daily_fin_reports_full f
WHERE f.date_from BETWEEN '2025-12-01' AND '2025-12-31'
GROUP BY f.account;


SELECT f.date_from,
	f.account,
	SUM(CASE WHEN f.supplier_oper_name = 'Продажа' THEN f.ppvz_for_pay ELSE 0 END) AS "Продажа",
	SUM(f.ppvz_for_pay) AS "К перечислению"
FROM daily_fin_reports_full f
WHERE f.date_from > '2025-12-31'
AND f.supplier_oper_name = 'Продажа' 
AND f.account = 'Вектор'
GROUP BY f.date_from, f.account
LIMIT 16;


SELECT 
	f.date_from,
	f.account,
    SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) AS "К перечислению за продажу",
    SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) AS "К перечислению за возврат",
    SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) - 
    SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) AS "Итого к перечислению",
    SUM(f.delivery_rub ) AS "Логистика",
    SUM(f.penalty) AS "Штрафы" ,
    SUM(f.deduction) AS "Удержания",
    SUM(CASE WHEN doc_type_name = 'Продажа' THEN ppvz_for_pay ELSE 0 END) - 
    SUM(CASE WHEN doc_type_name = 'Возврат' THEN ppvz_for_pay ELSE 0 END) - SUM(f.delivery_rub ) - SUM(f.penalty) - SUM(f.deduction) AS "Итого к оплате"
FROM fin_reports_full f 
--WHERE f.realizationreport_id = 23947020260101
WHERE f.date_from > '2025-12-31'
GROUP BY f.date_from, f.account; -- Дашборд по отчету


SELECT SUM(f.deduction) AS "Удержания",
	f.bonus_type_name,
	f.supplier_oper_name,
	f.date_from,
	f.account 
FROM daily_fin_reports_full f
WHERE f.date_from BETWEEN '2026-01-23' AND '2026-01-25'
AND f.deduction != 0
GROUP BY f.supplier_oper_name, f.date_from, f.bonus_type_name, f.account
ORDER BY f.date_from; -- Отчет по удержаниям


SELECT *
FROM daily_fin_reports_full f
WHERE f.date_from > '2026-01-23'
AND f.deduction != 0;

SELECT
        f.date_from,
        SUM(CASE WHEN f.supplier_oper_name = 'Продажа' THEN f.retail_price_withdisc_rub ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Возврат' THEN f.retail_price_withdisc_rub ELSE 0 END)
     - (
            SUM(CASE WHEN f.supplier_oper_name = 'Продажа' THEN f.ppvz_for_pay ELSE 0 END)
         - SUM(CASE WHEN f.supplier_oper_name = 'Коррекция продаж' THEN f.ppvz_for_pay ELSE 0 END)
         + SUM(CASE WHEN f.supplier_oper_name = 'Добровольная компенсация при возврате' THEN f.ppvz_for_pay ELSE 0 END)
         + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция возвратов' THEN f.ppvz_for_pay ELSE 0 END)
         - SUM(CASE WHEN f.supplier_oper_name = 'Возврат' THEN f.ppvz_for_pay ELSE 0 END)
         + SUM(CASE WHEN f.supplier_oper_name = 'Компенсация ущерба' THEN f.ppvz_for_pay ELSE 0 END)
         + SUM(CASE WHEN f.supplier_oper_name = 'Корректировка эквайринга' THEN f.ppvz_for_pay ELSE 0 END)
        ) AS wb_commission,
        SUM(CASE WHEN f.supplier_oper_name = 'Продажа' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Коррекция продаж' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Добровольная компенсация при возврате' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция возвратов' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Возврат' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Компенсация ущерба' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Корректировка эквайринга' THEN f.ppvz_for_pay ELSE 0 END) AS payout,
        SUM(CASE WHEN f.supplier_oper_name = 'Логистика' THEN f.delivery_rub ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция логистики' THEN f.delivery_rub ELSE 0 END) AS logistics,
        SUM(CASE WHEN f.supplier_oper_name = 'Продажа' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Коррекция продаж' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Добровольная компенсация при возврате' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция возвратов' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Возврат' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Компенсация ущерба' THEN f.ppvz_for_pay ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Корректировка эквайринга' THEN f.ppvz_for_pay ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Штраф' THEN f.penalty ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Удержание' THEN f.deduction ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Платная приемка' THEN f.acceptance ELSE 0 END)
     - (
            SUM(CASE WHEN f.supplier_oper_name = 'Логистика' THEN f.delivery_rub ELSE 0 END)
         + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция логистики' THEN f.delivery_rub ELSE 0 END)
        ) AS total_to_pay,
        SUM(CASE WHEN f.supplier_oper_name = 'Продажа' THEN f.retail_price_withdisc_rub ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Возврат' THEN f.retail_price_withdisc_rub ELSE 0 END)
     - SUM(CASE WHEN f.supplier_oper_name = 'Коррекция возвратов' THEN f.retail_price_withdisc_rub ELSE 0 END)
     + SUM(CASE WHEN f.supplier_oper_name = 'Коррекция продаж' THEN f.retail_price_withdisc_rub ELSE 0 END) AS revenue,
        SUM(f.retail_price_withdisc_rub) AS retail_price_disc,
        SUM(f.penalty) AS penalties,
        SUM(f.storage_fee) AS storage_fee,
        SUM(f.deduction) AS deductions,
        SUM(f.acceptance) AS paid_acceptance,
        SUM(CASE WHEN f.bonus_type_name ILIKE '%кредит%' THEN f.deduction ELSE 0 END) AS credit_transfers,
        SUM(CASE WHEN f.bonus_type_name = 'К клиенту при отмене' THEN f.delivery_rub ELSE 0 END) AS to_client_cancel,
        SUM(CASE WHEN f.bonus_type_name = 'От клиента при отмене' THEN f.delivery_rub ELSE 0 END) AS from_client_cancel,
        SUM(CASE WHEN f.bonus_type_name = 'От клиента при возврате' THEN f.delivery_rub ELSE 0 END) AS from_client_return,
        SUM(CASE WHEN f.bonus_type_name = 'К клиенту при продаже' THEN f.delivery_rub ELSE 0 END) AS to_client_sale
    FROM daily_fin_reports_full f
    WHERE f.date_from = '2026-01-01'
    GROUP BY f.date_from;


--- === Процент возврата по фин отчетам ===
WITH funnel_orders AS
(SELECT fd.article_id AS nm_id,
	fd."date" AS order_date,
	sum(fd.orders_count) AS order_count -- 826
FROM orders_revenues fd
WHERE fd."date" >= current_date - interval '183 days'
	AND fd."date" < current_date - interval '40 days'
--	AND fd.nm_id = $1
GROUP BY nm_id, order_date), -- 4 409 строк 
fin_sales AS(
	SELECT fs.nm_id,
		sum(fs.quantity) AS sales_count,
		sum(fs.retail_price_withdisc_rub) AS retail_price_withdisc_rub_sales,
		sum(fs.ppvz_for_pay) AS ppvz_for_pay_sales,
		DATE(fs.order_dt) AS order_date
	FROM daily_fin_reports_full fs
	WHERE fs.doc_type_name = 'Продажа'
	AND fs.order_dt >= current_date - interval '183 days'
	AND fs.order_dt < current_date - interval '40 days'
--		AND f.nm_id = $1
	GROUP BY fs.nm_id, order_date
	),
fin_returns AS( -- Сумма возврата
	SELECT 
		fr.nm_id,
		sum(fr.delivery_rub ) AS delivery_rub_returns,
		DATE(fr.order_dt) AS order_date
	FROM daily_fin_reports_full fr
	WHERE fr.bonus_type_name ilike '%возврат%'
		AND fr.order_dt >= current_date - interval '183 days'
		AND fr.order_dt < current_date - interval '40 days'
--		AND f.nm_id = $1
	GROUP BY fr.nm_id, order_date),
fin_returns_count AS(
		SELECT 
		frs.nm_id,
		sum(frs.return_amount) AS returns_count, -- 7,487
		DATE(frs.order_dt) AS order_date
	FROM daily_fin_reports_full frs
	WHERE frs.order_dt >= current_date - interval '183 days'
	AND frs.order_dt < current_date- INTERVAL '40 days'
	AND frs.return_amount != 0
	GROUP BY frs.nm_id, frs.order_dt),
cond_profit AS (
	SELECT a.article_id AS nm_id, 
		a.net_profit,
		a.sum_net_profit,
		a."date" AS order_date
	FROM accurate_npd_purchase_calculation a
	WHERE a."date" >= current_date - interval '183 days'
	AND a."date" < current_date - interval '40 days')
SELECT o.nm_id,
	a.local_vendor_code,
	SUM(o.order_count) AS order_count,
	SUM(coalesce(s.sales_count, 0)) AS sales_count,
	sum(fc.returns_count) AS returns_count,
	ROUND(SUM(coalesce(s.sales_count, 0))/NULLIF(SUM(o.order_count)::NUMERIC, 0), 2)*100 AS buyout_percent,
	sum(delivery_rub_returns) AS delivery_rub_returns,
	ROUND(sum(fc.returns_count)/NULLIF(SUM(o.order_count)::NUMERIC, 0), 2)*100 AS returns_percent,
	ROUND(AVG(c.net_profit), 2) AS net_profit,
	SUM(c.sum_net_profit) AS sum_net_profit
--	o.order_date 
FROM funnel_orders o
LEFT JOIN fin_sales s
	USING(nm_id, order_date)
LEFT JOIN fin_returns r
	USING(nm_id, order_date)
LEFT JOIN fin_returns_count fc
	USING(nm_id, order_date)
LEFT JOIN cond_profit c
	USING(nm_id, order_date)
LEFT JOIN article a
	USING(nm_id)
GROUP BY o.nm_id, a.local_vendor_code
ORDER BY order_count DESC;



SELECT 
	min(f.rr_dt) AS date_from,
	max(f.rr_dt) AS date_to,	
	SUM(f.return_amount) AS return_count,
	f.bonus_type_name
FROM fin_reports_full f
WHERE f.return_amount != 0
	AND f.create_dt >= '2024-01-01'
	AND f.create_dt <= '2024-12-31'
--	AND f.rr_dt >= current_date - interval '365 days'
GROUP BY f.bonus_type_name;


SELECT 
	f.nm_id,
	min(f.rr_dt) AS date_from,
	max(f.rr_dt) AS date_to,	
	SUM(f.return_amount) AS return_count,
	f.bonus_type_name,
	f.doc_type_name 
FROM daily_fin_reports_full f
WHERE f.return_amount != 0
	AND f.create_dt >= '2025-01-01'
	AND f.create_dt <= '2025-12-31'
--	AND f.rr_dt >= current_date - interval '365 days'
GROUP BY f.nm_id, f.bonus_type_name, f.doc_type_name;

SELECT SUM(f.return_amount) AS return_count
--	f.realizationreport_id,
--	f.account 
FROM fin_reports_full f
WHERE f.account = 'Вектор'
	AND f.rr_dt >= '2025-01-01'
	AND f.rr_dt <= '2025-12-31';


SELECT min(rr_dt)
FROM daily_fin_reports_full f;



SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'daily_fin_reports_full';


-- Проверка логистики на одном артикуле ---
	SELECT 
		frs.nm_id,
		sum(frs.retail_price_withdisc_rub) AS retail_price_withdisc_rub_returns,
		sum(frs.return_amount) AS returns_count -- 7,487
		DATE(frs.order_dt) AS order_date
	FROM daily_fin_reports_full frs
	WHERE frs.order_dt >= current_date - interval '365 days'
	AND frs.order_dt < current_date
	AND frs.return_amount != 0
	GROUP BY frs.nm_id;

SELECT *
FROM daily_fin_reports_full f
WHERE f.srid IN ('22644f2505b645348878313e9237850a');


-- Утилизация товаров
SELECT sum(f.deduction) AS deduction,
	f.create_dt,
	f.bonus_type_name,
	f.srid,
	f.account 
FROM daily_fin_reports_full f
WHERE bonus_type_name ILIKE '%утил%'
GROUP BY f.create_dt,
	f.bonus_type_name,
	f.srid,
	f.account
ORDER BY f.create_dt desc;

-- Отчет по штрафам --
SELECT 
	f.nm_id,
	smv.wild,	
	f.subject_name,
	f.order_dt,
	f.bonus_type_name,
	f.assembly_id,
	smv."Наш_статус",
	smv."Статус_ВБ",
	smv."Статус_поставщика",
	f.sticker_id,
	f.srid,
	smv."Номер_поставки",
	sd.scan_dt,
	sum(f.penalty) AS penalty,
	smv."Получен_в_БД",
	f.realizationreport_id,
	f.account
FROM daily_fin_reports_full f
LEFT JOIN status_model_view smv
	ON smv."Номер_СЗ" = f.assembly_id
LEFT JOIN supplies_data sd
	ON sd.id = smv."Номер_поставки" 
WHERE f.order_dt >= '2025-10-01'
AND f.penalty != 0
AND f.assembly_id != 0
AND f.bonus_type_name NOT ilike '%сторно%'
GROUP BY f.nm_id,
	smv.wild,	
	f.subject_name,
	f.order_dt,
	f.bonus_type_name,
	f.assembly_id,
	smv."Наш_статус",
	smv."Статус_ВБ",
	smv."Статус_поставщика",
	f.sticker_id,
	smv."Получен_в_БД",
	f.srid,
	f.realizationreport_id,
	f.account,
	smv."Номер_поставки",
	sd.scan_dt;

-- Штрафы в Расчет закупки Россия --
SELECT  
	sum(f.penalty), -- 250,935.92
	a.local_vendor_code
FROM daily_fin_reports_full f
LEFT JOIN article a 
	USING(nm_id)
WHERE f.create_dt BETWEEN CURRENT_DATE - INTERVAL '7 days'
AND CURRENT_DATE
AND f.bonus_type_name ILIKE ANY (ARRAY[
    '%брак%',
    '%невыполненный%',
    '%подмена%'
])
GROUP BY a.local_vendor_code;


-- Кредитный отчет для бухгалтерии --
SELECT 
	f.account 
	,f.create_dt
	,f.bonus_type_name
    ,substring(f.bonus_type_name FROM '([0-9]{10,})') AS doc_number
    ,SUM(CASE WHEN f.bonus_type_name ILIKE '%основного долга%' THEN f.deduction ELSE 0 END) AS credit_body
    ,SUM(CASE WHEN f.bonus_type_name ILIKE '%оплаты процентов%' THEN f.deduction ELSE 0 END) AS credit_percent
    ,SUM(CASE WHEN f.bonus_type_name ILIKE '%оплаты процентов%' THEN f.deduction ELSE 0 END) AS credit_percent
FROM fin_reports_full f
WHERE f.create_dt = '2026-02-09'
AND f.deduction != 0
AND f.bonus_type_name ILIKE ANY (array['%заём%', '%займ%', '%кредит%', '%комисс%'])
GROUP BY f.account 
	, f.create_dt
	, f.bonus_type_name	
	, f.create_dt;


