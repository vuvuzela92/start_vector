"""Запросы к БД для Финансового анализа"""

# Запрос для получения данных об удержаниях за месяц
query_deductions_by_month = """
SELECT * FROM deductions_by_month
    """

# Запрос для получения данных о затратах по данным из 1С
query_cash_flow_writeoffs = """
SELECT *
FROM cash_flow_writeoffs c
WHERE c.is_valid IS TRUE
ORDER BY c.date DESC;
"""

# Запрос для получения данных из ежедневных фин отчетов, сгруппированных до месяца
query_monthly_report = """
    WITH main AS (
    SELECT
        DATE_TRUNC('month', date_from)::date AS month,
        TO_CHAR(date_from, 'YYYY-MM') AS "Месяц",
        EXTRACT(YEAR FROM date_from) AS year,
        EXTRACT(MONTH FROM date_from) AS month_number,
        SUM(wb_commission) AS "Комиссия WB",
        ROUND(AVG(wb_commission_pct)/100,2) AS "Комиссия WB_pct",
        SUM(payout) AS "К перечислению",
        SUM(logistics) AS "Логистика",
        SUM(total_to_pay) AS "Итого к оплате",
        SUM(revenue) AS "Выручка",
        SUM(retail_price_disc) AS "Цена со скидкой",
        SUM(penalties) AS "Штрафы",
        SUM(storage_fee) AS "Хранение",
        SUM(deductions) AS "Удержания",
        SUM(paid_acceptance) AS "Платная приёмка",
        SUM(credit_transfers) AS "Перечисления по кредиту",
        SUM(to_client_cancel) AS "Клиент отменил",
        SUM(from_client_cancel) AS "Отмена клиенту",
        SUM(from_client_return)  AS "Возврат от клиента",
        SUM(to_client_sale) AS "Продажа клиенту",
        SUM(purchase_cost_sales) AS "Себестоимость продаж",
        SUM(purchase_cost_returns) AS "Себестоимость возвратов",
        SUM(purchase_cost_total) AS "Закупочная стоимость",
        ROUND(AVG(margin_before_cost_pct)/100,2) AS "Наша доля до вычета себестомости_pct",
        SUM(gp_after_wb) AS "ВП после WB",
        ROUND(AVG(gp_after_wb_pct)/100,2) AS "ВП после WB_pct",
        SUM(acquiring_fee) AS "Стоимость эквайринга",
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('month', date_from)) AS count_acc,
        account
    FROM daily_fin_reports_agg
    WHERE date_from >= '2025-01-01'
    GROUP BY
        account,
        DATE_TRUNC('month', date_from),
        TO_CHAR(date_from, 'YYYY-MM'),
        EXTRACT(YEAR FROM date_from),
        EXTRACT(MONTH FROM date_from))
    SELECT * 
    FROM main
    ORDER BY month DESC;
    """

query_stock_analyze = """
    SELECT 
        o."date",
        TO_CHAR(o."date", 'YYYY-mm') AS "месяц",
        o.local_vendor_code AS wild,
        avg(o.purchase_price) AS "Стоимость_закупки",
        avg(o.end_of_day_balance) AS "Остаток_на_нашем_складе",
        sum(o.total_quantity) AS "Остаток_на_складах_ВБ",
        sum(o.in_way_from_client) + sum(o.in_way_from_client) AS "Товаров_в_пути",
        avg(o.purchase_price) * avg(o.end_of_day_balance) AS "Сумма_остатков_на_нашем_складе",
        avg(o.purchase_price) * sum(o.total_quantity) AS "Сумма_остатков_на_складах_ВБ",
        avg(o.purchase_price) * (sum(o.in_way_from_client) + sum(o.in_way_from_client)) AS "Сумма_товаров_в_пути"
    FROM orders_articles_analyze o
    WHERE o."date" >= '2025-11-01'
    AND o.local_vendor_code ILIKE '%wild%'
    AND o.local_vendor_code != 'notwild'
    GROUP BY o."date",
        TO_CHAR(o."date", 'YYYY-mm'),
        
        o.local_vendor_code
    ORDER BY o."date" DESC;
            """

# Остатки WB по всем складам и товары в пути на последний доступный снимок.
query_wb_stock_financial_report = """
WITH latest_stock_snapshot AS (
    SELECT MAX(w.snapshot_date) AS snapshot_date
    FROM wb_stock_2 AS w
    WHERE w.snapshot_date <= CURRENT_DATE
),
stock_by_article AS (
    SELECT
        w.nm_id,
        SUM(w.quantity) AS quantity,
        SUM(w.in_way_to_client) AS in_way_to_client,
        SUM(w.in_way_from_client) AS in_way_from_client
    FROM wb_stock_2 AS w
    CROSS JOIN latest_stock_snapshot AS snapshot
    WHERE w.snapshot_date = snapshot.snapshot_date
    GROUP BY w.nm_id
),
price_by_article AS (
    SELECT DISTINCT ON (p.article_id)
        p.article_id,
        p.discounted_price
    FROM prices_data AS p
    WHERE p.date = (SELECT snapshot_date FROM latest_stock_snapshot)
    ORDER BY p.article_id, p.date DESC
),
stock_by_nm_id AS (
    SELECT
        a.nm_id,
        a.local_vendor_code,
        MAX(p.discounted_price) AS discounted_price,
        SUM(s.quantity) AS quantity,
        SUM(s.in_way_to_client) AS in_way_to_client,
        SUM(s.in_way_from_client) AS in_way_from_client
    FROM stock_by_article AS s
    JOIN article AS a
      ON a.nm_id = s.nm_id
    LEFT JOIN price_by_article AS p
      ON p.article_id = s.nm_id
    GROUP BY a.nm_id, a.local_vendor_code
),
purchase_price_by_product AS (
    SELECT
        cp.local_vendor_code,
        AVG(cp.purchase_price) AS average_purchase_price
    FROM cost_price AS cp
    WHERE cp."date" = (SELECT snapshot_date FROM latest_stock_snapshot)
    GROUP BY cp.local_vendor_code
)
SELECT
    (SELECT snapshot_date FROM latest_stock_snapshot) AS snapshot_date,
    s.local_vendor_code,
    COALESCE(s.quantity, 0) AS quantity,
    COALESCE(s.in_way_to_client, 0) AS in_way_to_client,
    COALESCE(s.in_way_from_client, 0) AS in_way_from_client,
    COALESCE(c.average_purchase_price, 0) AS average_purchase_price,
    s.discounted_price
FROM stock_by_nm_id AS s
LEFT JOIN purchase_price_by_product AS c
  ON c.local_vendor_code = s.local_vendor_code
ORDER BY s.nm_id;
"""

# Себестоимость заказов в пути по тем же ценам и дате, что у остатков WB.
query_in_transit_purchase_prices = """
SELECT
    cp.local_vendor_code,
    AVG(cp.purchase_price) AS average_purchase_price
FROM cost_price AS cp
WHERE cp."date" = :snapshot_date
  AND cp.local_vendor_code = ANY(:wilds)
GROUP BY cp.local_vendor_code;
"""

query_outcomes_detalize = """
            SELECT *
            FROM (
                SELECT
                    start_date,
                    end_date,
                    "month",
                    "type",
                    value,
                    created_at,
                    CASE
                        WHEN "type" IN (
                            'Расходы офис',
                            'Услуги подбора персонала',
                            'Арендные платежи (офис)',
                            'Услуги связи (интернет, телефон)',
                            'Стоянка',
                            'Эксплуатация здания',
                            'Коммунальные платежи'
                        ) THEN 'Расходы офис'
                        WHEN "type" = 'Заработная плата' THEN 'Заработная плата'
                        WHEN "type" IN (
                            'расчеты с поставщиками в валюте',
                            'расчеты с поставщиками ',
                            'расчету с поставщиком материалы',
                            'карго',
                            'консультационные услуги по оформлении сделки '
                        ) THEN 'Расходы закупка'
                        WHEN "type" IN (
                            'Расходы склад ',
                            'Транспортные расходы, гсм',
                            'Парковка',
                            'Обслуживание а/м',
                            'Арендные платежи (склад)',
                            'страховка'
                        ) THEN 'Расходы склада'
                        WHEN "type" IN (
                            'ПО, сервисы, обслуживание ПО',
                            'Выкуп вб',
                            'Реклама/продвижение на площадках МП',
                            'вб смена номера, перенос карточек ',
                            'Услуги ВБ: Продвижение ',
                            'Услуги ВБ: эквайринг',
                            'Услуги ВБ: перевозки',
                            'Услуги ВБ: штраф',
                            'Услуги ВБ: поверенного',
                            'Вознаграждение'
                        ) THEN 'Комерческие расходы'
                        WHEN "type" = 'Услуги банка' THEN 'Прочие расходы'
                        WHEN "type" IN (
                            'Налог: НДФЛ',
                            'Налог: УСН',
                            'Налог: СВ',
                            'Налог: прочее',
                            'Налог:НДС',
                            'Штрафы, взыскания'
                        ) THEN 'Налоги'
                        WHEN "type" IN (
                            'Инвестиционные платежи',
                            'Кредит ВБ',
                            'проценты кредит ВБ',
                            'Кредит СИМПЛФИНАНС ООО МККСИМПЛФИНАНС ООО МКК',
                            'Проценты СИМПЛФИНАНС ООО МКК',
                            'Займы ВБ, основной долг ',
                            'Займ ВБ Проценты ',
                            'Займ вб, комиссия',
                            'Кредит сбер',
                            'кредит сберпроценты',
                            'Кредит ФЛ Данилян',
                            'Рови факторинг: осн долг',
                            'Рови факторинг: проценты',
                            'Рови факторинг:пени',
                            'ООО БАРСТТ',
                            'Лизинг, покупка оборудования, помещений'
                        ) THEN 'Финансовые расходы '

                        ELSE 'Не определено'
                    END AS category
                FROM public.expenses
            ) t
            WHERE category <> 'Не определено'
            ORDER BY end_date DESC;
            """

query_fin_deductions_mv = """
    SELECT *
            FROM fin_deductions_mv
    """

query_daily_fin_reports_deductions_agg = """
    SELECT TO_CHAR(f.date_from, 'MM-YYYY') AS month,
            f.grouped_bonus_type_name,
            f.total_deduction
            FROM daily_fin_reports_deductions f
            WHERE f.grouped_bonus_type_name IS NOT NULL
            ORDER BY f.date_from desc;
    """

# Расчёт финансовой витрины по отчётам реализации и себестоимости.
query_fin_reports_art_analyze = """
WITH financials AS (
    SELECT
        f.account,
        f.realizationreport_id,
        f.date_from,
        f.date_to,
        MAX(f.create_dt) AS report_create_dt,
        MAX(f.report_type) AS report_type,
        MAX(f.currency_name) AS currency,
        SUM(CASE
            WHEN f.doc_type_name = 'Продажа' THEN COALESCE(f.retail_amount, 0)
            WHEN f.doc_type_name = 'Возврат' THEN -COALESCE(f.retail_amount, 0)
            ELSE 0
        END) AS dashboard_sale,
        SUM(CASE
            WHEN f.doc_type_name = 'Продажа' THEN COALESCE(f.cashback_discount, 0)
            WHEN f.doc_type_name = 'Возврат' THEN -COALESCE(f.cashback_discount, 0)
            ELSE 0
        END) AS loyalty_discount_compensation,
        SUM(CASE
            WHEN f.doc_type_name = 'Продажа' THEN COALESCE(f.cashback_amount, 0)
            WHEN f.doc_type_name = 'Возврат' THEN -COALESCE(f.cashback_amount, 0)
            ELSE 0
        END) AS loyalty_points_withheld,
        SUM(CASE
            WHEN f.doc_type_name = 'Продажа'
                THEN COALESCE(f.cashback_commission_change, 0)
            WHEN f.doc_type_name = 'Возврат'
                THEN -COALESCE(f.cashback_commission_change, 0)
            ELSE 0
        END) AS loyalty_program_cost,
        SUM(COALESCE(f.payment_schedule, 0)) AS payment_schedule_adjustment,
        SUM(COALESCE(f.additional_payment, 0)) AS wb_reward_adjustment,
        SUM(CASE
            WHEN f.supplier_oper_name IN ('Продажа', 'Возврат') THEN
                CASE
                    WHEN f.doc_type_name = 'Продажа' THEN 1
                    WHEN f.doc_type_name = 'Возврат' THEN -1
                    ELSE 0
                END * (
                    COALESCE(f.retail_price_withdisc_rub, 0)
                    - COALESCE(f.ppvz_for_pay, 0)
                    - COALESCE(f.acquiring_fee, 0)
                )
            ELSE 0
        END) AS wb_commission,
        SUM(CASE
            WHEN f.doc_type_name = 'Продажа' THEN COALESCE(f.acquiring_fee, 0)
            WHEN f.doc_type_name = 'Возврат' THEN -COALESCE(f.acquiring_fee, 0)
            ELSE 0
        END) AS acquiring_fee,
        SUM(CASE WHEN f.doc_type_name = 'Продажа' THEN f.ppvz_for_pay ELSE 0 END)
            - SUM(CASE WHEN f.doc_type_name = 'Возврат' THEN f.ppvz_for_pay ELSE 0 END)
            AS payout,
        SUM(f.delivery_rub) AS logistics,
        SUM(CASE WHEN f.doc_type_name = 'Продажа' THEN f.ppvz_for_pay ELSE 0 END)
            - SUM(CASE WHEN f.doc_type_name = 'Возврат' THEN f.ppvz_for_pay ELSE 0 END)
            - SUM(f.delivery_rub)
            - SUM(f.storage_fee)
            - SUM(f.penalty)
            - SUM(f.deduction)
            - SUM(COALESCE(f.acceptance, 0))
            - SUM(CASE
                WHEN f.doc_type_name = 'Продажа'
                    THEN COALESCE(f.cashback_amount, 0)
                WHEN f.doc_type_name = 'Возврат'
                    THEN -COALESCE(f.cashback_amount, 0)
                ELSE 0
            END)
            - SUM(CASE
                WHEN f.doc_type_name = 'Продажа'
                    THEN COALESCE(f.cashback_commission_change, 0)
                WHEN f.doc_type_name = 'Возврат'
                    THEN -COALESCE(f.cashback_commission_change, 0)
                ELSE 0
            END)
            - SUM(COALESCE(f.payment_schedule, 0))
            - SUM(COALESCE(f.additional_payment, 0)) AS total_to_pay,
        SUM(CASE WHEN f.doc_type_name = 'Продажа'
            THEN f.retail_price_withdisc_rub ELSE 0 END)
            - SUM(CASE WHEN f.doc_type_name = 'Возврат'
                THEN f.retail_price_withdisc_rub ELSE 0 END) AS revenue,
        SUM(f.retail_price_withdisc_rub) AS retail_price_disc,
        SUM(f.penalty) AS penalties,
        SUM(f.storage_fee) AS storage_fee,
        SUM(f.deduction) AS deductions,
        SUM(CASE
            WHEN COALESCE(f.bonus_type_name, '') ILIKE '%продвижен%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%реклам%'
                THEN COALESCE(f.deduction, 0)
            ELSE 0
        END) AS deductions_ads,
        SUM(CASE
            WHEN COALESCE(f.bonus_type_name, '') ILIKE '%продвижен%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%реклам%'
                THEN 0
            WHEN COALESCE(f.bonus_type_name, '') ILIKE '%кредит%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%займ%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%заём%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%факторинг%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%баланс заемщика%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%баланс заёмщика%'
                THEN COALESCE(f.deduction, 0)
            ELSE 0
        END) AS deductions_credits,
        SUM(CASE
            WHEN COALESCE(f.bonus_type_name, '') ILIKE '%продвижен%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%реклам%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%кредит%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%займ%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%заём%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%факторинг%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%баланс заемщика%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%баланс заёмщика%'
                THEN 0
            WHEN COALESCE(f.bonus_type_name, '') ILIKE '%услуг%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%подписк%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%перенос карточек%'
                THEN COALESCE(f.deduction, 0)
            ELSE 0
        END) AS deductions_services,
        SUM(CASE
            WHEN COALESCE(f.bonus_type_name, '') ILIKE '%продвижен%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%реклам%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%кредит%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%займ%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%заём%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%факторинг%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%баланс заемщика%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%баланс заёмщика%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%услуг%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%подписк%'
                OR COALESCE(f.bonus_type_name, '') ILIKE '%перенос карточек%'
                THEN 0
            ELSE COALESCE(f.deduction, 0)
        END) AS deductions_other,
        SUM(f.acceptance) AS paid_acceptance,
        SUM(CASE WHEN f.bonus_type_name ILIKE '%кредит%'
            THEN f.deduction ELSE 0 END) AS credit_transfers,
        SUM(CASE WHEN f.bonus_type_name = 'К клиенту при отмене'
            THEN f.delivery_rub ELSE 0 END) AS to_client_cancel,
        SUM(CASE WHEN f.bonus_type_name = 'От клиента при отмене'
            THEN f.delivery_rub ELSE 0 END) AS from_client_cancel,
        SUM(CASE WHEN f.bonus_type_name = 'От клиента при возврате'
            THEN f.delivery_rub ELSE 0 END) AS from_client_return,
        SUM(CASE WHEN f.bonus_type_name = 'К клиенту при продаже'
            THEN f.delivery_rub ELSE 0 END) AS to_client_sale
    FROM fin_reports_full f
    WHERE f.date_from BETWEEN :start_date AND :end_date
    GROUP BY f.account, f.realizationreport_id, f.date_from, f.date_to
), purchase_cost_lines AS (
    SELECT
        f.account,
        f.realizationreport_id,
        f.date_from,
        f.date_to,
        f.supplier_oper_name,
        f.quantity * COALESCE(price.purchase_price, 0) AS line_purchase_cost
    FROM fin_reports_full f
    LEFT JOIN article a
        ON a.nm_id = f.nm_id
        AND LOWER(a.account) = LOWER(f.account)
    LEFT JOIN LATERAL (
        SELECT AVG(cp.purchase_price)::numeric AS purchase_price
        FROM public.cost_price cp
        WHERE cp.local_vendor_code = a.local_vendor_code
            AND cp.date = (
                SELECT MAX(price_date.date)
                FROM public.cost_price price_date
                WHERE price_date.local_vendor_code = a.local_vendor_code
                    AND price_date.date <= COALESCE(f.order_dt::date, f.date_from)
            )
    ) price ON TRUE
    WHERE f.supplier_oper_name IN ('Продажа', 'Возврат')
        AND f.date_from BETWEEN :start_date AND :end_date
), purchase_cost_by_report AS (
    SELECT
        account,
        realizationreport_id,
        date_from,
        date_to,
        SUM(CASE WHEN supplier_oper_name = 'Продажа'
            THEN line_purchase_cost ELSE 0 END) AS purchase_cost_sales,
        SUM(CASE WHEN supplier_oper_name = 'Возврат'
            THEN line_purchase_cost ELSE 0 END) AS purchase_cost_returns
    FROM purchase_cost_lines
    GROUP BY account, realizationreport_id, date_from, date_to
)
SELECT
    f.account,
    f.realizationreport_id,
    f.date_from,
    f.date_to,
    f.report_create_dt,
    f.report_type,
    CASE f.report_type
        WHEN 1 THEN 'Основной'
        WHEN 2 THEN 'По выкупам'
        ELSE 'Не определён'
    END AS report_type_label,
    f.currency,
    0::numeric AS agreed_discount_pct,
    TO_CHAR(f.date_to, 'MM-YYYY') AS month,
    f.revenue,
    CASE
        WHEN vat.vat IS NOT NULL
            THEN ROUND(f.revenue / (1 + vat.vat), 2)
        ELSE NULL
    END AS revenue_without_vat,
    f.dashboard_sale,
    f.loyalty_discount_compensation,
    f.loyalty_points_withheld,
    f.loyalty_program_cost,
    f.payment_schedule_adjustment,
    f.wb_reward_adjustment,
    f.logistics,
    f.storage_fee,
    f.acquiring_fee,
    f.penalties,
    f.deductions,
    f.payout,
    f.total_to_pay,
    f.wb_commission,
    CASE
        WHEN f.date_from < DATE '2026-01-01'
            THEN ROUND(f.wb_commission / 1.20, 2)
        ELSE ROUND(f.wb_commission / 1.22, 2)
    END AS wb_commission_without_vat,
    CASE WHEN f.revenue <> 0
        THEN ROUND(f.wb_commission / f.revenue, 4) * 100 ELSE 0
    END AS wb_commission_pct,
    f.retail_price_disc,
    f.paid_acceptance,
    f.credit_transfers,
    f.to_client_cancel,
    f.from_client_cancel,
    f.from_client_return,
    f.to_client_sale,
    COALESCE(p.purchase_cost_sales, 0) AS purchase_cost_sales,
    COALESCE(p.purchase_cost_returns, 0) AS purchase_cost_returns,
    COALESCE(p.purchase_cost_sales, 0) - COALESCE(p.purchase_cost_returns, 0)
        AS purchase_cost_total,
    CASE WHEN f.revenue <> 0
        THEN ROUND(f.total_to_pay / f.revenue, 4) * 100 ELSE 0
    END AS margin_before_cost_pct,
    f.total_to_pay
        - (COALESCE(p.purchase_cost_sales, 0) - COALESCE(p.purchase_cost_returns, 0))
        + f.credit_transfers AS gp_after_wb,
    CASE WHEN f.revenue <> 0 THEN ROUND((
        f.total_to_pay
        - (COALESCE(p.purchase_cost_sales, 0) - COALESCE(p.purchase_cost_returns, 0))
        + f.credit_transfers
    ) / f.revenue, 4) * 100 ELSE 0
    END AS gp_after_wb_pct
    ,f.deductions_ads
    ,f.deductions_services
    ,f.deductions_credits
    ,f.deductions_other
FROM financials f
LEFT JOIN LATERAL (
    SELECT guide.vat
    FROM public.vat_guide guide
    WHERE UPPER(TRIM(guide.account)) = UPPER(TRIM(f.account))
        AND guide.start_date <= f.date_from::date
        AND (
            guide.end_date IS NULL
            OR guide.end_date = DATE '1970-01-01'
            OR guide.end_date >= f.date_from::date
        )
    ORDER BY guide.start_date DESC
    LIMIT 1
) vat ON TRUE
LEFT JOIN purchase_cost_by_report p
    ON f.account = p.account
    AND f.realizationreport_id = p.realizationreport_id
    AND f.date_from = p.date_from
    AND f.date_to = p.date_to
"""

query_fin_reports_art_analyze_export = """
SELECT
    account,
    realizationreport_id,
    date_from,
    date_to,
    month,
    revenue,
    revenue_without_vat,
    logistics,
    storage_fee,
    acquiring_fee,
    penalties,
    deductions,
    payout,
    total_to_pay,
    wb_commission,
    wb_commission_without_vat,
    wb_commission_pct,
    retail_price_disc,
    paid_acceptance,
    credit_transfers,
    to_client_cancel,
    from_client_cancel,
    from_client_return,
    to_client_sale,
    purchase_cost_sales,
    purchase_cost_returns,
    purchase_cost_total,
    margin_before_cost_pct,
    gp_after_wb,
    gp_after_wb_pct,
    report_create_dt,
    report_type,
    report_type_label,
    currency,
    agreed_discount_pct,
    dashboard_sale,
    loyalty_discount_compensation,
    loyalty_points_withheld,
    loyalty_program_cost,
    payment_schedule_adjustment,
    wb_reward_adjustment,
    deductions_ads,
    deductions_services,
    deductions_credits,
    deductions_other
FROM public.fin_reports_art_analyze
ORDER BY date_from DESC, account, realizationreport_id
"""

query_wild_frm_products = """
    SELECT DISTINCT(p.id)
                    FROM products p
    """
