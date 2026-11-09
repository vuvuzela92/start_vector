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

# Остатки WB и цены по артикулам за вчера без складов, признанных сгоревшими.
query_wb_stock_financial_report = """
WITH stock_by_article AS (
    SELECT
        w.nm_id,
        SUM(w.quantity) AS quantity,
        SUM(w.in_way_to_client) AS in_way_to_client,
        SUM(w.in_way_from_client) AS in_way_from_client
    FROM wb_stock_2 AS w
    WHERE w.snapshot_date = CURRENT_DATE - INTERVAL '1 day'
      AND NOT EXISTS (
          SELECT 1
          FROM unnest(ARRAY[
              'Волгоград', 'Самара', 'Тула',
              'Пенза', 'Краснодар', 'Сарапул',
              'Шушары', 'Электросталь', 'Коледино'
          ]) AS excluded_city(city_name)
          WHERE w.warehouse_name ILIKE '%%' || excluded_city.city_name || '%%'
      )
    GROUP BY w.nm_id
),
price_by_article AS (
    SELECT DISTINCT ON (p.article_id)
        p.article_id,
        p.discounted_price
    FROM prices_data AS p
    WHERE p.date = CURRENT_DATE - INTERVAL '1 day'
    ORDER BY p.article_id, p.date DESC
),
stock_by_nm_id AS (
    SELECT
        a.nm_id,
        a.local_vendor_code,
        MAX(p.discounted_price) AS discounted_price,
        SUM(s.quantity + s.in_way_to_client + s.in_way_from_client) AS total_quantity
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
    WHERE cp."date" = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY cp.local_vendor_code
)
SELECT
    s.local_vendor_code,
    s.total_quantity,
    COALESCE(c.average_purchase_price, 0) AS average_purchase_price,
    s.discounted_price
FROM stock_by_nm_id AS s
LEFT JOIN purchase_price_by_product AS c
  ON c.local_vendor_code = s.local_vendor_code
ORDER BY s.nm_id;
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

query_wild_frm_products = """
    SELECT DISTINCT(p.id)
                    FROM products p
    """
