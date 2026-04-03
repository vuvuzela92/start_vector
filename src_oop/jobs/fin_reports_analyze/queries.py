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

# Устаревший запрос, который использовался для получения данных о ежемесячных удержаниях, сгруппированных по типам
# query_monthly_report = """
#             WITH main AS (
#                 SELECT
#                     DATE_TRUNC('month', date_from)::date AS month,
#                     TO_CHAR(date_from, 'YYYY-MM') AS "Месяц",
#                     EXTRACT(YEAR FROM date_from) AS year,
#                     EXTRACT(MONTH FROM date_from) AS month_number,
#                     SUM(wb_commission) AS "Комиссия WB",
#                     ROUND(AVG(wb_commission_pct)/100,2) AS "Комиссия WB_pct",
#                     SUM(payout) AS "К перечислению",
#                     SUM(logistics) AS "Логистика",
#                     SUM(total_to_pay) AS "Итого к оплате",
#                     SUM(revenue) AS "Выручка",
#                     SUM(retail_price_disc) AS "Цена со скидкой",
#                     SUM(penalties) AS "Штрафы",
#                     SUM(storage_fee) AS "Хранение",
#                     SUM(deductions) AS "Удержания",
#                     SUM(paid_acceptance) AS "Платная приёмка",
#                     SUM(credit_transfers) AS "Перечисления по кредиту",
#                     SUM(to_client_cancel) AS "Клиент отменил",
#                     SUM(from_client_cancel) AS "Отмена клиенту",
#                     SUM(from_client_return)  AS "Возврат от клиента",
#                     SUM(to_client_sale) AS "Продажа клиенту",
#                     SUM(purchase_cost_sales) AS "Себестоимость продаж",
#                     SUM(purchase_cost_returns) AS "Себестоимость возвратов",
#                     SUM(purchase_cost_total) AS "Закупочная стоимость",
#                     ROUND(AVG(margin_before_cost_pct)/100,2) AS "Наша доля до вычета себестомости_pct",
#                     SUM(gp_after_wb) AS "ВП после WB",
#                     ROUND(AVG(gp_after_wb_pct)/100,2) AS "ВП после WB_pct",
#                     COUNT(*) OVER (PARTITION BY DATE_TRUNC('month', date_from)) AS count_acc,
#                     account
#                 FROM daily_fin_reports_agg
#                 WHERE date_from >= '2025-01-01'
#                 GROUP BY
#                     account,
#                     DATE_TRUNC('month', date_from),
#                     TO_CHAR(date_from, 'YYYY-MM'),
#                     EXTRACT(YEAR FROM date_from),
#                     EXTRACT(MONTH FROM date_from)),
#                 exp AS (
#                 SELECT
#                     EXTRACT(YEAR FROM start_date) AS e_year,
#                     TO_CHAR(start_date, 'YYYY-MM') AS e_month,
#                     -- Individual expense columns
#                     SUM(CASE WHEN type='Расходы офис' THEN value ELSE 0 END) AS "Расходы офис",
#                     SUM(CASE WHEN type='Услуги подбора персонала' THEN value ELSE 0 END) AS "Услуги подбора персонала",
#                     SUM(CASE WHEN type='Арендные платежи (офис)' THEN value ELSE 0 END) AS "Арендные платежи (офис)",
#                     SUM(CASE WHEN type='Услуги связи (интернет, телефон)' THEN value ELSE 0 END) AS "Услуги связи (интернет, телефон)",
#                     SUM(CASE WHEN type='Стоянка' THEN value ELSE 0 END) AS "Стоянка",
#                     SUM(CASE WHEN type='Эксплуатация здания' THEN value ELSE 0 END) AS "Эксплуатация здания",
#                     SUM(CASE WHEN type='Коммунальные платежи' THEN value ELSE 0 END) AS "Коммунальные платежи",
#                     SUM(CASE WHEN type='Расчету с поставщиком материалы' THEN value ELSE 0 END) AS "Расчеты с поставщиком материалы",
#                     SUM(CASE WHEN type='Карго' THEN value ELSE 0 END) AS "Карго",
#                     SUM(CASE WHEN type='Консультационные услуги по оформлении сделки' THEN value ELSE 0 END) AS "Консультационные услуги по оформлении сделки",
#                     SUM(CASE WHEN type='Расходы склад' THEN value ELSE 0 END) AS "Расходы склад",
#                     SUM(CASE WHEN type='Транспортные расходы, гсм' THEN value ELSE 0 END) AS "Транспортные расходы, ГСМ",
#                     SUM(CASE WHEN type='Парковка' THEN value ELSE 0 END) AS "Парковка",
#                     SUM(CASE WHEN type='Обслуживание а/м' THEN value ELSE 0 END) AS "Обслуживание а/м",
#                     SUM(CASE WHEN type='Арендные платежи (склад)' THEN value ELSE 0 END) AS "Арендные платежи (склад)",
#                     SUM(CASE WHEN type='Страховка' THEN value ELSE 0 END) AS "Страховка",
#                     SUM(CASE WHEN type='ПО, сервисы, обслуживание ПО' THEN value ELSE 0 END) AS "ПО, сервисы, обслуживание ПО",
#                     SUM(CASE WHEN type='Реклама/продвижение на площадках МП' THEN value ELSE 0 END) AS "Реклама/продвижение на площадках МП",
#                     SUM(CASE WHEN type='Вб смена номера, перенос карточек' THEN value ELSE 0 END) AS "ВБ смена номера, перенос карточек",
#                     SUM(CASE WHEN type='Услуги банка' THEN value ELSE 0 END) AS "Услуги банка",
#                     SUM(CASE WHEN type='Налог: НДФЛ' THEN value ELSE 0 END) AS "Налог: НДФЛ",
#                     SUM(CASE WHEN type='Налог: УСН' THEN value ELSE 0 END) AS "Налог: УСН",
#                     SUM(CASE WHEN type='Налог: СВ' THEN value ELSE 0 END) AS "Налог: СВ",
#                     SUM(CASE WHEN type='Налог: прочее' THEN value ELSE 0 END) AS "Налог: прочее",
#                     SUM(CASE WHEN type='Налог:НДС' THEN value ELSE 0 END) AS "Налог: НДС",
#                     SUM(CASE WHEN type='Штрафы, взыскания' THEN value ELSE 0 END) AS "Штрафы, взыскания",
#                     SUM(CASE WHEN type='Проценты кредит ВБ' THEN value ELSE 0 END) AS "Проценты кредит ВБ",
#                     SUM(CASE WHEN type='Проценты СИМПЛФИНАНС ООО МКК' THEN value ELSE 0 END) AS "Проценты СИМПЛФИНАНС",
#                     SUM(CASE WHEN type='Займ ВБ Проценты' THEN value ELSE 0 END) AS "Займ ВБ: проценты",
#                     SUM(CASE WHEN type='Кредит сберпроценты' THEN value ELSE 0 END) AS "Кредит Сбер: проценты",
#                     SUM(CASE WHEN type='Рови факторинг: проценты' THEN value ELSE 0 END) AS "Рови факторинг: проценты",
#                     SUM(CASE WHEN type='Рови факторинг:пени' THEN value ELSE 0 END) AS "Рови факторинг: пени",
#                     SUM(CASE WHEN type='Лизинг, покупка оборудования, помещений' THEN value ELSE 0 END) AS "Лизинг, покупка оборудования, помещений",
#                     SUM(CASE WHEN type='Заработная плата' THEN value ELSE 0 END) AS "Заработная плата",
#                     -- Sum of all above as 'Расходы компании'
#                     SUM(
#                         CASE WHEN type IN (
#                             'Расходы офис', 'Услуги подбора персонала', 'Арендные платежи (офис)',
#                             'Услуги связи (интернет, телефон)', 'Стоянка', 'Эксплуатация здания',
#                             'Коммунальные платежи', 'Расчету с поставщиком материалы', 'Карго',
#                             'Консультационные услуги по оформлении сделки', 'Расходы склад',
#                             'Транспортные расходы, гсм', 'Парковка', 'Обслуживание а/м',
#                             'Арендные платежи (склад)', 'Страховка', 'ПО, сервисы, обслуживание ПО',
#                             'Реклама/продвижение на площадках МП', 'ВБ смена номера, перенос карточек',
#                             'Услуги банка', 'Налог: НДФЛ', 'Налог: УСН', 'Налог: СВ', 'Налог: прочее',
#                             'Налог:НДС', 'Штрафы, взыскания', 'Проценты кредит ВБ', 'Проценты СИМПЛФИНАНС ООО МКК',
#                             'Займ ВБ Проценты', 'Кредит сберпроценты', 'Рови факторинг: проценты',
#                             'Рови факторинг:пени', 'Лизинг, покупка оборудования, помещений', 'Заработная плата'
#                         ) THEN value ELSE 0 END
#                     ) AS "Расходы компании"
#                 FROM expenses e
#                 GROUP BY EXTRACT(YEAR FROM start_date), TO_CHAR(start_date, 'YYYY-MM')
#                 )
#                 SELECT
#                     m.*,
#                     e.e_year,
#                     e."Расходы офис"/m.count_acc AS "Расходы офис",
#                     e."Услуги подбора персонала"/m.count_acc AS "Услуги подбора персонала",
#                     e."Арендные платежи (офис)"/m.count_acc AS "Арендные платежи (офис)",
#                     e."Услуги связи (интернет, телефон)"/m.count_acc AS "Услуги связи (интернет, телефон)",
#                     e."Стоянка"/m.count_acc AS "Стоянка",
#                     e."Эксплуатация здания"/m.count_acc AS "Эксплуатация здания",
#                     e."Коммунальные платежи"/m.count_acc AS "Коммунальные платежи",
#                     e."Расчеты с поставщиком материалы"/m.count_acc AS "Расчеты с поставщиком материалы",
#                     e."Карго"/m.count_acc AS "Карго",
#                     e."Консультационные услуги по оформл"/m.count_acc AS "Консультационные услуги по оформл",
#                     e."Расходы склад"/m.count_acc AS "Расходы склад",
#                     e."Транспортные расходы, ГСМ"/m.count_acc AS "Транспортные расходы, ГСМ",
#                     e."Парковка"/m.count_acc AS "Парковка",
#                     e."Обслуживание а/м"/m.count_acc AS "Обслуживание а/м",
#                     e."Арендные платежи (склад)"/m.count_acc AS "Арендные платежи (склад)",
#                     e."Страховка"/m.count_acc AS "Страховка",
#                     e."ПО, сервисы, обслуживание ПО"/m.count_acc AS "ПО, сервисы, обслуживание ПО",
#                     e."Реклама/продвижение на площадках МП"/m.count_acc AS "Реклама/продвижение на площадках",
#                     e."ВБ смена номера, перенос карточек"/m.count_acc AS "ВБ смена номера, перенос карточек",
#                     e."Услуги банка"/m.count_acc AS "Услуги банка",
#                     e."Налог: НДФЛ"/m.count_acc AS "Налог: НДФЛ",
#                     e."Налог: УСН"/m.count_acc AS "Налог: УСН",
#                     e."Налог: СВ"/m.count_acc AS "Налог: СВ",
#                     e."Налог: прочее"/m.count_acc AS "Налог: прочее",
#                     e."Налог: НДС"/m.count_acc AS "Налог: НДС",
#                     e."Штрафы, взыскания"/m.count_acc AS "Штрафы, взыскания",
#                     e."Проценты кредит ВБ"/m.count_acc AS "Проценты кредит ВБ",
#                     e."Проценты СИМПЛФИНАНС"/m.count_acc AS "Проценты СИМПЛФИНАНС",
#                     e."Займ ВБ: проценты"/m.count_acc AS "Займ ВБ: проценты",
#                     e."Кредит Сбер: проценты"/m.count_acc AS "Кредит Сбер: проценты",
#                     e."Рови факторинг: проценты"/m.count_acc AS "Рови факторинг: проценты",
#                     e."Рови факторинг: пени"/m.count_acc AS "Рови факторинг: пени",
#                     e."Лизинг, покупка оборудования, поме"/m.count_acc AS "Лизинг, покупка оборудования, поме",
#                     e."Заработная плата"/m.count_acc AS "Заработная плата",
#                     e."Расходы компании"/m.count_acc AS "Расходы компании",
#                     m."ВП после WB" - e."Расходы компании"/m.count_acc as "Чистая прибыль",
#                     ROUND((m."ВП после WB" - e."Расходы компании"/m.count_acc)/m."Выручка", 2) as "Чистая прибыль_pct",
#                     m."Удержания" / NULLIF(m."Выручка", 0) AS "Удержания_pct",
#                     m."Логистика" / NULLIF(m."Выручка", 0) AS "Логистика_pct",
#                     m."Перечисления по кредиту" / NULLIF(m."Выручка", 0) AS "Перечисления по кредиту_pct",
#                     m."ВП после WB" - e."Расходы компании"/m.count_acc AS "Чистая прибыль",
#                     ROUND(
#                         (m."ВП после WB" - e."Расходы компании"/m.count_acc)
#                         / NULLIF(m."Выручка", 0),
#                         2
#                     ) AS "Чистая прибыль_pct"
#                 FROM main m
#                 LEFT JOIN exp e
#                     ON m.year = e.e_year
#                     AND m.Месяц = e.e_month
#                 ORDER BY m.month desc;
#                         """

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