import pandas as pd
from sqlalchemy import text

class GetDataFromDB:
    """Класс для получения сложной аналитики из БД."""
    
    def __init__(self, engine):
        self.engine = engine

    def get_weekly_profit_report(self) -> pd.DataFrame:
            """Формирует отчет по чистой прибыли и расходам."""
            
            # Оборачиваем строку в text() для SQLAlchemy 2.0+
            query = text("""
                WITH exp AS (
                    SELECT
                        end_date,
                        SUM(CASE WHEN type='Расходы офис' THEN value ELSE 0 END) AS "Расходы офис",
                        -- ... остальные твои CASE WHEN ...
                        SUM(CASE WHEN type IN ('Расходы офис', 'Заработная плата') THEN value ELSE 0 END) AS "Расходы компании"
                    FROM expenses
                    GROUP BY end_date
                ),
                wfrm_agg AS (
                    SELECT
                        date_to,
                        SUM("Выручка") AS "Выручка",
                        SUM("Удержания") AS "Удержания",
                        SUM("Логистика") AS "Логистика",
                        SUM("ВП после ВБ") AS "ВП после ВБ"
                    FROM weekly_fin_reports_mv
                    GROUP BY date_to
                )
                SELECT
                    wfrm.*,
                    (w."ВП после ВБ" - e."Расходы компании") AS "Чистая прибыль",
                    e.*
                FROM weekly_fin_reports_mv wfrm
                LEFT JOIN wfrm_agg w ON wfrm.date_to = w.date_to
                LEFT JOIN exp e ON wfrm.date_to = e.end_date
                ORDER BY wfrm.date_to DESC;
            """)

            # Используем контекстный менеджер соединения
            with self.engine.connect() as connection:
                return pd.read_sql(query, connection)
            
    def get_monthly_profit_report(self) -> pd.DataFrame:
        """Формирует отчет по чистой прибыли и расходам."""
        
        # Оборачиваем строку в text() для SQLAlchemy 2.0+
        query = text("""
            WITH main AS (
                SELECT
                    DATE_TRUNC('month', date_from)::date AS month,
                    TO_CHAR(date_from, 'YYYY-MM') AS "Месяц",
                    EXTRACT(YEAR FROM date_from) AS year,
                    EXTRACT(MONTH FROM date_from) AS month_number,
                    SUM(wb_commission)                    AS "Комиссия WB",
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
                    ROUND(AVG(gp_after_wb_pct)/100,2) AS "ВП после WB_pct"
                FROM daily_fin_reports_agg
                GROUP BY
                    DATE_TRUNC('month', date_from),
                    TO_CHAR(date_from, 'YYYY-MM'),
                    EXTRACT(YEAR FROM date_from),
                    EXTRACT(MONTH FROM date_from)
            ),
            exp AS (
                SELECT
                    EXTRACT(YEAR FROM start_date) AS e_year,
                    month AS e_month,
                    -- Individual expense columns
                    SUM(CASE WHEN type='Расходы офис' THEN value ELSE 0 END) AS "Расходы офис",
                    SUM(CASE WHEN type='Услуги подбора персонала' THEN value ELSE 0 END) AS "Услуги подбора персонала",
                    SUM(CASE WHEN type='Арендные платежи (офис)' THEN value ELSE 0 END) AS "Арендные платежи (офис)",
                    SUM(CASE WHEN type='Услуги связи (интернет, телефон)' THEN value ELSE 0 END) AS "Услуги связи (интернет, телефон)",
                    SUM(CASE WHEN type='Стоянка' THEN value ELSE 0 END) AS "Стоянка",
                    SUM(CASE WHEN type='Эксплуатация здания' THEN value ELSE 0 END) AS "Эксплуатация здания",
                    SUM(CASE WHEN type='Коммунальные платежи' THEN value ELSE 0 END) AS "Коммунальные платежи",
                    SUM(CASE WHEN type='Расчету с поставщиком материалы' THEN value ELSE 0 END) AS "Расчеты с поставщиком материалы",
                    SUM(CASE WHEN type='Карго' THEN value ELSE 0 END) AS "Карго",
                    SUM(CASE WHEN type='Консультационные услуги по оформлении сделки' THEN value ELSE 0 END) AS "Консультационные услуги по оформлении сделки",
                    SUM(CASE WHEN type='Расходы склад' THEN value ELSE 0 END) AS "Расходы склад",
                    SUM(CASE WHEN type='Транспортные расходы, гсм' THEN value ELSE 0 END) AS "Транспортные расходы, ГСМ",
                    SUM(CASE WHEN type='Парковка' THEN value ELSE 0 END) AS "Парковка",
                    SUM(CASE WHEN type='Обслуживание а/м' THEN value ELSE 0 END) AS "Обслуживание а/м",
                    SUM(CASE WHEN type='Арендные платежи (склад)' THEN value ELSE 0 END) AS "Арендные платежи (склад)",
                    SUM(CASE WHEN type='Страховка' THEN value ELSE 0 END) AS "Страховка",
                    SUM(CASE WHEN type='ПО, сервисы, обслуживание ПО' THEN value ELSE 0 END) AS "ПО, сервисы, обслуживание ПО",
                    SUM(CASE WHEN type='Реклама/продвижение на площадках МП' THEN value ELSE 0 END) AS "Реклама/продвижение на площадках МП",
                    SUM(CASE WHEN type='Вб смена номера, перенос карточек' THEN value ELSE 0 END) AS "ВБ смена номера, перенос карточек",
                    SUM(CASE WHEN type='Услуги банка' THEN value ELSE 0 END) AS "Услуги банка",
                    SUM(CASE WHEN type='Налог: НДФЛ' THEN value ELSE 0 END) AS "Налог: НДФЛ",
                    SUM(CASE WHEN type='Налог: УСН' THEN value ELSE 0 END) AS "Налог: УСН",
                    SUM(CASE WHEN type='Налог: СВ' THEN value ELSE 0 END) AS "Налог: СВ",
                    SUM(CASE WHEN type='Налог: прочее' THEN value ELSE 0 END) AS "Налог: прочее",
                    SUM(CASE WHEN type='Налог:НДС' THEN value ELSE 0 END) AS "Налог: НДС",
                    SUM(CASE WHEN type='Штрафы, взыскания' THEN value ELSE 0 END) AS "Штрафы, взыскания",
                    SUM(CASE WHEN type='Проценты кредит ВБ' THEN value ELSE 0 END) AS "Проценты кредит ВБ",
                    SUM(CASE WHEN type='Проценты СИМПЛФИНАНС ООО МКК' THEN value ELSE 0 END) AS "Проценты СИМПЛФИНАНС",
                    SUM(CASE WHEN type='Займ ВБ Проценты' THEN value ELSE 0 END) AS "Займ ВБ: проценты",
                    SUM(CASE WHEN type='Кредит сберпроценты' THEN value ELSE 0 END) AS "Кредит Сбер: проценты",
                    SUM(CASE WHEN type='Рови факторинг: проценты' THEN value ELSE 0 END) AS "Рови факторинг: проценты",
                    SUM(CASE WHEN type='Рови факторинг:пени' THEN value ELSE 0 END) AS "Рови факторинг: пени",
                    SUM(CASE WHEN type='Лизинг, покупка оборудования, помещений' THEN value ELSE 0 END) AS "Лизинг, покупка оборудования, помещений",
                    SUM(CASE WHEN type='Заработная плата' THEN value ELSE 0 END) AS "Заработная плата",
                    -- Sum of all above as 'Расходы компании'
                    SUM(
                        CASE WHEN type IN (
                            'Расходы офис', 'Услуги подбора персонала', 'Арендные платежи (офис)',
                            'Услуги связи (интернет, телефон)', 'Стоянка', 'Эксплуатация здания',
                            'Коммунальные платежи', 'Расчету с поставщиком материалы', 'Карго',
                            'Консультационные услуги по оформлении сделки', 'Расходы склад',
                            'Транспортные расходы, гсм', 'Парковка', 'Обслуживание а/м',
                            'Арендные платежи (склад)', 'Страховка', 'ПО, сервисы, обслуживание ПО',
                            'Реклама/продвижение на площадках МП', 'ВБ смена номера, перенос карточек',
                            'Услуги банка', 'Налог: НДФЛ', 'Налог: УСН', 'Налог: СВ', 'Налог: прочее',
                            'Налог:НДС', 'Штрафы, взыскания', 'Проценты кредит ВБ', 'Проценты СИМПЛФИНАНС ООО МКК',
                            'Займ ВБ Проценты', 'Кредит сберпроценты', 'Рови факторинг: проценты',
                            'Рови факторинг:пени', 'Лизинг, покупка оборудования, помещений', 'Заработная плата'
                        ) THEN value ELSE 0 END
                    ) AS "Расходы компании"
                FROM expenses
                GROUP BY EXTRACT(YEAR FROM start_date), month
            )
            SELECT
                m.*,
                e.*,
                m."ВП после WB" - e."Расходы компании" as "Чистая прибыль",
                ROUND((m."ВП после WB" - e."Расходы компании")/m."Выручка", 2) as "Чистая прибыль_pct",
                m."Удержания" / NULLIF(m."Выручка", 0) AS "Удержания_pct",
                m."Логистика" / NULLIF(m."Выручка", 0) AS "Логистика_pct",
                m."Перечисления по кредиту" / NULLIF(m."Выручка", 0) AS "Перечисления по кредиту_pct",
                m."ВП после WB" - e."Расходы компании" AS "Чистая прибыль",
                ROUND(
                    (m."ВП после WB" - e."Расходы компании")
                    / NULLIF(m."Выручка", 0),
                    2
                ) AS "Чистая прибыль_pct"
            FROM main m
            LEFT JOIN exp e
                ON m.year = e.e_year
            AND m.month_number = e.e_month
            ORDER BY m.month desc;
        """)

        # Используем контекстный менеджер соединения
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)
        

    def get_outcomes_detalize(self):
        """Таблица Расходы: Детализация"""
        query = text("""
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
                    """)
        # Используем контекстный менеджер соединения
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)


    def get_fin_deductions_mv(self):
        "Удержания: Детализация"
        query = text("""
        SELECT *
            FROM fin_deductions_mv
        """)
        # Используем контекстный менеджер соединения
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)


    def get_daily_fin_reports_deductions_agg(self):
        "Удержания: Детализация"
        query = text("""
            SELECT TO_CHAR(f.date_from, 'MM-YYYY') AS month,
            f.grouped_bonus_type_name,
            f.total_deduction
            FROM daily_fin_reports_deductions f
            WHERE f.grouped_bonus_type_name IS NOT NULL
            ORDER BY f.date_from desc;
        """)
        # Используем контекстный менеджер соединения
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)
    
    def get_wild_frm_products(self):
            "Удержания: Детализация"
            query = text("""
                SELECT DISTINCT(p.id)
                    FROM products p
            """)
            # Используем контекстный менеджер соединения
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
                return df['id'].to_list()
                     