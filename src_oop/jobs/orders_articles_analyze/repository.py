import logging

from sqlalchemy import text

from src_oop.core.database import Database


logger = logging.getLogger(__name__)


class ArticleAnalyzeRepository:
    """Содержит SQL-запросы для формирования артикульного анализа."""

    def __init__(self, days_ago: int = 2, days_to: int = 1):
        self.days_ago = days_ago
        self.days_to = days_to

    def get_general_stat(self, days_ago: int, days_to: int):
        """Возвращает общую статистику по артикулам за указанный период."""
        logger.info(
            "Начато выполнение запроса общей статистики | method=get_general_stat | days_ago=%s | days_to=%s",
            days_ago,
            days_to,
        )
        query = text(f"""
            WITH raw_base AS (
                SELECT nm_id AS article_id, "date"
                FROM funnel_daily
                WHERE "date" BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'

                UNION ALL

                SELECT nm_id AS article_id, DATE(date_from) AS "date"
                FROM daily_fin_reports_full
                WHERE DATE(date_from) BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'

                UNION ALL

                SELECT article_id, "date"
                FROM sales
                WHERE "date" BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
            ),
            base AS (
                SELECT article_id, "date"
                FROM raw_base
                GROUP BY article_id, "date"
            )
            SELECT
                b.date,
                DATE_PART('week', b.date) AS week_num,
                DATE_PART('month', b.date) AS month_num,
                a.account,
                a.local_vendor_code,
                b.article_id,
                cd.subject_name,
                COALESCE(o.avg_price, ord.price_with_disc, 0) AS price_with_disc,
                ord.spp,
                COALESCE(o.orders_sum, 0) AS orders_sum_rub,
                COALESCE(o.order_count, 0) AS orders_count,
                COALESCE(anpd.profit_by_orders, 0) AS profit_by_orders,

                COALESCE(o.order_count, 0) * (
                    COALESCE(o.avg_price, ord.price_with_disc, 0) - (
                        (COALESCE(o.avg_price, ord.price_with_disc, 0) / 100.0 * (8 +
                            CASE
                                WHEN b.date >= '2026-01-01' THEN ic_2026.fbo_individual_conditions
                                WHEN b.date BETWEEN '2025-01-01' AND '2025-12-31' THEN ic_2025.fbo_individual_conditions
                                ELSE 19
                            END)) + COALESCE(cp.purchase_price, 0)
                    )
                ) AS profit_by_cond_orders,

                COALESCE(s.sales_sum, 0) AS sales_sum,
                COALESCE(s.sales_count, 0) AS sales_count,

                ROUND(
                    COALESCE(s.sales_count, 0) * (
                        COALESCE(s.sales_sum / NULLIF(s.sales_count, 0), 0) - (
                            (COALESCE(s.sales_sum / NULLIF(s.sales_count, 0), 0) / 100.0 * (6 +
                                CASE
                                    WHEN b.date >= '2026-01-01' THEN ic_2026.fbo_individual_conditions
                                    WHEN b.date BETWEEN '2025-01-01' AND '2025-12-31' THEN ic_2025.fbo_individual_conditions
                                    ELSE 19
                                END)) + COALESCE(cp.purchase_price, 0)
                        )
                    ), 2
                ) AS profit_by_cond_sales,

                COALESCE(o.open_count, 0) AS open_card_count,
                COALESCE(o.cart_count, 0) AS add_to_cart_count,
                ROUND(COALESCE(o.cart_count, 0) / NULLIF(o.open_count::NUMERIC, 0), 2) * 100 AS to_cart_convers,
                ROUND(COALESCE(o.order_count, 0) / NULLIF(o.cart_count::NUMERIC, 0), 2) * 100 AS to_orders_convers,
                pami.manager, pami.promo_title, pami.promo_status,
                itr.central, itr.south, itr.privolzhskiy, itr.north_caucase,
                COALESCE(itr.total_quantity, ws.quantity) AS total_quantity,
                ord.central_fo_orders, ord.south_fo_orders, ord.privolzhskiy_fo_orders, ord.north_caucase_fo_orders, ord.far_eastern_fo_orders, ord.ural_fo_orders, ord.north_west_fo_orders, ord.syberian_fo_orders,
                cp.purchase_price,
                cwd.kgvp_marketplace,
                CASE
                    WHEN b.date >= '2026-01-01' THEN ic_2026.fbo_individual_conditions
                    WHEN b.date BETWEEN '2025-01-01' AND '2025-12-31' THEN ic_2025.fbo_individual_conditions
                    ELSE 19
                END AS ind_comission_fbo,
                pd.logistic_from_wb_wh_to_opp, ord.fbo_orders, ord.fbs_orders, cd.rating, hs.end_of_day_balance,
                ws.in_way_to_client, ws.in_way_from_client,
                COALESCE(fin.sales_revenue_rep, 0) AS sales_revenue_rep,
                COALESCE(fin.sales_revenue_rep, 0) - COALESCE(fin.wb_commission_rep, 0) - COALESCE(fin.logistics, 0) - (COALESCE(cp.purchase_price, 0) * COALESCE(fin.sales_count_rep, 0)) AS sales_profit_cond_rep,
                COALESCE(fin.wb_commission_rep, 0) AS wb_commission_rep,
                COALESCE(fin.logistics, 0) AS logistics,
                COALESCE(fin.sales_count_rep, 0) AS sales_count_rep,
                COALESCE(fin.returns_count_rep, 0) AS returns_count_rep,
                COALESCE(fin.sales_count_rep, 0) * COALESCE(cp.purchase_price, 0) AS cost_price_sales_fin_rep,
                COALESCE(fin.returns_count_rep, 0) * COALESCE(cp.purchase_price, 0) AS cost_price_returns_fin_rep
            FROM base b
            LEFT JOIN article a ON a.nm_id = b.article_id
            LEFT JOIN card_data cd ON b.article_id = cd.article_id
            LEFT JOIN funnel_daily o ON o.nm_id = b.article_id AND o."date" = b."date"
            LEFT JOIN (
                SELECT ord."date", ord.article_id, ROUND(AVG(ord.spp)) AS spp, ROUND(AVG(ord.price_with_disc)) AS price_with_disc,
                    SUM(CASE WHEN ord.oblast_okrug_name = 'Р¦РµРЅС‚СЂР°Р»СЊРЅС‹Р№ С„РµРґРµСЂР°Р»СЊРЅС‹Р№ РѕРєСЂСѓРі' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS central_fo_orders,
                    SUM(CASE WHEN ord.oblast_okrug_name = 'Р®Р¶РЅС‹Р№ С„РµРґРµСЂР°Р»СЊРЅС‹Р№ РѕРєСЂСѓРі' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS south_fo_orders,
                    SUM(CASE WHEN ord.oblast_okrug_name = 'РџСЂРёРІРѕР»Р¶СЃРєРёР№ С„РµРґРµСЂР°Р»СЊРЅС‹Р№ РѕРєСЂСѓРі' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS privolzhskiy_fo_orders,
                    SUM(CASE WHEN ord.oblast_okrug_name = 'РЎРµРІРµСЂРѕ-РљР°РІРєР°Р·СЃРєРёР№ С„РµРґРµСЂР°Р»СЊРЅС‹Р№ РѕРєСЂСѓРі' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS north_caucase_fo_orders,
                    SUM(CASE WHEN ord.oblast_okrug_name = 'Р”Р°Р»СЊРЅРµРІРѕСЃС‚РѕС‡РЅС‹Р№ С„РµРґРµСЂР°Р»СЊРЅС‹Р№ РѕРєСЂСѓРі' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS far_eastern_fo_orders,
                    SUM(CASE WHEN ord.oblast_okrug_name = 'РЈСЂР°Р»СЊСЃРєРёР№ С„РµРґРµСЂР°Р»СЊРЅС‹Р№ РѕРєСЂСѓРі' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS ural_fo_orders,
                    SUM(CASE WHEN ord.oblast_okrug_name = 'РЎРµРІРµСЂРѕ-Р—Р°РїР°РґРЅС‹Р№ С„РµРґРµСЂР°Р»СЊРЅС‹Р№ РѕРєСЂСѓРі' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS north_west_fo_orders,
                    SUM(CASE WHEN ord.oblast_okrug_name = 'РЎРёР±РёСЂСЃРєРёР№ С„РµРґРµСЂР°Р»СЊРЅС‹Р№ РѕРєСЂСѓРі' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS syberian_fo_orders,
                    SUM(CASE WHEN ord.warehouse_type = 'РЎРєР»Р°Рґ РїСЂРѕРґР°РІС†Р°' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS fbs_orders,
                    SUM(CASE WHEN ord.warehouse_type = 'РЎРєР»Р°Рґ WB' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS fbo_orders
                FROM orders ord
                WHERE ord.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY ord.article_id, ord."date"
            ) AS ord ON ord.article_id = b.article_id AND ord."date" = b."date"
            LEFT JOIN (
                SELECT cp.local_vendor_code, ROUND(AVG(cp.purchase_price)) AS purchase_price, cp.date
                FROM cost_price cp
                WHERE cp.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY cp.date, cp.local_vendor_code
            ) cp ON cp.date = b."date" AND cp.local_vendor_code = a.local_vendor_code
            LEFT JOIN (
                SELECT anpd.article_id, SUM(anpd.sum_net_profit) AS profit_by_orders, anpd."date"
                FROM accurate_net_profit_data anpd
                WHERE anpd."date" BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY anpd.article_id, anpd."date"
            ) anpd ON anpd.article_id = b.article_id AND anpd."date" = b."date"
            LEFT JOIN (
                SELECT s.date, s.article_id, SUM(s.price_with_disc) AS sales_sum, COUNT(s.is_realization) AS sales_count
                FROM sales s
                WHERE is_realization IS TRUE AND s.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY s.article_id, s.date
            ) s ON s.article_id = b.article_id AND s."date" = b."date"
            LEFT JOIN (
                SELECT pami.date, pami.nm_id, pami.manager, pami.promo_title, pami.promo_status
                FROM promo_and_managers_info AS pami
                WHERE pami.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
            ) pami ON pami.nm_id = b.article_id AND pami."date" = b."date"
            LEFT JOIN (
                SELECT itr.article_id, itr.date,
                    SUM(CASE WHEN itr.federal_district = 'Р¦РµРЅС‚СЂР°Р»СЊРЅС‹Р№' THEN quantity ELSE 0 END) AS central,
                    SUM(CASE WHEN itr.federal_district = 'Р®Р¶РЅС‹Р№' THEN quantity ELSE 0 END) AS south,
                    SUM(CASE WHEN itr.federal_district = 'РџСЂРёРІРѕР»Р¶СЃРєРёР№' THEN quantity ELSE 0 END) AS privolzhskiy,
                    SUM(CASE WHEN itr.federal_district = 'РЎРµРІРµСЂРѕ-РљР°РІРєР°Р·СЃРєРёР№' THEN quantity ELSE 0 END) AS north_caucase,
                    SUM(quantity) AS total_quantity
                FROM inventory_turnover_by_reg AS itr
                WHERE itr.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY itr.article_id, itr.date
            ) itr ON itr.article_id = b.article_id AND itr."date" = b."date"
            LEFT JOIN (
                SELECT date, subject_name, AVG(kgvp_marketplace) AS kgvp_marketplace
                FROM comission_wb_data
                WHERE date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY date, subject_name
            ) cwd ON cwd."date" = b."date" AND cwd.subject_name = cd.subject_name
            LEFT JOIN (
                SELECT ic_2026.subject_name, AVG(ic_2026.fbo_individual_conditions) AS fbo_individual_conditions, ic_2026.date_from, ic_2026.date_to
                FROM individual_conditions ic_2026
                WHERE ic_2026.date_from >= '2026-01-01'
                GROUP BY ic_2026.subject_name, ic_2026.date_from, ic_2026.date_to
            ) ic_2026 ON ic_2026.subject_name = cd.subject_name AND b.date BETWEEN ic_2026.date_from AND ic_2026.date_to
            LEFT JOIN (
                SELECT ic_2025.subject_name, AVG(ic_2025.fbo_individual_conditions) AS fbo_individual_conditions, ic_2025.date_from, ic_2025.date_to
                FROM individual_conditions ic_2025
                WHERE ic_2025.date_from BETWEEN '2025-01-01' AND '2025-12-31'
                GROUP BY ic_2025.subject_name, ic_2025.date_from, ic_2025.date_to
            ) ic_2025 ON ic_2025.subject_name = cd.subject_name AND b.date BETWEEN ic_2025.date_from AND ic_2025.date_to
            LEFT JOIN (
                SELECT date, pd.article_id, pd.discounted_price, pd.logistic_from_wb_wh_to_opp
                FROM prices_data pd
                WHERE date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
            ) pd ON pd.article_id = b.article_id AND pd."date" = b."date"
            LEFT JOIN (
                SELECT DATE(ws.last_change_date) AS date, ws.nm_id, SUM(ws.in_way_to_client) AS in_way_to_client, SUM(ws.in_way_from_client) AS in_way_from_client, SUM(quantity) AS quantity
                FROM wb_stock ws
                WHERE DATE(ws.last_change_date) BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY date, ws.nm_id
            ) ws ON ws.nm_id = b.article_id AND ws."date" = b."date"
            LEFT JOIN (
                SELECT hs.transaction_date, hs.end_of_day_balance, hs.wild
                FROM historical_stocks_fbs_service hs
                WHERE DATE(hs.transaction_date) BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
            ) hs ON hs.wild = a.local_vendor_code AND hs.transaction_date = b.date
            LEFT JOIN (
                SELECT fin.nm_id, DATE(fin.date_from) AS date_from,
                    SUM(CASE WHEN fin.supplier_oper_name = 'РџСЂРѕРґР°Р¶Р°' THEN fin.retail_price_withdisc_rub ELSE 0 END) -
                    SUM(CASE WHEN fin.supplier_oper_name = 'Р’РѕР·РІСЂР°С‚' THEN fin.retail_price_withdisc_rub ELSE 0 END) -
                    SUM(CASE WHEN fin.supplier_oper_name = 'РљРѕСЂСЂРµРєС†РёСЏ РІРѕР·РІСЂР°С‚РѕРІ' THEN fin.retail_price_withdisc_rub ELSE 0 END) +
                    SUM(CASE WHEN fin.supplier_oper_name = 'РљРѕСЂСЂРµРєС†РёСЏ РїСЂРѕРґР°Р¶' THEN fin.retail_price_withdisc_rub ELSE 0 END) AS sales_revenue_rep,
                    SUM(CASE WHEN fin.supplier_oper_name = 'РџСЂРѕРґР°Р¶Р°' THEN fin.retail_price_withdisc_rub ELSE 0 END) -
                    SUM(CASE WHEN fin.supplier_oper_name = 'Р’РѕР·РІСЂР°С‚' THEN fin.retail_price_withdisc_rub ELSE 0 END) -
                    (
                        SUM(CASE WHEN fin.supplier_oper_name = 'РџСЂРѕРґР°Р¶Р°' THEN fin.ppvz_for_pay ELSE 0 END) -
                        SUM(CASE WHEN fin.supplier_oper_name = 'РљРѕСЂСЂРµРєС†РёСЏ РїСЂРѕРґР°Р¶' THEN fin.ppvz_for_pay ELSE 0 END) +
                        SUM(CASE WHEN fin.supplier_oper_name = 'Р”РѕР±СЂРѕРІРѕР»СЊРЅР°СЏ РєРѕРјРїРµРЅСЃР°С†РёСЏ РїСЂРё РІРѕР·РІСЂР°С‚Рµ' THEN fin.ppvz_for_pay ELSE 0 END) +
                        SUM(CASE WHEN fin.supplier_oper_name = 'РљРѕСЂСЂРµРєС†РёСЏ РІРѕР·РІСЂР°С‚РѕРІ' THEN fin.ppvz_for_pay ELSE 0 END) -
                        SUM(CASE WHEN fin.supplier_oper_name = 'Р’РѕР·РІСЂР°С‚' THEN fin.ppvz_for_pay ELSE 0 END) +
                        SUM(CASE WHEN fin.supplier_oper_name = 'РљРѕРјРїРµРЅСЃР°С†РёСЏ СѓС‰РµСЂР±Р°' THEN fin.ppvz_for_pay ELSE 0 END) +
                        SUM(CASE WHEN fin.supplier_oper_name = 'РљРѕСЂСЂРµРєС‚РёСЂРѕРІРєР° СЌРєРІР°Р№СЂРёРЅРіР°' THEN fin.ppvz_for_pay ELSE 0 END)
                    ) AS wb_commission_rep,
                    SUM(CASE WHEN fin.supplier_oper_name = 'Р›РѕРіРёСЃС‚РёРєР°' THEN fin.delivery_rub ELSE 0 END) +
                    SUM(CASE WHEN fin.supplier_oper_name = 'РљРѕСЂСЂРµРєС†РёСЏ Р»РѕРіРёСЃС‚РёРєРё' THEN fin.delivery_rub ELSE 0 END) AS logistics,
                    SUM(CASE WHEN fin.doc_type_name = 'РџСЂРѕРґР°Р¶Р°' THEN fin.quantity ELSE 0 END) AS sales_count_rep,
                    SUM(CASE WHEN fin.doc_type_name = 'Р’РѕР·РІСЂР°С‚' THEN fin.quantity ELSE 0 END) AS returns_count_rep
                FROM daily_fin_reports_full fin
                WHERE DATE(fin.date_from) BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY fin.nm_id, DATE(fin.date_from)
            ) fin ON fin.date_from = b.date AND fin.nm_id = b.article_id
            ORDER BY b."date" DESC, orders_sum_rub DESC;""")
        df = Database.read_sql_to_dataframe(query)
        logger.info(
            "Запрос общей статистики выполнен | method=get_general_stat | rows=%s | columns=%s",
            len(df.index),
            list(df.columns),
        )
        return df

    def get_adv_stat(self, days_ago: int, days_to: int):
        """Возвращает рекламную статистику по артикулам за указанный период."""
        logger.info(
            "Начато выполнение запроса рекламной статистики | method=get_adv_stat | days_ago=%s | days_to=%s",
            days_ago,
            days_to,
        )
        query = text(f"""WITH spend_agg AS (
                SELECT
                    advert_id,
                    date,
                    SUM(upd_sum) AS adv_spend,
                    SUM(CASE WHEN payment_type IN ('Р‘РѕРЅСѓСЃС‹','РљСЌС€Р±СЌРє') THEN upd_sum END) AS bonuses
                FROM advert_spend
                WHERE date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to} days'
                GROUP BY advert_id, date
            )
            SELECT
                as3.article_id,
                as3.date,
                SUM(as3.clicks) AS clicks,
                SUM(as3."views") AS "views",
                ROUND((SUM(as3.clicks)::NUMERIC / NULLIF(SUM(as3."views"), 0)) * 100, 2) AS ctr,
                ROUND(AVG(as3.cpc), 2) AS cpc,
                ROUND(AVG(as3.cpm), 2) AS cpm,
                SUM(spend_agg.adv_spend) AS adv_spend,
                SUM(spend_agg.bonuses) AS bonuses
            FROM advert_stat as3
            LEFT JOIN spend_agg ON spend_agg.advert_id = as3.campaign_id AND spend_agg.date = as3.date
            WHERE as3.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days'
                AND CURRENT_DATE - INTERVAL '{days_to} days'
            GROUP BY as3.article_id, as3.date;""")
        df = Database.read_sql_to_dataframe(query)
        logger.info(
            "Запрос рекламной статистики выполнен | method=get_adv_stat | rows=%s | columns=%s",
            len(df.index),
            list(df.columns),
        )
        return df

    def get_all_goods_directory(self):
        """Возвращает полный справочник товаров без фильтрации по датам."""
        logger.info("Начато получение полного справочника товаров | method=get_all_goods_directory")
        query = text("""
            SELECT DISTINCT a.nm_id AS article_id, a.account, a.local_vendor_code, cd.subject_name
            FROM article a
            LEFT JOIN card_data cd ON a.nm_id = cd.article_id
        """)
        df = Database.read_sql_to_dataframe(query)
        logger.info(
            "Справочник товаров получен | method=get_all_goods_directory | rows=%s | columns=%s",
            len(df.index),
            list(df.columns),
        )
        return df
