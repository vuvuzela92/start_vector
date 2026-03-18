from oauthlib.uri_validate import query

from src_oop.core.database import Database
from src_oop.core.scraper import HTTPClient
import pandas as pd
from time import time, sleep
import logging
from typing import List, Dict, Optional
import aiohttp
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import text

logger = logging.getLogger(__name__)

class ArticleAnalyze:
    """Класс для получения сложной аналитики из БД."""
    
    def __init__(self, engine, days_ago: int = 30, days_to: int = 1):
        self.engine = engine


    def get_general_stat(self, days_ago: int = 30, days_to: int = 1) -> pd.DataFrame:
        query = text(f"""SELECT o.date,
            DATE_PART('week', o.date) AS week_num,
            DATE_PART('month', o.date) AS month_num,
            a.account,
            a.local_vendor_code, -- wild
            o.article_id,
            cd.subject_name,
            COALESCE(o.orders_sum_rub/ NULLIF(o.orders_count, 0), COALESCE(ord.price_with_disc, 0)) AS price_with_disc, -- Наша_цена
            ord.spp,
            o.orders_sum_rub,
            o.orders_count,
            anpd.profit_by_orders, -- Прибыль_с_заказов_по_общим_условиям
            anpdc.profit_by_cond_orders, -- Прибыль_с_заказов_по_индивидуальным_условиям
            npfs.sales_sum,
            npfs.profit_by_cond_sales, -- Прибыль_с_продаж_по_индивидуальным_условиям
            o.open_card_count,
            o.add_to_cart_count,
            ROUND(COALESCE(o.add_to_cart_count, 0)/NULLIF(open_card_count::NUMERIC, 0),2) * 100 AS to_cart_convers,
            ROUND(COALESCE(o.orders_count, 0)/NULLIF(o.add_to_cart_count::NUMERIC, 0), 2)* 100 AS to_orders_convers,
            pami.manager,
            pami.promo_title,
            pami.promo_status,
            itr.central,
            itr.south,
            itr.privolzhskiy,
            itr.north_caucase,
            COALESCE(itr.total_quantity, ws.quantity) AS total_quantity,
            ord.central_fo_orders,
            ord.south_fo_orders,
            ord.privolzhskiy_fo_orders,
            ord.north_caucase_fo_orders,
            ord.far_eastern_fo_orders,
            ord.ural_fo_orders,
            ord.north_west_fo_orders,
            ord.syberian_fo_orders,
            cp.purchase_price,
            cwd.kgvp_marketplace,
            CASE
                WHEN o.date >= '2026-01-01'
                    THEN (ic_2026.fbo_individual_conditions)
                WHEN o.date BETWEEN '2025-01-01' AND '2025-12-31'
                    THEN (ic_2025.fbo_individual_conditions)
                ELSE 19
            END AS ind_comission,
            pd.logistic_from_wb_wh_to_opp,
            ord.fbo_orders,
            ord.fbs_orders,
            cd.rating,
            hs.end_of_day_balance,
            ws.in_way_to_client,
            ws.in_way_from_client 
        FROM orders_revenues o 
        LEFT JOIN card_data cd 
            ON o.article_id = cd.article_id
        LEFT JOIN article a
            ON a.nm_id = o.article_id
        LEFT JOIN (
        SELECT 
            ord."date",
            ord.article_id,
            ROUND(AVG(ord.spp)) AS spp,
            ROUND(AVG(ord.price_with_disc)) AS price_with_disc,
            SUM(CASE WHEN ord.oblast_okrug_name = 'Центральный федеральный округ' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS central_fo_orders,
            SUM(CASE WHEN ord.oblast_okrug_name = 'Южный федеральный округ' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS south_fo_orders,
            SUM(CASE WHEN ord.oblast_okrug_name = 'Приволжский федеральный округ' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS privolzhskiy_fo_orders,
            SUM(CASE WHEN ord.oblast_okrug_name = 'Северо-Кавказский федеральный округ' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS north_caucase_fo_orders,
            SUM(CASE WHEN ord.oblast_okrug_name = 'Дальневосточный федеральный округ' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS far_eastern_fo_orders,
            SUM(CASE WHEN ord.oblast_okrug_name = 'Уральский федеральный округ' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS ural_fo_orders,
            SUM(CASE WHEN ord.oblast_okrug_name = 'Северо-Западный федеральный округ' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS north_west_fo_orders,
            SUM(CASE WHEN ord.oblast_okrug_name = 'Сибирский федеральный округ' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS syberian_fo_orders,
            SUM(CASE WHEN ord.warehouse_type = 'Склад продавца' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS fbs_orders,
            SUM(CASE WHEN ord.warehouse_type = 'Склад WB' AND is_realization IS TRUE THEN 1 ELSE 0 END) AS fbo_orders
        FROM orders ord
        WHERE ord.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
        GROUP BY ord.article_id,
            ord."date"
                ) AS ord
        ON ord.article_id = o.article_id
            AND ord."date" = o."date"
        LEFT JOIN (
        SELECT cp.local_vendor_code,
            ROUND(AVG(cp.purchase_price)) AS purchase_price,
            cp.date
            FROM cost_price cp
            WHERE cp.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
            GROUP BY 
            cp.date,
            cp.local_vendor_code 
            ) cp
        ON cp.date = o."date" 
            AND cp.local_vendor_code = a.local_vendor_code 
        LEFT JOIN (
            SELECT anpd.article_id,
                SUM(anpd.sum_net_profit) AS profit_by_orders,
                anpd."date"
            FROM accurate_net_profit_data anpd
                WHERE anpd."date" BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
            GROUP BY anpd.article_id, anpd."date"
            ) anpd
        ON anpd.article_id = o.article_id
            AND anpd."date" = o."date"
        LEFT JOIN (
            SELECT anpdc.date,
            anpdc.article_id,
            anpdc.sum_net_profit AS profit_by_cond_orders
        FROM accurate_npd_purchase_calculation anpdc
        WHERE anpdc."date" BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}') anpdc
        ON anpdc.article_id = o.article_id
            AND anpdc."date" = o."date"
        LEFT JOIN 
            (
            SELECT
                npfs.article_id,
                npfs."date",
                SUM(npfs.sum_by_sales) AS sales_sum,
                SUM(npfs.result_net_profit) AS profit_by_cond_sales
            FROM net_profit_from_sales npfs
            WHERE npfs."date" BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
                AND npfs.article_id = 234321543
            GROUP BY npfs.article_id,
                    npfs."date" 
            ) npfs
        ON npfs.article_id = o.article_id
            AND npfs."date" = o."date"
        LEFT JOIN 
            (
            SELECT
                s.date,
                s.article_id,
                COUNT(s.is_realization) AS sales_count 
            FROM sales s
            WHERE is_realization IS TRUE
            AND s.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
            AND s.article_id = 234321543
            GROUP BY s.article_id, s.date
            ) s
        ON s.article_id = o.article_id
            AND s."date" = o."date"
        LEFT JOIN (
            SELECT  pami.date,
                pami.nm_id,
                pami.manager,
                pami.promo_title,
                pami.promo_status
        FROM promo_and_managers_info AS pami
        WHERE pami.date  BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
            ) pami
        ON pami.nm_id = o.article_id
            AND pami."date" = o."date"
        LEFT JOIN 
            (
            SELECT
            itr.article_id,
            itr.date,
            SUM(CASE WHEN itr.federal_district = 'Центральный' THEN quantity ELSE 0 END) AS central,
            SUM(CASE WHEN itr.federal_district = 'Южный' THEN quantity ELSE 0 END) AS south,
            SUM(CASE WHEN itr.federal_district = 'Приволжский' THEN quantity ELSE 0 END) AS privolzhskiy,
            SUM(CASE WHEN itr.federal_district = 'Северо-Кавказский' THEN quantity ELSE 0 END) AS north_caucase,
            SUM(quantity) AS total_quantity
        FROM inventory_turnover_by_reg AS itr
        WHERE itr.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
        GROUP BY itr.article_id,
                itr.date
            ) itr
        ON itr.article_id = o.article_id
            AND itr."date" = o."date"
        LEFT JOIN
            (
            SELECT date,
            subject_name,
            AVG(kgvp_marketplace) AS kgvp_marketplace
        FROM comission_wb_data
        WHERE date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
        GROUP BY date,
            subject_name) cwd
        ON cwd."date" = o."date"
            AND cwd.subject_name = cd.subject_name
        LEFT JOIN (
            SELECT ic_2026.subject_name,
            AVG(ic_2026.fbo_individual_conditions) AS fbo_individual_conditions,
            ic_2026.date_from
        FROM individual_conditions ic_2026
        WHERE ic_2026.date_from  >= '2026-01-01'
        GROUP BY ic_2026.subject_name,
            ic_2026.date_from
            ) ic_2026
        ON ic_2026.subject_name = cd.subject_name
        LEFT JOIN (
            SELECT ic_2025.subject_name,
            AVG(ic_2025.fbo_individual_conditions) AS fbo_individual_conditions,
            ic_2025.date_from
        FROM individual_conditions ic_2025
        WHERE ic_2025.date_from  BETWEEN '2026-01-01' AND '2026-12-31'
        GROUP BY ic_2025.subject_name,
        ic_2025.date_from
            ) ic_2025
        ON ic_2025.subject_name = cd.subject_name
        LEFT JOIN (
                SELECT
                date,
                pd.article_id,
                pd.discounted_price,
                pd.logistic_from_wb_wh_to_opp
            FROM prices_data pd
            WHERE date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
            ) pd
        ON pd.article_id = o.article_id
            AND pd."date" = o."date" 
        LEFT JOIN (
            SELECT DATE(ws.last_change_date) AS date,
                ws.nm_id,
                SUM(ws.in_way_to_client) AS in_way_to_client,
                SUM(ws.in_way_from_client) AS in_way_from_client,
                SUM(quantity) AS quantity
            FROM wb_stock ws
            WHERE DATE(ws.last_change_date) BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
            GROUP BY date, 
                    ws.nm_id) ws
        ON ws.nm_id = o.article_id 
            AND ws."date" = o."date"
        LEFT JOIN (
            SELECT hs.transaction_date,
                hs.end_of_day_balance,
                hs.wild
            FROM historical_stocks_fbs_service hs
            WHERE DATE(hs.transaction_date) BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
            ) hs
        ON hs.wild = a.local_vendor_code 
            AND hs.transaction_date = o.date
        WHERE o."date" BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
        ORDER BY o."date" DESC,
            o.orders_sum_rub DESC;""")
        
        # Используем контекстный менеджер соединения
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)
    
    def get_adv_stat(self, days_ago: int = 30, days_to: int = 1) -> pd.DataFrame:
        query = text(f"""SELECT as3.article_id,
            as3.date,
            SUM(as3.clicks) AS clicks,
            SUM(as3."views") AS "views",
            ROUND(AVG(as3.cpc), 2) AS cpc,
            ROUND(AVG(as3.cpm), 2) AS cpm,
            SUM(as2.upd_sum) AS adv_spend,
            SUM(CASE WHEN as2.payment_type IN ('Бонусы','Кэшбэк')
                            THEN as2.upd_sum END) AS bonuses
        FROM advert_stat as3
        LEFT JOIN advert_spend as2
        ON as2.advert_id = as3.campaign_id
        AND as2.date = as3.date
        WHERE as3.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
        AND as2.date BETWEEN CURRENT_DATE - INTERVAL '{days_ago} days' AND CURRENT_DATE - INTERVAL '{days_to}'
        GROUP BY as3.article_id,
            as3.date;
        """)

        # Используем контекстный менеджер соединения
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)
        

            
        # Используем контекстный менеджер соединения
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection)
        
    def merging_dataframes(self, days_ago: int = 30, days_to: int = 1) -> pd.DataFrame:
        df1 = self.get_adv_stat(days_ago, days_to)
        if df1:
            logger.info(f"Получено {len(df1)} строк из orders_revenues и связанных таблиц для периода с {days_ago} по {days_to} дней.")        
        df2 = self.get_general_stat(days_ago, days_to)
        if df2:
            logger.info(f"Получено {len(df2)} строк из advert_stat и advert_spend для периода с {days_ago} по {days_to} дней.")
        df = pd.merge(pd.merge(df1, df2, on=['article_id', 'date'], how='left'))
        # Оформляем данные в датафрейме
        df = df.drop_duplicates(subset=['date', 'article_id'])
        df = df.fillna(0)
        df = df.sort_values(by=['date', 'orders_sum_rub'], ascending=[False, False])
        return df