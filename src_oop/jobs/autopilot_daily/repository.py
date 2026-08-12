from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from src_oop.core.database import Database

logger = logging.getLogger(__name__)


class AutopilotDailyRepository:
    """Слой чтения данных PostgreSQL для дневного автопилота.

    Бизнес-логика:
    изолирует SQL для дневных метрик ПУ, исторических средних, средних позиций,
    справочника артикулов и заказов Сопоста. Так orchestration-код не смешивает
    правила Google Sheets с правилами получения данных из БД.
    """

    def __init__(self, database_cls: type[Database] = Database) -> None:
        """Создает repository дневного автопилота.

        Бизнес-логика:
        позволяет подменять подключение к PostgreSQL в тестах и проверять
        расчетные сценарии без реальной записи или чтения production-БД.
        """
        self.database_cls = database_cls

    def fetch_current_metrics(self) -> pd.DataFrame:
        """Читает текущие дневные метрики за последние 6 завершенных дней.

        Бизнес-логика:
        дневной ПУ показывает завершенные дни, поэтому запрос исключает текущую
        дату и берет период от `CURRENT_DATE - 6 days` до вчера. Процентные
        метрики сохраняются как доли, потому что форматирование процентов
        выполняется средствами Google Sheets.
        """
        query = text(
            """
            SELECT
                date,
                article_id,
                subject_name,
                account,
                local_vendor_code,
                orders_sum_rub,
                orders_count,
                adv_spend,
                price_with_disc,
                ROUND(spp / 100, 5) AS spp,
                total_quantity,
                profit_by_cond_orders,
                views,
                clicks,
                ROUND(ctr / 100, 5) AS ctr,
                ROUND(to_cart_convers / 100, 5) AS to_cart_convers,
                ROUND(to_orders_convers / 100, 5) AS to_orders_convers,
                add_to_cart_count,
                open_card_count,
                cpc,
                rating,
                CASE
                    WHEN orders_count = 0 THEN adv_spend
                    ELSE ROUND(adv_spend / orders_count, 2)
                END AS cpo,
                CASE WHEN promo_title != '' THEN 1 ELSE 0 END AS promo_status,
                profit_by_cond_orders - adv_spend AS net_profit_after_ad,
                CASE
                    WHEN orders_sum_rub = 0 THEN 1
                    ELSE ROUND(adv_spend / orders_sum_rub, 2)
                END AS advertising_cost_share,
                CASE
                    WHEN views = 0 THEN 0
                    ELSE ROUND(adv_spend / views * 1000, 2)
                END AS cpm,
                open_card_count - clicks AS organic
            FROM orders_articles_analyze
            WHERE date BETWEEN CURRENT_DATE - INTERVAL '6 days'
                AND CURRENT_DATE - INTERVAL '1 days'
            """
        )
        dataframe = self.database_cls.read_sql_to_dataframe(query)
        logger.info(
            "Текущие дневные метрики ПУ загружены из PostgreSQL: rows=%s",
            len(dataframe.index),
        )
        return dataframe

    def fetch_history_metrics(self) -> pd.DataFrame:
        """Читает исторические средние метрики для дневного ПУ.

        Бизнес-логика:
        сохраняет legacy-окна: средние за предыдущую неделю и средняя/медианная
        цена за последние 30 дней. `SELECT *` не используется, чтобы порядок и
        состав колонок был явно контролируемым перед записью в ПУ.
        """
        query = text(
            """
            WITH week_metrics AS (
                SELECT
                    article_id,
                    ROUND(AVG(orders_sum_rub), 2) AS avg_orders_sum_rub,
                    ROUND(AVG(orders_count), 2) AS avg_orders_count,
                    ROUND(AVG(adv_spend), 2) AS avg_adv_spend,
                    ROUND(AVG(price_with_disc), 2) AS avg_price_with_disc,
                    ROUND(AVG(spp) / 100, 5) AS avg_spp,
                    ROUND(AVG(total_quantity), 2) AS avg_total_quantity,
                    ROUND(AVG(profit_by_cond_orders), 2) AS avg_profit_by_cond_orders,
                    ROUND(AVG(views), 2) AS avg_views,
                    ROUND(AVG(clicks), 2) AS avg_clicks,
                    ROUND(AVG(ctr) / 100, 5) AS avg_ctr,
                    ROUND(AVG(to_cart_convers) / 100, 5) AS avg_to_cart_convers,
                    ROUND(AVG(to_orders_convers) / 100, 5) AS avg_to_orders_convers,
                    ROUND(AVG(add_to_cart_count), 2) AS avg_add_to_cart_count,
                    ROUND(AVG(open_card_count), 2) AS avg_open_card_count,
                    ROUND(AVG(cpc), 2) AS avg_cpc,
                    ROUND(AVG(rating), 2) AS avg_rating,
                    ROUND(AVG(profit_by_cond_orders), 2) - ROUND(AVG(adv_spend), 2)
                        AS avg_net_profit_after_ad,
                    ROUND(SUM(adv_spend) * 1000.0 / NULLIF(SUM(views), 0), 2)
                        AS avg_cpm,
                    ROUND(AVG(open_card_count - clicks), 2) AS avg_organic,
                    CASE
                        WHEN SUM(orders_sum_rub) = 0 THEN 1
                        ELSE ROUND(SUM(adv_spend) / SUM(orders_sum_rub), 2)
                    END AS avg_advertising_cost_share,
                    CASE
                        WHEN SUM(orders_count) = 0 THEN SUM(adv_spend)
                        ELSE ROUND(SUM(adv_spend) / SUM(orders_count), 2)
                    END AS avg_cpo
                FROM orders_articles_analyze
                WHERE date >= CURRENT_DATE - INTERVAL '2 weeks' + INTERVAL '1 day'
                    AND date < CURRENT_DATE - INTERVAL '1 week' + INTERVAL '1 day'
                GROUP BY article_id
            ),
            month_metrics AS (
                SELECT
                    article_id,
                    ROUND(AVG(price_with_disc), 2) AS month_avg_price_with_disc,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY price_with_disc)
                        AS month_median_price_with_disc
                FROM orders_articles_analyze
                WHERE date > CURRENT_DATE - INTERVAL '1 month'
                GROUP BY article_id
            )
            SELECT
                week_metrics.article_id,
                week_metrics.avg_orders_sum_rub,
                week_metrics.avg_orders_count,
                week_metrics.avg_adv_spend,
                week_metrics.avg_price_with_disc,
                week_metrics.avg_spp,
                week_metrics.avg_total_quantity,
                week_metrics.avg_profit_by_cond_orders,
                week_metrics.avg_views,
                week_metrics.avg_clicks,
                week_metrics.avg_ctr,
                week_metrics.avg_to_cart_convers,
                week_metrics.avg_to_orders_convers,
                week_metrics.avg_add_to_cart_count,
                week_metrics.avg_open_card_count,
                week_metrics.avg_cpc,
                week_metrics.avg_rating,
                week_metrics.avg_net_profit_after_ad,
                week_metrics.avg_cpm,
                week_metrics.avg_organic,
                week_metrics.avg_advertising_cost_share,
                week_metrics.avg_cpo,
                month_metrics.month_avg_price_with_disc,
                month_metrics.month_median_price_with_disc
            FROM week_metrics
            JOIN month_metrics ON week_metrics.article_id = month_metrics.article_id
            """
        )
        dataframe = self.database_cls.read_sql_to_dataframe(query)
        logger.info(
            "Исторические метрики ПУ загружены из PostgreSQL: rows=%s",
            len(dataframe.index),
        )
        return dataframe

    def fetch_current_avg_positions(self, articles: list[int]) -> pd.DataFrame:
        """Читает среднюю позицию за последние 6 дней и сортирует под ПУ.

        Бизнес-логика:
        позиции пишутся в отдельный недельный блок `IQ:IV`; отсутствующие
        артикулы остаются пустыми, чтобы не создавать ложные нули в таблице.
        """
        query = text(
            """
            SELECT
                nmid,
                avgposition,
                report_date
            FROM avg_position
            WHERE report_date >= CURRENT_DATE - INTERVAL '6 days'
                AND report_date < CURRENT_DATE
            """
        )
        dataframe = self.database_cls.read_sql_to_dataframe(query)
        if dataframe.empty:
            return pd.DataFrame(index=articles)

        dataframe["nmid"] = dataframe["nmid"].astype(int)
        pivot = dataframe.pivot(
            index="nmid",
            columns="report_date",
            values="avgposition",
        )
        pivot = pivot.reindex(articles).fillna("")
        logger.info("Текущие средние позиции загружены из PostgreSQL: rows=%s", len(pivot.index))
        return pivot

    def fetch_history_avg_positions(self, articles: list[int]) -> dict[int, object]:
        """Читает среднюю позицию за предыдущий период и сортирует под ПУ.

        Бизнес-логика:
        историческая позиция пишется одной колонкой `IP`; пропуски по артикулам
        сохраняются пустыми ячейками, чтобы пользователь видел отсутствие
        наблюдений, а не нулевую позицию.
        """
        rows = self.database_cls.read_sql_to_dict(
            """
            SELECT
                nmid,
                ROUND(AVG(avgposition), 3) AS avg_position_prior
            FROM avg_position
            WHERE report_date < CURRENT_DATE - INTERVAL '6 days'
            GROUP BY nmid
            """
        )
        values = {
            int(row["nmid"]): row["avg_position_prior"]
            for row in rows
            if row.get("nmid") is not None
        }
        ordered = {article: values.get(article, "") for article in articles}
        logger.info("Исторические средние позиции загружены из PostgreSQL: rows=%s", len(values))
        return ordered

    def fetch_vendor_codes_info(self, articles: list[int] | None = None) -> dict[int, dict[str, object]]:
        """Читает справочные поля артикула для колонок A:D в ПУ.

        Бизнес-логика:
        дневной сценарий поддерживает в ПУ актуальные предмет, личный кабинет и
        wild-код. Если по артикулу нет нового значения, слой записи оставляет
        старое значение из таблицы.
        """
        params: dict[str, int] = {}
        where = ""
        if articles:
            params = {f"article_{index}": article for index, article in enumerate(set(articles))}
            placeholders = ", ".join(f":{name}" for name in params)
            where = f"WHERE a.nm_id IN ({placeholders})"

        rows = self.database_cls.read_sql_to_dict(
            f"""
            SELECT DISTINCT ON (a.nm_id)
                a.nm_id,
                a.local_vendor_code,
                a.account,
                cd.subject_name AS category
            FROM article AS a
            LEFT JOIN card_data AS cd ON a.nm_id = cd.article_id
            {where}
            """,
            params=params,
        )
        result = {
            int(row["nm_id"]): {
                "local_vendor_code": row.get("local_vendor_code"),
                "account": row.get("account"),
                "category": row.get("category"),
            }
            for row in rows
            if row.get("nm_id") is not None
        }
        logger.info("Справочник артикулов для ПУ загружен из PostgreSQL: rows=%s", len(result))
        return result

    def fetch_sopost_orders(self) -> pd.DataFrame:
        """Читает заказы за последние 31 день для листа Сопост.

        Бизнес-логика:
        UNIT использует лист Сопост как оперативную витрину по wild-кодам.
        Текущий день исключается, вчерашний день добавляется пустым нулевым
        столбцом при отсутствии данных, как в legacy-сценарии.
        """
        query = text(
            """
            SELECT
                fd.date,
                a.local_vendor_code,
                SUM(fd.order_count) AS orders_count
            FROM funnel_daily AS fd
            LEFT JOIN article AS a ON fd.nm_id = a.nm_id
            WHERE fd.date > NOW() - INTERVAL '31 days'
                AND fd.date < CURRENT_DATE
                AND a.local_vendor_code LIKE 'wild%'
            GROUP BY fd.date, a.local_vendor_code
            ORDER BY fd.date DESC
            """
        )
        dataframe = self.database_cls.read_sql_to_dataframe(query)
        if dataframe.empty:
            return pd.DataFrame(columns=["local_vendor_code"])

        pivot = dataframe.pivot(
            index="local_vendor_code",
            columns="date",
            values="orders_count",
        )
        yesterday = date.today() - timedelta(days=1)
        if yesterday not in pivot.columns:
            pivot[yesterday] = 0
        pivot = pivot.reindex(sorted(pivot.columns, reverse=True), axis=1).fillna(0)
        result = pivot.reset_index()
        logger.info("Заказы для листа Сопост загружены из PostgreSQL: rows=%s", len(result.index))
        return result
