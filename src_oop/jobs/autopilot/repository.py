from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal

from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.jobs.autopilot.models import (
    MetricValues,
    ProfitCalculationInputs,
    WBCardSnapshot,
)

logger = logging.getLogger(__name__)


class AutopilotRepository:
    def __init__(self, database_cls: type[Database] = Database) -> None:
        """
        Инициализирует слой доступа к PostgreSQL для hourly autpilot job.

        Бизнес-логика:
        отделяет чтение справочников и запись истории СПП от orchestration-кода,
        чтобы бизнес-расчет можно было проверять без прямой работы с БД.
        """
        self.database_cls = database_cls

    def fetch_article_accounts(self, articles: list[int]) -> dict[int, str]:
        """
        Возвращает соответствие артикулов ПУ и личных кабинетов WB.

        Бизнес-логика:
        нужно сгруппировать артикулы по токенам WB перед запросами воронки и цен.
        Артикула без найденного кабинета не участвуют во внешних WB-запросах.
        """
        if not articles:
            return {}

        params = {
            f"article_{index}": article_id
            for index, article_id in enumerate(sorted(set(articles)))
        }
        placeholders = ", ".join(f":{name}" for name in params)
        rows = self.database_cls.read_sql_to_dict(
            f"""
            SELECT c.article_id, a.account
            FROM card_data AS c
            JOIN article AS a ON c.article_id = a.nm_id
            WHERE c.article_id IN ({placeholders})
            """,
            params=params,
        )
        result = {
            int(row["article_id"]): str(row["account"]).strip()
            for row in rows
            if row.get("article_id") is not None and row.get("account") is not None
        }
        logger.info("Соответствие артикулов и кабинетов WB загружено из БД: rows=%s", len(result))
        return result

    def fetch_today_advert_activity(self, report_date: date) -> dict[int, dict[str, float]]:
        """
        Читает клики и показы из текущей таблицы `advert_stat` за дату отчета.

        Бизнес-логика:
        расходы берутся из Cometa, но клики/показы используются из актуального WB-контура,
        чтобы посчитать CTR, CPC и CPM по legacy-формулам на уровне артикула.
        """
        dataframe = self.database_cls.read_sql_to_dataframe(
            text(
                """
                SELECT article_id, SUM(clicks) AS clicks, SUM(views) AS views
                FROM advert_stat
                WHERE date = :report_date
                GROUP BY article_id
                """
            ),
            params={"report_date": report_date},
        )
        if dataframe.empty:
            return {}

        result: dict[int, dict[str, float]] = {}
        for row in dataframe.to_dict(orient="records"):
            try:
                article_id = int(row["article_id"])
            except (TypeError, ValueError):
                continue
            result[article_id] = {
                "clicks": float(row.get("clicks") or 0),
                "views": float(row.get("views") or 0),
            }
        logger.info("Рекламная активность загружена из БД для расчета ПУ: rows=%s", len(result))
        return result

    def fetch_profit_calculation_inputs(
        self,
        report_date: date,
        articles: list[int],
    ) -> ProfitCalculationInputs:
        """
        Читает справочные компоненты для расчета прибыли по ИУ.

        Бизнес-логика:
        hourly-сценарий считает прибыль за текущий день из свежей Воронки WB,
        но использует те же справочники, что daily-витрина
        `orders_articles_analyze`: закупочную цену по `local_vendor_code` и
        индивидуальные условия FBO по предмету товара. Для закупки берется
        последняя доступная дата не позже даты отчета, потому что справочник
        может обновляться позже текущего дня. Если справочник не нашелся,
        calculator применит SQL fallback daily-формулы: закупка `0`,
        индивидуальные условия `19`.
        """
        if not articles:
            return {}

        params: dict[str, int | date] = {"report_date": report_date}
        article_params = {
            f"article_{index}": article_id
            for index, article_id in enumerate(sorted(set(articles)))
        }
        params.update(article_params)
        placeholders = ", ".join(f":{name}" for name in article_params)

        dataframe = self.database_cls.read_sql_to_dataframe(
            text(
                f"""
                SELECT
                    base.article_id,
                    cp.price_date AS purchase_price_date,
                    COALESCE(cp.purchase_price, 0) AS purchase_price,
                    COALESCE(
                        CASE
                            WHEN :report_date >= DATE '2026-01-01'
                                THEN ic_2026.fbo_individual_conditions
                            WHEN :report_date BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
                                THEN ic_2025.fbo_individual_conditions
                            ELSE 19
                        END,
                        19
                    ) AS fbo_individual_conditions
                FROM (
                    SELECT
                        c.article_id,
                        a.local_vendor_code,
                        cd.subject_name
                    FROM card_data AS c
                    LEFT JOIN article AS a ON c.article_id = a.nm_id
                    LEFT JOIN card_data AS cd ON c.article_id = cd.article_id
                    WHERE c.article_id IN ({placeholders})
                ) AS base
                LEFT JOIN (
                    SELECT DISTINCT ON (local_vendor_code)
                        local_vendor_code,
                        date AS price_date,
                        ROUND(AVG(purchase_price) OVER (
                            PARTITION BY local_vendor_code, date
                        )) AS purchase_price
                    FROM cost_price
                    WHERE date <= :report_date
                    ORDER BY local_vendor_code, date DESC
                ) AS cp ON cp.local_vendor_code = base.local_vendor_code
                LEFT JOIN (
                    SELECT
                        subject_name,
                        AVG(fbo_individual_conditions) AS fbo_individual_conditions,
                        date_from,
                        date_to
                    FROM individual_conditions
                    WHERE date_from >= DATE '2026-01-01'
                    GROUP BY subject_name, date_from, date_to
                ) AS ic_2026
                    ON ic_2026.subject_name = base.subject_name
                    AND :report_date BETWEEN ic_2026.date_from AND ic_2026.date_to
                LEFT JOIN (
                    SELECT
                        subject_name,
                        AVG(fbo_individual_conditions) AS fbo_individual_conditions,
                        date_from,
                        date_to
                    FROM individual_conditions
                    WHERE date_from BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
                    GROUP BY subject_name, date_from, date_to
                ) AS ic_2025
                    ON ic_2025.subject_name = base.subject_name
                    AND :report_date BETWEEN ic_2025.date_from AND ic_2025.date_to
                """
            ),
            params=params,
        )
        if dataframe.empty:
            logger.warning(
                "Справочные данные для расчета прибыли по ИУ не найдены для почасового ПУ: "
                "report_date=%s articles=%s",
                report_date.strftime("%Y-%m-%d"),
                len(set(articles)),
            )
            return {}

        result: ProfitCalculationInputs = {}
        for row in dataframe.to_dict(orient="records"):
            article_id = row.get("article_id")
            if article_id is None:
                continue
            try:
                result[int(article_id)] = {
                    "purchase_price": float(row.get("purchase_price") or 0),
                    "fbo_individual_conditions": float(
                        row.get("fbo_individual_conditions") or 19
                    ),
                }
            except (TypeError, ValueError):
                continue
        logger.info(
            "Справочные данные для расчета прибыли по ИУ загружены для почасового ПУ: "
            "report_date=%s rows=%s",
            report_date.strftime("%Y-%m-%d"),
            len(result),
        )
        return result

    def insert_spp_history_changes(self, snapshots: dict[int, WBCardSnapshot]) -> int:
        """
        Записывает изменения полной цены и цены СПП в `spp_history`.

        Бизнес-логика:
        сохраняет legacy-ограничение: запись выполняется не чаще одного раза в час
        и только для товаров, у которых изменилась полная цена или цена с СПП.
        Некорректные/неполные карточки не попадают в историю.
        """
        if not snapshots:
            return 0

        engine = self.database_cls.get_engine()
        with engine.begin() as connection:
            has_current_hour = connection.execute(
                text(
                    """
                    SELECT 1
                    FROM spp_history
                    WHERE date(created_at) = current_date
                        AND date_part('hour', created_at) = date_part('hour', NOW())
                    LIMIT 1
                    """
                )
            ).first()
            if has_current_hour:
                logger.info(
                    "В spp_history уже есть записи за текущий час, новая запись изменений СПП пропущена."
                )
                return 0

            last_rows = connection.execute(
                text(
                    """
                    SELECT DISTINCT ON (nm_id) nm_id, full_price, spp_price
                    FROM spp_history
                    ORDER BY nm_id, created_at DESC
                    """
                )
            ).mappings()
            last_data = {
                int(row["nm_id"]): {
                    "full_price": self._as_float(row["full_price"]),
                    "spp_price": self._as_float(row["spp_price"]),
                }
                for row in last_rows
            }

            records: list[dict[str, float | int]] = []
            for article_id, snapshot in snapshots.items():
                if (
                    snapshot.full_price is None
                    or snapshot.discounted_price is None
                    or snapshot.spp is None
                ):
                    continue
                previous = last_data.get(article_id, {})
                if (
                    not previous
                    or snapshot.full_price != previous.get("full_price")
                    or snapshot.discounted_price != previous.get("spp_price")
                ):
                    records.append(
                        {
                            "nm_id": article_id,
                            "full_price": snapshot.full_price,
                            "spp_percent": snapshot.spp,
                            "spp_price": snapshot.discounted_price,
                        }
                    )

            if not records:
                logger.info("Для spp_history нет изменений цены или СПП, запись в БД пропущена.")
                return 0

            connection.execute(
                text(
                    """
                    INSERT INTO spp_history (nm_id, full_price, spp_percent, spp_price)
                    VALUES (:nm_id, :full_price, :spp_percent, :spp_price)
                    """
                ),
                records,
            )
        logger.info("Изменения цены и СПП записаны в spp_history: rows=%s", len(records))
        return len(records)

    @staticmethod
    def _as_float(value: object) -> float | None:
        """
        Приводит значение из PostgreSQL к float для сравнения цен.

        Бизнес-логика:
        цены из БД могут приходить как Decimal, а свежие значения WB как float;
        единый тип нужен, чтобы корректно определить изменение цены.
        """
        if value is None:
            return None
        if isinstance(value, Decimal):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
