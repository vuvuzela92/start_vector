from __future__ import annotations

import math

import pandas as pd

from src_oop.jobs.autopilot.models import MetricValues, WBCardSnapshot


class AutopilotCalculator:
    @staticmethod
    def dataframe_to_metric_dicts(dataframe: pd.DataFrame) -> dict[str, MetricValues]:
        """
        Преобразует DataFrame с метриками в словари `metric -> article -> value`.

        Бизнес-логика:
        это формат, удобный для порционной записи метрик в ПУ. Пустые значения
        не добавляются в словарь, чтобы writer оставил в таблице пропуск.
        """
        if dataframe.empty:
            return {}

        metric_names = [column for column in dataframe.columns if column != "article_id"]
        metrics: dict[str, MetricValues] = {name: {} for name in metric_names}
        for row in dataframe.to_dict(orient="records"):
            article_id = int(row["article_id"])
            for metric_name in metric_names:
                value = row.get(metric_name)
                if AutopilotCalculator._is_present(value):
                    metrics[metric_name][article_id] = value
        return metrics

    @staticmethod
    def calculate_margin_metrics(
        funnel_metrics: dict[str, MetricValues],
        adv_spend: MetricValues,
        margin_by_article: dict[int, float],
    ) -> dict[str, MetricValues]:
        """
        Считает прибыль, ЧП-РК, ДРР и CPO по legacy-формулам.

        Бизнес-логика:
        прибыль берется как сумма заказов из воронки, умноженная на маржу UNIT.
        ЧП-РК, ДРР и CPO используют расходы Cometa, потому что для автопилота
        согласован именно этот источник рекламных затрат.
        """
        orders_sum = funnel_metrics.get("orders_sum_rub", {})
        orders_count = funnel_metrics.get("orders_count", {})

        profit: MetricValues = {}
        for article_id, order_sum in orders_sum.items():
            if AutopilotCalculator._is_present(order_sum):
                profit[article_id] = float(order_sum) * margin_by_article.get(article_id, 1.0)

        net_profit: MetricValues = {}
        for article_id in set(profit) | set(adv_spend):
            net_profit[article_id] = float(profit.get(article_id, 0) or 0) - float(
                adv_spend.get(article_id, 0) or 0
            )

        adv_part: MetricValues = {}
        for article_id in set(adv_spend) | set(orders_sum):
            numerator = float(adv_spend.get(article_id, 0) or 0)
            denominator = float(orders_sum.get(article_id, numerator) or 0)
            adv_part[article_id] = numerator / denominator if denominator != 0 else 1.0

        cpo: MetricValues = {}
        for article_id in set(adv_spend) | set(orders_count):
            numerator = float(adv_spend.get(article_id, 0) or 0)
            denominator = float(orders_count.get(article_id, numerator) or 0)
            cpo[article_id] = numerator / denominator if denominator != 0 else 1.0

        return {
            "profit_by_cond_orders": profit,
            "net_profit_after_ad": net_profit,
            "advertising_cost_share": adv_part,
            "cpo": cpo,
        }

    @staticmethod
    def calculate_advert_metrics(
        activity_by_article: dict[int, dict[str, float]],
        adv_spend: MetricValues,
    ) -> dict[str, MetricValues]:
        """
        Считает клики, показы, CTR, CPC и CPM для ПУ.

        Бизнес-логика:
        CTR остается долей `clicks / views`, без умножения на 100.
        CPC и CPM считаются как в legacy: от агрегированных кликов/показов
        и расхода Cometa по артикулу.
        """
        clicks: MetricValues = {}
        views: MetricValues = {}
        ctr: MetricValues = {}
        cpc: MetricValues = {}
        cpm: MetricValues = {}

        for article_id, activity in activity_by_article.items():
            article_clicks = float(activity.get("clicks") or 0)
            article_views = float(activity.get("views") or 0)
            spend = float(adv_spend.get(article_id, 0) or 0)

            clicks[article_id] = article_clicks
            views[article_id] = article_views
            ctr[article_id] = round(article_clicks / article_views, 2) if article_views > 0 else 0
            cpc[article_id] = round(spend / article_clicks, 2) if article_clicks > 0 else 0
            cpm[article_id] = round((spend / article_views) * 1000, 2) if article_views > 0 else 0

        return {
            "clicks": clicks,
            "views": views,
            "ctr": ctr,
            "cpc": cpc,
            "cpm": cpm,
        }

    @staticmethod
    def calculate_organic(
        funnel_metrics: dict[str, MetricValues],
        advert_metrics: dict[str, MetricValues],
    ) -> MetricValues:
        """
        Считает органические переходы в карточку.

        Бизнес-логика:
        органика равна `open_card_count - clicks`, но не может быть ниже нуля.
        Это сохраняет legacy-поведение для метрики `Органика` в ПУ.
        """
        open_cards = funnel_metrics.get("open_card_count", {})
        clicks = advert_metrics.get("clicks", {})
        result: MetricValues = {}
        for article_id, open_count in open_cards.items():
            organic = float(open_count or 0) - float(clicks.get(article_id, 0) or 0)
            result[article_id] = max(0, organic)
        return result

    @staticmethod
    def card_snapshots_to_metrics(
        snapshots: dict[int, WBCardSnapshot],
    ) -> dict[str, MetricValues]:
        """
        Раскладывает снимки карточек WB на отдельные метрики ПУ.

        Бизнес-логика:
        из одного запроса карточки получаются несколько колонок: акция, рейтинг,
        полная цена, процент СПП и наша цена с СПП.
        """
        result: dict[str, MetricValues] = {
            "promo_status": {},
            "rating": {},
            "full_price": {},
            "spp": {},
            "discounted_price": {},
        }
        for article_id, snapshot in snapshots.items():
            for metric_name in result:
                value = getattr(snapshot, metric_name)
                if AutopilotCalculator._is_present(value):
                    result[metric_name][article_id] = value
        return result

    @staticmethod
    def _is_present(value: object) -> bool:
        """
        Проверяет, можно ли записывать значение как бизнес-метрику.

        Бизнес-логика:
        `None` и `NaN` считаются отсутствием данных и превращаются в пропуск,
        а не в ноль, чтобы не исказить показатели ПУ.
        """
        if value is None:
            return False
        if isinstance(value, float) and math.isnan(value):
            return False
        return True
