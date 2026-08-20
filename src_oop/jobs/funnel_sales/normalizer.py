from __future__ import annotations

import logging

import pandas as pd

from src_oop.jobs.funnel_sales.config import DATE_COLUMNS, DB_COLUMNS, INTEGER_COLUMNS, NUMERIC_COLUMNS

logger = logging.getLogger(__name__)


class FunnelSalesNormalizer:
    """Нормализует сырой ответ WB в структуру таблицы funnel_daily."""

    def normalize(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Готовит ежедневную воронку продаж к записи в PostgreSQL.

        Бизнес-логика: один товар превращается в одну строку витрины, а дата
        берётся из `selected.period.end`, потому что именно по завершённому дню
        строится historical daily funnel.
        """
        if dataframe.empty:
            return pd.DataFrame(columns=list(DB_COLUMNS))

        rows = [self._normalize_product_row(product) for product in dataframe.to_dict(orient="records")]
        prepared_df = pd.DataFrame(rows)

        self._coerce_typed_columns(prepared_df)
        prepared_df["month"] = pd.to_datetime(prepared_df["date"], errors="coerce").dt.strftime("%m-%Y")
        prepared_df["wild"] = prepared_df["vendor_code"].astype("string").str.extract(r"(wild\d+)", expand=False)
        prepared_df = prepared_df.loc[:, list(DB_COLUMNS)]
        prepared_df = prepared_df.astype(object).where(pd.notna(prepared_df), None)

        logger.info(
            "Нормализована ежедневная воронка продаж WB | rows=%s | columns=%s",
            len(prepared_df.index),
            list(prepared_df.columns),
        )
        return prepared_df

    def _normalize_product_row(self, product: dict) -> dict[str, object]:
        """Раскладывает один товар WB в плоскую строку для daily funnel.

        Бизнес-логика: в витрину попадают только поля, которые использовались в
        legacy-выгрузке для анализа продаж, отмен, выкупа, остатков и карточки.
        """
        product_info = product.get("product", {}) if isinstance(product, dict) else {}
        statistic = product.get("statistic", {}) if isinstance(product, dict) else {}
        selected = statistic.get("selected", {}) if isinstance(statistic, dict) else {}
        time_to_ready = selected.get("timeToReady", {}) if isinstance(selected, dict) else {}

        return {
            "account": product.get("account"),
            "nm_id": product_info.get("nmId"),
            "vendor_code": product_info.get("vendorCode"),
            "title": product_info.get("title"),
            "subject_id": product_info.get("subjectId"),
            "subject_name": product_info.get("subjectName"),
            "brand_name": product_info.get("brandName"),
            "product_rating": product_info.get("productRating"),
            "feedback_rating": product_info.get("feedbackRating"),
            "stocks_wb": product_info.get("stocks", {}).get("wb"),
            "stocks_mp": product_info.get("stocks", {}).get("mp"),
            "balance_sum": product_info.get("stocks", {}).get("balanceSum"),
            "open_count": selected.get("openCount"),
            "cart_count": selected.get("cartCount"),
            "order_count": selected.get("orderCount"),
            "orders_sum": selected.get("orderSum"),
            "buyout_count": selected.get("buyoutCount"),
            "buyout_sum": selected.get("buyoutSum"),
            "cancel_count": selected.get("cancelCount"),
            "cancel_sum": selected.get("cancelSum"),
            "avg_price": selected.get("avgPrice"),
            "avg_orders_count_per_day": selected.get("avgOrdersCountPerDay"),
            "share_order_percent": selected.get("shareOrderPercent"),
            "add_to_wish_list": selected.get("addToWishlist"),
            "time_to_ready": (
                self._coerce_time_part(time_to_ready.get("days")) * 24 * 60
                + self._coerce_time_part(time_to_ready.get("hours")) * 60
                + self._coerce_time_part(time_to_ready.get("mins"))
            ),
            "localization_percent": selected.get("localizationPercent"),
            "date": selected.get("period", {}).get("end"),
        }

    def _coerce_time_part(self, value: object) -> int:
        """Безопасно приводит часть `timeToReady` к целому числу минутной формулы.

        Бизнес-логика: пропуски в блоке `timeToReady` не должны ронять daily
        выгрузку, потому что отсутствие этого поля у части товаров допустимо для
        аналитической витрины и должно давать вклад `0`.
        """
        try:
            return int(value) if value is not None else 0
        except (TypeError, ValueError):
            return 0

    def _coerce_typed_columns(self, dataframe: pd.DataFrame) -> None:
        """Приводит типы daily funnel к формату, ожидаемому схемой PostgreSQL."""
        for column in DATE_COLUMNS:
            date_series = pd.to_datetime(dataframe[column], errors="coerce")
            dataframe[column] = date_series.dt.date

        for column in INTEGER_COLUMNS:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").astype("Int64")

        for column in NUMERIC_COLUMNS:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").round(2)
