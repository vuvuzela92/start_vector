"""Нормализация ответа WB Order Feed в табличную схему PostgreSQL."""

from __future__ import annotations

import logging

import pandas as pd

from src_oop.jobs.orders_feed.common.mapping import ORDER_FEED_COLUMN_MAP
from src_oop.jobs.orders_feed.config import (
    DATETIME_COLUMNS,
    DB_COLUMNS,
    INTEGER_COLUMNS,
    NUMERIC_COLUMNS,
    SOURCE_COLUMNS,
)
from src_oop.jobs.orders_feed.models import (
    CancelType,
    DataSource,
    OrderFeedPage,
    OrderStatus,
    SaleType,
    WarehouseType,
)

logger = logging.getLogger(__name__)


class OrderFeedNormalizer:
    """Преобразует camelCase-поля WB в типизированные snake_case-колонки витрины."""

    def normalize(self, page: OrderFeedPage) -> pd.DataFrame:
        """Нормализует одну страницу и добавляет кабинет, валюту и источник данных."""
        dataframe = pd.DataFrame(page.orders)
        if dataframe.empty:
            return pd.DataFrame(columns=list(DB_COLUMNS))
        self._ensure_source_columns(dataframe)
        dataframe = dataframe.loc[:, list(SOURCE_COLUMNS)].rename(
            columns=ORDER_FEED_COLUMN_MAP
        )
        dataframe.insert(0, "account", page.account)
        dataframe["currency"] = page.currency
        dataframe["data_source"] = DataSource.ORDER_FEED.value
        dataframe["snapshot_time"] = page.snapshot_time
        dataframe["loaded_at"] = pd.Timestamp.now(tz="UTC")
        self._map_business_enums(dataframe)
        self._coerce_types(dataframe)
        dataframe = dataframe.loc[:, list(DB_COLUMNS)]
        result = dataframe.astype(object).where(pd.notna(dataframe), None)
        logger.info(
            "Нормализована страница Order Feed | account=%s | offset=%s | rows=%s",
            page.account,
            page.offset,
            len(result.index),
        )
        return result

    def _ensure_source_columns(self, dataframe: pd.DataFrame) -> None:
        """Добавляет необязательный cancelType и защищает загрузку при расширении ответа WB."""
        for column in SOURCE_COLUMNS:
            if column not in dataframe.columns:
                dataframe[column] = None

    def _coerce_types(self, dataframe: pd.DataFrame) -> None:
        """Приводит значения API к типам схемы БД без падения на единичном некорректном поле."""
        for column in DATETIME_COLUMNS:
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce", utc=True)
        for column in INTEGER_COLUMNS:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").astype("Int64")
        for column in NUMERIC_COLUMNS:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").round(2)

    def _map_business_enums(self, dataframe: pd.DataFrame) -> None:
        """Преобразует статусы и boolean-флаги WB в самодокументируемые значения PostgreSQL enum."""
        dataframe["status"] = dataframe["status"].map(
            lambda value: OrderStatus(value).value if value is not None else None
        )
        dataframe["cancel_type"] = dataframe["cancel_type"].map(
            lambda value: CancelType(value).value if value is not None else None
        )
        dataframe["warehouse_type"] = dataframe.pop("is_mp").map(
            {True: WarehouseType.SELLER.value, False: WarehouseType.WB.value}
        )
        dataframe["sale_type"] = dataframe.pop("is_b2b").map(
            {True: SaleType.B2B.value, False: SaleType.B2C.value}
        )
