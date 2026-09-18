"""Репозитории PostgreSQL-справочников и ClickHouse-витрин контроля остатков."""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from uuid import uuid4

from src_oop.core.clickhouse import ClickHouseDatabase
from src_oop.core.database import Database
from src_oop.jobs.wb_stock_control.config import CLICKHOUSE_CURRENT_TABLE, CLICKHOUSE_HISTORY_TABLE
from src_oop.jobs.wb_stock_control.models import StockObservation, StockTarget

logger = logging.getLogger(__name__)


class WBStockControlRepository:
    """Читает справочники и сохраняет снимки контроля в ClickHouse."""

    def __init__(self, clickhouse: ClickHouseDatabase | None = None) -> None:
        """Подключает существующие PostgreSQL- и ClickHouse-слои проекта."""
        self.clickhouse = clickhouse or ClickHouseDatabase()

    def fetch_targets(self, unit_article_ids: set[int]) -> list[StockTarget]:
        """Возвращает все активные товары по активным FBS-складам.

        Бизнес-правило: в контроль попадают все карточки ЛК, а флаг UNIT
        используется только для приоритизации тревоги, не для фильтрации.
        """
        rows = Database.read_sql_to_dict(
            """
            SELECT c.article_id, c.chrt_id, a.account, a.local_vendor_code,
                   w.warehouse_id, w.wb_warehouse_id
            FROM card_data AS c
            JOIN article AS a ON a.nm_id = c.article_id
            JOIN warehouses_fbs AS w ON w.account = a.account
            WHERE c.chrt_id IS NOT NULL
              AND w.status = 'active'
              AND w.wb_warehouse_id IS NOT NULL
            """
        )
        return [
            StockTarget(
                account=str(row["account"]).strip().casefold(),
                warehouse_id=int(row["warehouse_id"]),
                wb_warehouse_id=int(row["wb_warehouse_id"]),
                article_id=int(row["article_id"]),
                wild=str(row.get("local_vendor_code") or row["article_id"]).strip(),
                chrt_id=int(row["chrt_id"]),
                is_in_unit=int(row["article_id"]) in unit_article_ids,
            )
            for row in rows
            if row.get("account") and row.get("article_id") is not None
        ]

    def ensure_tables(self) -> None:
        """Создаёт ClickHouse-таблицы истории и текущего состояния идемпотентно."""
        self.clickhouse.execute(f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_HISTORY_TABLE} (
                check_id UUID, checked_at DateTime64(3, 'UTC'), account String,
                warehouse_id Int32, wb_warehouse_id Int64, article_id Int64,
                chrt_id Int64, amount Nullable(Int32), is_in_unit UInt8,
                check_status LowCardinality(String), error_type Nullable(String),
                loaded_at DateTime64(3, 'UTC')
            ) ENGINE = MergeTree ORDER BY (account, wb_warehouse_id, chrt_id, checked_at)
        """)
        self.clickhouse.execute(f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_CURRENT_TABLE} (
                checked_at DateTime64(3, 'UTC'), account String,
                warehouse_id Int32, wb_warehouse_id Int64, article_id Int64,
                chrt_id Int64, amount Nullable(Int32), is_in_unit UInt8,
                check_status LowCardinality(String), error_type Nullable(String),
                loaded_at DateTime64(3, 'UTC')
            ) ENGINE = ReplacingMergeTree(checked_at)
            ORDER BY (account, wb_warehouse_id, chrt_id)
        """)

    def save_observations(self, observations: list[StockObservation]) -> None:
        """Пакетно сохраняет историю и текущую витрину остатков."""
        if not observations:
            return
        now = datetime.now(UTC).replace(tzinfo=None)
        history = []
        current = []
        for observation in observations:
            target = observation.target
            values = (
                observation.checked_at.replace(tzinfo=None), target.account,
                target.warehouse_id, target.wb_warehouse_id, target.article_id,
                target.chrt_id, observation.amount, int(target.is_in_unit),
                observation.check_status, observation.error_type, now,
            )
            history.append((str(uuid4()), *values))
            current.append(values)
        self.clickhouse.insert(f"INSERT INTO {CLICKHOUSE_HISTORY_TABLE} VALUES", history)
        self.clickhouse.insert(f"INSERT INTO {CLICKHOUSE_CURRENT_TABLE} VALUES", current)
        logger.info("Результаты контроля остатков записаны в ClickHouse | rows=%s", len(observations))

    def fetch_previous_states(self, keys: list[tuple[str, int, int]]) -> dict[tuple[str, int, int], tuple[int | None, str]]:
        """Читает предыдущие состояния для защиты Bitrix от повторных уведомлений."""
        if not keys:
            return {}
        dataframe = self.clickhouse.read_sql_to_dataframe(
            f"""
            SELECT account, wb_warehouse_id, chrt_id, amount, check_status
            FROM {CLICKHOUSE_CURRENT_TABLE} FINAL
            """
        )
        requested_keys = set(keys)
        result: dict[tuple[str, int, int], tuple[int | None, str]] = {}
        for row in dataframe.itertuples():
            key = (str(row.account), int(row.wb_warehouse_id), int(row.chrt_id))
            if key not in requested_keys:
                continue
            amount = row.amount
            normalized_amount = (
                None
                if amount is None or (isinstance(amount, float) and math.isnan(amount))
                else int(amount)
            )
            result[key] = (normalized_amount, str(row.check_status))
        return result
