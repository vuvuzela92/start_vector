"""Репозиторий PostgreSQL для контроля остатков WB."""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.jobs.wb_stock_control.models import StockObservation, StockTarget

logger = logging.getLogger(__name__)

HISTORY_TABLE = "wb_fbs_stock_checks"
CURRENT_TABLE = "wb_fbs_stock_current"


class WBStockControlRepository:
    """Читает товары и сохраняет историю контроля остатков в PostgreSQL."""

    def __init__(self, clickhouse=None, database_cls: type[Database] = Database) -> None:
        """Подключает общий PostgreSQL-слой проекта.

        Параметр `clickhouse` оставлен для обратной совместимости со старыми
        тестами и внешними вызовами, но больше не используется.
        """
        self.database_cls = database_cls

    def fetch_targets(self, unit_article_ids: set[int]) -> list[StockTarget]:
        """Возвращает все активные товары по активным FBS-складам.

        Бизнес-правило: в контроль попадают все карточки ЛК, а принадлежность
        к UNIT используется только для решения об обнулении и уведомлении.
        """
        rows = self.database_cls.read_sql_to_dict(
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
        """Создаёт PostgreSQL-таблицы истории и текущего состояния идемпотентно."""
        statements = (
            f"""
            CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
                check_id UUID PRIMARY KEY, checked_at TIMESTAMPTZ NOT NULL,
                account TEXT NOT NULL, warehouse_id BIGINT NOT NULL,
                wb_warehouse_id BIGINT NOT NULL, article_id BIGINT NOT NULL,
                chrt_id BIGINT NOT NULL, amount INTEGER, is_in_unit BOOLEAN NOT NULL,
                check_status TEXT NOT NULL, error_type TEXT,
                loaded_at TIMESTAMPTZ NOT NULL
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {CURRENT_TABLE} (
                checked_at TIMESTAMPTZ NOT NULL, account TEXT NOT NULL,
                warehouse_id BIGINT NOT NULL, wb_warehouse_id BIGINT NOT NULL,
                article_id BIGINT NOT NULL, chrt_id BIGINT NOT NULL,
                amount INTEGER, is_in_unit BOOLEAN NOT NULL,
                check_status TEXT NOT NULL, error_type TEXT,
                loaded_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (account, wb_warehouse_id, chrt_id)
            )
            """,
            f"CREATE INDEX IF NOT EXISTS ix_{HISTORY_TABLE}_checked_at "
            f"ON {HISTORY_TABLE} (checked_at)",
            f"CREATE INDEX IF NOT EXISTS ix_{HISTORY_TABLE}_business_key "
            f"ON {HISTORY_TABLE} (account, wb_warehouse_id, chrt_id, checked_at)",
        )
        with self.database_cls.get_engine().begin() as connection:
            for statement in statements:
                connection.execute(text(statement))
        logger.info("Таблицы контроля остатков подготовлены в PostgreSQL")

    def save_observations(self, observations: list[StockObservation]) -> None:
        """Пакетно сохраняет историю и актуальное состояние остатков в PostgreSQL."""
        if not observations:
            return
        loaded_at = datetime.now(UTC)
        history_rows = []
        current_rows = []
        for observation in observations:
            target = observation.target
            row = {
                "checked_at": observation.checked_at,
                "account": target.account,
                "warehouse_id": target.warehouse_id,
                "wb_warehouse_id": target.wb_warehouse_id,
                "article_id": target.article_id,
                "chrt_id": target.chrt_id,
                "amount": observation.amount,
                "is_in_unit": target.is_in_unit,
                "check_status": observation.check_status,
                "error_type": observation.error_type,
                "loaded_at": loaded_at,
            }
            history_rows.append({"check_id": uuid4(), **row})
            current_rows.append(row)
        history_sql = text(
            f"""INSERT INTO {HISTORY_TABLE} (
                check_id, checked_at, account, warehouse_id, wb_warehouse_id,
                article_id, chrt_id, amount, is_in_unit, check_status,
                error_type, loaded_at
            ) VALUES (
                :check_id, :checked_at, :account, :warehouse_id, :wb_warehouse_id,
                :article_id, :chrt_id, :amount, :is_in_unit, :check_status,
                :error_type, :loaded_at
            )"""
        )
        current_sql = text(
            f"""INSERT INTO {CURRENT_TABLE} (
                checked_at, account, warehouse_id, wb_warehouse_id, article_id,
                chrt_id, amount, is_in_unit, check_status, error_type, loaded_at
            ) VALUES (
                :checked_at, :account, :warehouse_id, :wb_warehouse_id, :article_id,
                :chrt_id, :amount, :is_in_unit, :check_status, :error_type, :loaded_at
            ) ON CONFLICT (account, wb_warehouse_id, chrt_id) DO UPDATE SET
                checked_at = EXCLUDED.checked_at, warehouse_id = EXCLUDED.warehouse_id,
                article_id = EXCLUDED.article_id, amount = EXCLUDED.amount,
                is_in_unit = EXCLUDED.is_in_unit, check_status = EXCLUDED.check_status,
                error_type = EXCLUDED.error_type, loaded_at = EXCLUDED.loaded_at
            WHERE {CURRENT_TABLE}.checked_at <= EXCLUDED.checked_at"""
        )
        with self.database_cls.get_engine().begin() as connection:
            connection.execute(history_sql, history_rows)
            connection.execute(current_sql, current_rows)
        logger.info(
            "Результаты контроля остатков записаны в PostgreSQL | rows=%s",
            len(observations),
        )

    def fetch_previous_states(
        self, keys: list[tuple[str, int, int]]
    ) -> dict[tuple[str, int, int], tuple[int | None, str]]:
        """Читает предыдущие состояния для защиты Bitrix от повторных уведомлений."""
        if not keys:
            return {}
        rows = self.database_cls.read_sql_to_dict(
            f"SELECT account, wb_warehouse_id, chrt_id, amount, check_status "
            f"FROM {CURRENT_TABLE}"
        )
        requested_keys = set(keys)
        result: dict[tuple[str, int, int], tuple[int | None, str]] = {}
        for row in rows:
            key = (str(row["account"]), int(row["wb_warehouse_id"]), int(row["chrt_id"]))
            if key not in requested_keys:
                continue
            amount = row.get("amount")
            normalized_amount = (
                None
                if amount is None or (isinstance(amount, float) and math.isnan(amount))
                else int(amount)
            )
            result[key] = (normalized_amount, str(row["check_status"]))
        return result
