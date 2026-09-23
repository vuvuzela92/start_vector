"""Быстрая и восстанавливаемая запись статусной модели сборочных заданий."""

from __future__ import annotations

import csv
from datetime import date, datetime
from io import StringIO
import logging
import time
from typing import Any

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    TIMESTAMP,
    UniqueConstraint,
    text,
)
from sqlalchemy.exc import DBAPIError, OperationalError

from src_oop.core.database import Database
from src_oop.jobs.assembly_info.config import AssemblyInfoSettings

logger = logging.getLogger(__name__)


class AssemblyInfoRepository:
    """Сохраняет статусную модель через COPY и порционные идемпотентные upsert."""

    TABLE_NAME = "assembly_task_status_model"
    UNIQUE_KEYS = ("id", "supplier_status", "wb_status")
    COLUMNS = (
        "date", "nm_id", "local_vendor_code", "vendor_code", "id", "supplier_status", "wb_status",
        "supply_id", "address", "scan_price", "price", "converted_price", "comment", "delivery_type",
        "order_uid", "color_code", "rid", "created_at", "created_at_msk", "offices", "skus", "warehouse_id",
        "chrt_id", "currency_code", "converted_currency_code", "cargo_type", "is_zero_order", "options", "office_id", "account",
    )
    SCHEMA = {
        "date": Date, "nm_id": BigInteger, "local_vendor_code": Text, "vendor_code": Text,
        "id": BigInteger, "supplier_status": Text, "wb_status": Text, "supply_id": Text,
        "address": Text, "scan_price": Numeric(10, 3), "price": Numeric(10, 3),
        "converted_price": Numeric(10, 3), "comment": Text, "delivery_type": Text,
        "order_uid": Text, "color_code": Text, "rid": Text, "created_at": TIMESTAMP,
        "created_at_msk": TIMESTAMP, "offices": Text, "skus": Text, "warehouse_id": Integer,
        "chrt_id": BigInteger, "currency_code": Integer, "converted_currency_code": Integer,
        "cargo_type": Integer, "is_zero_order": Boolean, "options": Text, "office_id": Integer,
        "account": String(50),
    }

    def __init__(self, settings: AssemblyInfoSettings | None = None) -> None:
        """Создает репозиторий с размером порции и числом повторов записи из настроек."""
        self.settings = settings or AssemblyInfoSettings.from_env()

    def save(self, dataframe: pd.DataFrame) -> None:
        """Загружает весь снимок через COPY и выполняет один условный merge.

        Бизнес-правило: снимок статусов фиксируется атомарно, а повторный запуск
        обновляет только строки, в которых действительно изменилось состояние заказа.
        """
        if dataframe.empty:
            logger.info("Сохранение статусной модели пропущено: нет строк для записи.")
            return

        table = self._ensure_table()
        rows = dataframe.loc[:, self.COLUMNS].to_dict(orient="records")
        total = len(rows)
        started_at = time.perf_counter()
        logger.info(
            "Начата запись статусной модели через staging | table=%s | rows=%s | method=COPY",
            self.TABLE_NAME, total,
        )
        csv_data = self._build_csv(rows)
        self._save_all_with_retry(table, csv_data, total)
        elapsed = time.perf_counter() - started_at
        logger.info(
            "Запись статусной модели завершена | table=%s | rows=%s | seconds=%.2f",
            self.TABLE_NAME, total, elapsed,
        )

    def fetch_terminal_order_ids(
        self,
        statuses: tuple[str, ...],
        order_ids_by_account: dict[str, list[int]],
    ) -> dict[str, set[int]]:
        """Возвращает задания с уже зафиксированным финальным статусом WB.

        Бизнес-правило: задания в финальных статусах не запрашиваются повторно
        на каждом пятиминутном запуске, но их история сохраняется в витрине.
        Запрос выбирает только явные поля и только среди заказов текущего
        запуска, поэтому не сканирует всю накопленную историю модели.
        """
        if not statuses or not order_ids_by_account:
            return {}
        query = text(
            f"""
            SELECT DISTINCT id, account
            FROM {self.TABLE_NAME}
            WHERE wb_status = ANY(:statuses)
              AND account = :account
              AND id = ANY(:order_ids)
              AND account IS NOT NULL
            """
        )
        try:
            engine = Database.get_engine()
            with engine.begin() as connection:
                connection.execute(text(
                    f"""
                    CREATE INDEX IF NOT EXISTS ix_{self.TABLE_NAME}_terminal_account_id
                    ON {self.TABLE_NAME} (wb_status, account, id)
                    """
                ))
                rows: list[dict[str, object]] = []
                for account, order_ids in order_ids_by_account.items():
                    if not order_ids:
                        continue
                    rows.extend(
                        connection.execute(
                            query,
                            {
                                "statuses": list(statuses),
                                "account": account,
                                "order_ids": order_ids,
                            },
                        ).mappings().all()
                    )
        except Exception as error:
            logger.warning(
                "Не удалось прочитать завершенные сборочные задания; статусы будут проверены полностью | error_type=%s",
                type(error).__name__,
            )
            return {}
        result: dict[str, set[int]] = {}
        for row in rows:
            account = str(row["account"])
            result.setdefault(account, set()).add(int(row["id"]))
        logger.info(
            "Завершенные сборочные задания текущего запуска загружены для инкрементальной проверки | accounts=%s | orders=%s",
            len(result), sum(len(order_ids) for order_ids in result.values()),
        )
        return result

    def _ensure_table(self) -> Table:
        """Создает целевую таблицу и уникальное ограничение до массовой загрузки."""
        metadata = MetaData()
        table = Table(
            self.TABLE_NAME,
            metadata,
            *(Column(name, column_type) for name, column_type in self.SCHEMA.items()),
            UniqueConstraint(*self.UNIQUE_KEYS, name=f"uq_{self.TABLE_NAME}_keys"),
        )
        metadata.create_all(Database.get_engine())
        return table

    def _save_all_with_retry(self, table: Table, csv_data: str, total: int) -> None:
        """Повторяет атомарную загрузку снимка после временного обрыва PostgreSQL."""
        for attempt in range(1, self.settings.db_max_retries + 1):
            try:
                self._save_all(table, csv_data, total)
                return
            except (OperationalError, DBAPIError) as error:
                if attempt >= self.settings.db_max_retries:
                    logger.error(
                        "Атомарная запись статусной модели завершилась ошибкой | table=%s | attempt=%s/%s | error_type=%s",
                        self.TABLE_NAME, attempt, self.settings.db_max_retries, type(error).__name__,
                    )
                    raise
                delay = min(2 ** (attempt - 1), 10)
                logger.warning(
                    "Временная ошибка PostgreSQL, staging будет повторен | table=%s | attempt=%s/%s | delay=%s | error_type=%s",
                    self.TABLE_NAME, attempt, self.settings.db_max_retries, delay, type(error).__name__,
                )
                Database.get_engine().dispose()
                time.sleep(delay)

    def _save_all(self, table: Table, csv_data: str, total: int) -> None:
        """Загружает полный снимок во временную таблицу и выполняет условный merge."""
        stage_name = "assembly_task_status_model_stage"
        insert_columns = ", ".join(self.COLUMNS)
        update_columns = ", ".join(
            f"{column} = EXCLUDED.{column}" for column in self.COLUMNS if column not in self.UNIQUE_KEYS
        )
        changed_condition = " OR ".join(
            f"target.{column} IS DISTINCT FROM EXCLUDED.{column}"
            for column in self.COLUMNS if column not in self.UNIQUE_KEYS
        )
        merge_sql = f"""
            INSERT INTO {self.TABLE_NAME} AS target ({insert_columns})
            SELECT {insert_columns} FROM {stage_name}
            ON CONFLICT ({', '.join(self.UNIQUE_KEYS)})
            DO UPDATE SET {update_columns}
            WHERE {changed_condition}
        """
        copy_sql = (
            f"COPY {stage_name} ({insert_columns}) FROM STDIN "
            "WITH (FORMAT CSV, NULL '\\N')"
        )

        csv_buffer = StringIO(csv_data)
        csv_buffer.seek(0)
        engine = Database.get_engine()
        with engine.begin() as connection:
            connection.execute(
                text("SELECT set_config('lock_timeout', :lock_timeout, true)"),
                {"lock_timeout": f"{self.settings.db_lock_timeout_seconds}s"},
            )
            connection.execute(
                text("SELECT set_config('statement_timeout', :statement_timeout, true)"),
                {"statement_timeout": f"{self.settings.db_statement_timeout_seconds}s"},
            )
            connection.execute(text(
                f"CREATE TEMP TABLE {stage_name} "
                f"(LIKE {self.TABLE_NAME} INCLUDING DEFAULTS) ON COMMIT DROP"
            ))
            raw_connection = connection.connection.driver_connection
            with raw_connection.cursor() as cursor:
                cursor.copy_expert(copy_sql, csv_buffer)
            logger.info(
                "COPY статусной модели завершен | table=%s | rows=%s",
                self.TABLE_NAME, total,
            )
            connection.execute(text(merge_sql))
            logger.info(
                "Merge статусной модели завершен | table=%s | rows=%s",
                self.TABLE_NAME, total,
            )

    def _build_csv(self, rows: list[dict[str, Any]]) -> str:
        """Сериализует снимок в CSV для передачи PostgreSQL через COPY."""
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer, lineterminator="\n")
        for row in rows:
            writer.writerow([self._serialize_value(row.get(column)) for column in self.COLUMNS])
        return csv_buffer.getvalue()

    @staticmethod
    def _serialize_value(value: Any) -> str:
        """Преобразует значение DataFrame в безопасный текстовый формат COPY."""
        if value is None or value is pd.NA:
            return "\\N"
        if isinstance(value, (pd.Timestamp, datetime, date)):
            return value.isoformat()
        if isinstance(value, bool):
            return "true" if value else "false"
        if pd.isna(value):
            return "\\N"
        return str(value)
