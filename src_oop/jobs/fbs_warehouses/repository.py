from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.jobs.fbs_warehouses.config import (
    KEY_COLUMNS,
    SCHEMA_DEFINITION,
    TABLE_NAME,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WarehouseSaveResult:
    """Итог записи справочника FBS-складов в PostgreSQL."""

    written_rows: int
    warehouse_id: int


@dataclass(slots=True)
class WarehousesSyncResult:
    """Итог дозаполнения справочника FBS-складов данными из WB API."""

    account: str
    api_rows: int = 0
    updated_rows: int = 0
    unmatched_warehouses: list[dict[str, object]] = field(default_factory=list)


class FBSWarehousesRepository:
    """Сохраняет соответствие наших FBS-складов и WB-складов по аккаунтам."""

    def __init__(self, database_cls: type[Database] = Database) -> None:
        """Подключает слой БД для справочника складов без глобального состояния."""
        self.database_cls = database_cls

    def get_next_warehouse_id(self) -> int:
        """Возвращает следующий наш `warehouse_id`, чтобы один склад имел общий ID во всех аккаунтах."""
        self._ensure_table_exists()
        query = text(f"SELECT COALESCE(MAX(warehouse_id), 0) + 1 AS next_id FROM {TABLE_NAME}")
        with self.database_cls.get_engine().connect() as connection:
            row = connection.execute(query).mappings().one()
        next_id = int(row["next_id"])
        logger.info(
            "Рассчитан следующий warehouse_id для нового FBS-склада | warehouse_id=%s",
            next_id,
        )
        return next_id

    def save(self, dataframe: pd.DataFrame, warehouse_id: int) -> WarehouseSaveResult:
        """Записывает строки FBS-складов, чтобы остатки могли находить WB warehouseId по аккаунту."""
        if dataframe.empty:
            logger.warning("Запись warehouses_fbs пропущена: нет строк для сохранения.")
            return WarehouseSaveResult(written_rows=0, warehouse_id=warehouse_id)

        self._ensure_table_exists()
        self.database_cls.sync_data_to_postgres(
            table_name=TABLE_NAME,
            data=dataframe,
            schema_definition=SCHEMA_DEFINITION,
            unique_keys=KEY_COLUMNS,
        )
        logger.info(
            "Справочник FBS-складов сохранен | table=%s | warehouse_id=%s | rows=%s",
            TABLE_NAME,
            warehouse_id,
            len(dataframe.index),
        )
        return WarehouseSaveResult(
            written_rows=len(dataframe.index),
            warehouse_id=warehouse_id,
        )

    def update_existing_from_wb(
        self,
        account: str,
        warehouses_payload: list[dict[str, object]],
    ) -> WarehousesSyncResult:
        """Дозаполняет существующие строки warehouses_fbs данными из списка складов WB.

        Бизнес-правило: WB-склады без нашего `warehouse_id` не создаются
        автоматически, потому что для управления остатками нужен осознанный
        общий идентификатор склада. Такие склады возвращаются как unmatched.
        """
        self._ensure_table_exists()
        result = WarehousesSyncResult(
            account=account,
            api_rows=len(warehouses_payload),
        )
        update_sql = text(
            f"""
            UPDATE {TABLE_NAME}
            SET
                warehouse_name = COALESCE(NULLIF(warehouse_name, ''), :warehouse_name),
                wb_office_id = COALESCE(wb_office_id, :wb_office_id),
                status = COALESCE(NULLIF(status, ''), 'active'),
                updated_at = now()
            WHERE account = :account
              AND wb_warehouse_id = :wb_warehouse_id
            RETURNING warehouse_id
            """
        )

        with self.database_cls.get_engine().begin() as connection:
            for warehouse in warehouses_payload:
                wb_warehouse_id = self._extract_int(
                    warehouse,
                    ("id", "warehouseId", "warehouseID"),
                )
                if wb_warehouse_id is None:
                    result.unmatched_warehouses.append(warehouse)
                    continue

                update_result = connection.execute(
                    update_sql,
                    {
                        "account": account,
                        "wb_warehouse_id": wb_warehouse_id,
                        "warehouse_name": self._extract_str(warehouse, ("name", "warehouseName")),
                        "wb_office_id": self._extract_int(
                            warehouse,
                            ("officeId", "officeID", "office_id"),
                        ),
                    },
                )
                if update_result.first() is None:
                    result.unmatched_warehouses.append(warehouse)
                else:
                    result.updated_rows += 1

        logger.info(
            "Справочник FBS-складов дозаполнен из WB API | account=%s | api_rows=%s | updated_rows=%s | unmatched_rows=%s",
            result.account,
            result.api_rows,
            result.updated_rows,
            len(result.unmatched_warehouses),
        )
        return result

    def _extract_int(
        self,
        payload: dict[str, object],
        field_names: tuple[str, ...],
    ) -> int | None:
        """Достает числовое поле WB по нескольким возможным именам, защищая синхронизацию от смены casing."""
        for field_name in field_names:
            value = payload.get(field_name)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.strip().isdigit():
                return int(value)
        return None

    def _extract_str(
        self,
        payload: dict[str, object],
        field_names: tuple[str, ...],
    ) -> str | None:
        """Достает текстовое поле WB по нескольким возможным именам для дозаполнения справочника."""
        for field_name in field_names:
            value = payload.get(field_name)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _ensure_table_exists(self) -> None:
        """Создает таблицу FBS-складов с ограничениями, защищающими маппинг от дублей."""
        create_table_sql = text(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id BIGSERIAL PRIMARY KEY,
                warehouse_id BIGINT NOT NULL,
                warehouse_name VARCHAR(255) NOT NULL,
                account VARCHAR(255) NOT NULL,
                wb_warehouse_id BIGINT NOT NULL,
                wb_office_id BIGINT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'active',
                create_payload JSONB NULL,
                created_at TIMESTAMP NOT NULL DEFAULT now(),
                updated_at TIMESTAMP NOT NULL DEFAULT now(),
                deleted_at TIMESTAMP NULL,
                CONSTRAINT uq_{TABLE_NAME}_warehouse_account UNIQUE (warehouse_id, account),
                CONSTRAINT uq_{TABLE_NAME}_account_wb_warehouse UNIQUE (account, wb_warehouse_id)
            )
            """
        )
        with self.database_cls.get_engine().begin() as connection:
            connection.execute(create_table_sql)
