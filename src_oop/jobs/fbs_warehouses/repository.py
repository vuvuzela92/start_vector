from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, TypeVar

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.exc import DBAPIError, DisconnectionError, OperationalError, SQLAlchemyError

from src_oop.core.database import Database
from src_oop.jobs.fbs_warehouses.config import (
    DB_MAX_RETRIES,
    DB_RETRY_BASE_SLEEP_SECONDS,
    DB_RETRY_MAX_SLEEP_SECONDS,
    KEY_COLUMNS,
    SCHEMA_DEFINITION,
    TABLE_NAME,
)

logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass(slots=True)
class WarehouseSaveResult:
    """Итог записи справочника FBS-складов в PostgreSQL."""

    written_rows: int
    warehouse_id: int


@dataclass(slots=True)
class WarehouseDeleteResult:
    """Итог мягкого удаления FBS-склада в справочнике PostgreSQL."""

    account: str
    wb_warehouse_id: int
    updated_rows: int


@dataclass(slots=True)
class WarehousesSyncResult:
    """Итог дозаполнения справочника FBS-складов данными из WB API."""

    account: str
    api_rows: int = 0
    updated_rows: int = 0
    inserted_rows: int = 0
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

        def _read_next_id() -> int:
            """Читает следующий бизнес-ID склада внутри повторяемой DB-операции."""
            with self.database_cls.get_engine().connect() as connection:
                row = connection.execute(query).mappings().one()
            return int(row["next_id"])

        next_id = self._run_with_database_retry(
            operation_name="расчет следующего warehouse_id",
            callback=_read_next_id,
        )
        logger.info(
            "Рассчитан следующий warehouse_id для нового FBS-склада | warehouse_id=%s",
            next_id,
        )
        return next_id

    def fetch_active_warehouses_by_logical_id(
        self,
        warehouse_id: int,
    ) -> dict[str, dict[str, object]]:
        """Возвращает активные связки склада по нашему `warehouse_id`.

        Бизнес-сценарий: при массовом создании одного логического склада на всех ЛК нельзя повторно
        создавать склад там, где активная связка уже есть в `warehouses_fbs`. Ключ результата
        нормализован через `casefold`, чтобы сравнение аккаунтов не зависело от регистра.
        """
        self._ensure_table_exists()
        query = text(
            f"""
            SELECT account, warehouse_id, warehouse_name, wb_warehouse_id, wb_office_id, status
            FROM {TABLE_NAME}
            WHERE warehouse_id = :warehouse_id
              AND status = 'active'
            """
        )

        def _read_active_warehouses() -> list[dict[str, object]]:
            """Читает активные связки склада для защиты массового создания от дублей."""
            with self.database_cls.get_engine().connect() as connection:
                rows = connection.execute(query, {"warehouse_id": warehouse_id}).mappings().all()
            return [dict(row) for row in rows]

        rows = self._run_with_database_retry(
            operation_name="чтение активных FBS-складов по warehouse_id",
            callback=_read_active_warehouses,
        )

        result = {
            str(row["account"]).strip().casefold(): row
            for row in rows
            if row.get("account")
        }
        logger.info(
            "Активные FBS-склады найдены для защиты от дублей | warehouse_id=%s | rows=%s",
            warehouse_id,
            len(result),
        )
        return result

    def save(self, dataframe: pd.DataFrame, warehouse_id: int) -> WarehouseSaveResult:
        """Записывает строки FBS-складов, чтобы остатки могли находить WB warehouseId по аккаунту."""
        if dataframe.empty:
            logger.warning("Запись warehouses_fbs пропущена: нет строк для сохранения.")
            return WarehouseSaveResult(written_rows=0, warehouse_id=warehouse_id)

        self._ensure_table_exists()
        self._normalize_account_names()
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

    def mark_deleted(self, account: str, wb_warehouse_id: int) -> WarehouseDeleteResult:
        """Помечает удаленный в WB склад как неактивный в `warehouses_fbs`.

        Бизнес-сценарий: после удаления FBS-склада в личном кабинете WB мы сохраняем его строку
        для истории связок, но исключаем из управления остатками через `status = 'deleted'`.
        Это защищает будущие запросы остатков от обращения к складу, которого уже нет в WB.
        """
        self._ensure_table_exists()
        self._normalize_account_names()
        update_sql = text(
            f"""
            UPDATE {TABLE_NAME}
            SET
                status = 'deleted',
                deleted_at = COALESCE(deleted_at, now()),
                updated_at = now()
            WHERE upper(account) = upper(:account)
              AND wb_warehouse_id = :wb_warehouse_id
              AND status <> 'deleted'
            """
        )

        def _mark_deleted() -> int:
            """Обновляет статус удаленного WB-склада в справочнике для исключения из остатков."""
            with self.database_cls.get_engine().begin() as connection:
                result = connection.execute(
                    update_sql,
                    {
                        "account": account,
                        "wb_warehouse_id": wb_warehouse_id,
                    },
                )
            return int(result.rowcount or 0)

        updated_rows = self._run_with_database_retry(
            operation_name="пометка FBS-склада удаленным",
            callback=_mark_deleted,
        )
        logger.info(
            "FBS-склад помечен удаленным в справочнике warehouses_fbs | account=%s | wb_warehouse_id=%s | updated_rows=%s",
            account,
            wb_warehouse_id,
            updated_rows,
        )
        return WarehouseDeleteResult(
            account=account,
            wb_warehouse_id=wb_warehouse_id,
            updated_rows=updated_rows,
        )

    def update_existing_from_wb(
        self,
        account: str,
        warehouses_payload: list[dict[str, object]],
    ) -> WarehousesSyncResult:
        """Синхронизирует действующие WB-склады аккаунта со справочником warehouses_fbs.

        Бизнес-правило: если WB возвращает действующий склад, которого еще нет в БД,
        мы добавляем его автоматически. Наш `warehouse_id` подбирается по названию склада
        среди уже известных ЛК, чтобы один логический склад сохранял общий ID на всех аккаунтах.
        Если название новое для системы, создается следующий свободный `warehouse_id`.
        """
        self._ensure_table_exists()
        self._normalize_account_names()
        update_by_wb_id_sql = text(
            f"""
            UPDATE {TABLE_NAME}
            SET
                warehouse_name = :warehouse_name,
                wb_office_id = :wb_office_id,
                status = 'active',
                create_payload = :create_payload,
                deleted_at = NULL,
                updated_at = now()
            WHERE upper(account) = upper(:account)
              AND wb_warehouse_id = :wb_warehouse_id
            RETURNING warehouse_id
            """
        ).bindparams(
            bindparam("create_payload", type_=SCHEMA_DEFINITION["create_payload"])
        )
        warehouse_id_by_name_sql = text(
            f"""
            SELECT warehouse_id
            FROM {TABLE_NAME}
            WHERE upper(warehouse_name) = upper(:warehouse_name)
            ORDER BY warehouse_id
            LIMIT 1
            """
        )
        next_warehouse_id_sql = text(
            f"SELECT COALESCE(MAX(warehouse_id), 0) + 1 AS next_id FROM {TABLE_NAME}"
        )
        insert_sql = text(
            f"""
            INSERT INTO {TABLE_NAME} (
                warehouse_id,
                warehouse_name,
                account,
                wb_warehouse_id,
                wb_office_id,
                status,
                create_payload
            )
            VALUES (
                :warehouse_id,
                :warehouse_name,
                upper(:account),
                :wb_warehouse_id,
                :wb_office_id,
                'active',
                :create_payload
            )
            ON CONFLICT (warehouse_id, account)
            DO UPDATE SET
                warehouse_name = EXCLUDED.warehouse_name,
                wb_warehouse_id = EXCLUDED.wb_warehouse_id,
                wb_office_id = EXCLUDED.wb_office_id,
                status = 'active',
                create_payload = EXCLUDED.create_payload,
                deleted_at = NULL,
                updated_at = now()
            """
        ).bindparams(
            bindparam("create_payload", type_=SCHEMA_DEFINITION["create_payload"])
        )

        def _update_existing_rows() -> WarehousesSyncResult:
            """Обновляет известные WB-склады и добавляет новые действующие склады в справочник."""
            sync_result = WarehousesSyncResult(
                account=account.upper(),
                api_rows=len(warehouses_payload),
            )
            with self.database_cls.get_engine().begin() as connection:
                for warehouse in warehouses_payload:
                    if not self._is_active_wb_warehouse(warehouse):
                        continue

                    wb_warehouse_id = self._extract_int(
                        warehouse,
                        ("id", "warehouseId", "warehouseID"),
                    )
                    warehouse_name = self._extract_str(warehouse, ("name", "warehouseName"))
                    if wb_warehouse_id is None:
                        sync_result.unmatched_warehouses.append(warehouse)
                        continue
                    if warehouse_name is None:
                        sync_result.unmatched_warehouses.append(warehouse)
                        continue

                    update_result = connection.execute(
                        update_by_wb_id_sql,
                        {
                            "account": account,
                            "wb_warehouse_id": wb_warehouse_id,
                            "warehouse_name": warehouse_name,
                            "wb_office_id": self._extract_int(
                                warehouse,
                                ("officeId", "officeID", "office_id"),
                            ),
                            "create_payload": warehouse,
                        },
                    )
                    if update_result.first() is not None:
                        sync_result.updated_rows += 1
                        continue

                    warehouse_id = self._resolve_logical_warehouse_id(
                        connection=connection,
                        warehouse_name=warehouse_name,
                        warehouse_id_by_name_sql=warehouse_id_by_name_sql,
                        next_warehouse_id_sql=next_warehouse_id_sql,
                    )
                    connection.execute(
                        insert_sql,
                        {
                            "warehouse_id": warehouse_id,
                            "warehouse_name": warehouse_name,
                            "account": account,
                            "wb_warehouse_id": wb_warehouse_id,
                            "wb_office_id": self._extract_int(
                                warehouse,
                                ("officeId", "officeID", "office_id"),
                            ),
                            "create_payload": warehouse,
                        },
                    )
                    sync_result.inserted_rows += 1
            return sync_result

        result = self._run_with_database_retry(
            operation_name="дозаполнение справочника FBS-складов из WB",
            callback=_update_existing_rows,
        )

        logger.info(
            "Справочник FBS-складов синхронизирован из WB API | account=%s | api_rows=%s | updated_rows=%s | inserted_rows=%s | unmatched_rows=%s",
            result.account,
            result.api_rows,
            result.updated_rows,
            result.inserted_rows,
            len(result.unmatched_warehouses),
        )
        return result

    def _is_active_wb_warehouse(self, warehouse: dict[str, object]) -> bool:
        """Проверяет, что WB-склад действующий и его можно включать в управление остатками.

        Бизнес-правило: склады, которые WB уже удаляет или обрабатывает как удаляемые,
        не должны автоматически возвращаться в `warehouses_fbs` как активные.
        """
        return warehouse.get("isDeleting") is not True

    def _resolve_logical_warehouse_id(
        self,
        connection,
        warehouse_name: str,
        warehouse_id_by_name_sql,
        next_warehouse_id_sql,
    ) -> int:
        """Подбирает общий `warehouse_id` для нового склада WB по названию склада.

        Бизнес-сценарий: одинаковые склады на разных ЛК должны иметь один внутренний ID.
        Поэтому сначала ищем существующий `warehouse_id` по названию, а новый ID выдаем
        только для ранее неизвестного названия склада.
        """
        existing_row = connection.execute(
            warehouse_id_by_name_sql,
            {"warehouse_name": warehouse_name},
        ).mappings().first()
        if existing_row is not None:
            return int(existing_row["warehouse_id"])

        next_row = connection.execute(next_warehouse_id_sql).mappings().one()
        return int(next_row["next_id"])

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

        def _create_table() -> None:
            """Проверяет наличие таблицы перед операциями со складами и создает ее при первом запуске."""
            with self.database_cls.get_engine().begin() as connection:
                connection.execute(create_table_sql)

        self._run_with_database_retry(
            operation_name="подготовка таблицы warehouses_fbs",
            callback=_create_table,
        )

    def _normalize_account_names(self) -> None:
        """Приводит уже сохраненные ЛК к uppercase, чтобы справочник складов не создавал дубли.

        Бизнес-сценарий: пользователь может запускать операции как `старт0854`, `Старт0854`
        или `СТАРТ0854`, но для управления остатками это один и тот же кабинет WB.
        """
        normalize_sql = text(
            f"""
            UPDATE {TABLE_NAME}
            SET
                account = upper(account),
                updated_at = now()
            WHERE account <> upper(account)
            """
        )

        def _normalize_rows() -> int:
            """Обновляет старые строки справочника, которые были сохранены не в uppercase."""
            with self.database_cls.get_engine().begin() as connection:
                result = connection.execute(normalize_sql)
            return int(result.rowcount or 0)

        updated_rows = self._run_with_database_retry(
            operation_name="нормализация названий ЛК в warehouses_fbs",
            callback=_normalize_rows,
        )
        if updated_rows:
            logger.info(
                "Названия ЛК в warehouses_fbs приведены к uppercase | updated_rows=%s",
                updated_rows,
            )

    def _run_with_database_retry(
        self,
        operation_name: str,
        callback: Callable[[], T],
    ) -> T:
        """Повторяет безопасную DB-операцию справочника FBS-складов после временного обрыва.

        Бизнес-сценарий: создание или удаление склада не должно идти в WB, пока справочник
        `warehouses_fbs` не проверен или не обновлен. При кратковременном разрыве соединения
        мы сбрасываем пул и повторяем только идемпотентные операции чтения/служебного обновления.
        """
        for attempt in range(1, DB_MAX_RETRIES + 1):
            try:
                return callback()
            except SQLAlchemyError as error:
                transient = self._is_transient_database_error(error)
                if not transient or attempt >= DB_MAX_RETRIES:
                    logger.exception(
                        "Операция PostgreSQL для FBS-складов завершилась ошибкой | operation=%s | attempt=%s/%s | transient=%s",
                        operation_name,
                        attempt,
                        DB_MAX_RETRIES,
                        transient,
                    )
                    raise

                sleep_seconds = min(
                    DB_RETRY_BASE_SLEEP_SECONDS * 2 ** (attempt - 1),
                    DB_RETRY_MAX_SLEEP_SECONDS,
                )
                logger.warning(
                    "Временный обрыв PostgreSQL при работе с FBS-складами, операция будет повторена | operation=%s | attempt=%s/%s | sleep_seconds=%s | error_type=%s",
                    operation_name,
                    attempt,
                    DB_MAX_RETRIES,
                    sleep_seconds,
                    type(error).__name__,
                )
                self.database_cls.get_engine().dispose()
                time.sleep(sleep_seconds)

        raise RuntimeError(
            f"Операция PostgreSQL для FBS-складов не выполнена: operation={operation_name}."
        )

    def _is_transient_database_error(self, error: SQLAlchemyError) -> bool:
        """Отличает временный обрыв PostgreSQL от ошибок данных, SQL и ограничений таблицы."""
        if isinstance(error, (OperationalError, DisconnectionError)):
            return True
        if isinstance(error, DBAPIError) and error.connection_invalidated:
            return True
        sqlstate = getattr(getattr(error, "orig", None), "pgcode", None)
        return isinstance(sqlstate, str) and sqlstate.startswith("08")
