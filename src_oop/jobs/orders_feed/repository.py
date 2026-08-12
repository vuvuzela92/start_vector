"""Сохранение страниц WB Order Feed в PostgreSQL."""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    OperationalError,
    SQLAlchemyError,
)
from src_oop.core.database import Database
from src_oop.jobs.orders_feed.config import (
    DB_WRITE_CHUNK_SIZE,
    DB_WRITE_MAX_RETRIES,
    DB_WRITE_RETRY_BASE_SECONDS,
    DB_WRITE_RETRY_MAX_SECONDS,
    KEY_COLUMNS,
    TABLE_NAME,
    UPSERT_UPDATE_COLUMNS,
)
from src_oop.jobs.orders_feed.exceptions import OrderFeedRepositoryError
from src_oop.jobs.orders_feed.models import (
    OrderFeedBase,
    OrderFeedSaveResult,
    WBOrderFeedRecord,
)
from src_oop.jobs.orders_feed.schemas.database import OrderFeedDatabaseRow

logger = logging.getLogger(__name__)


class OrderFeedRepository:
    """Создаёт витрину при первом запуске и выполняет идемпотентный upsert страниц."""

    def __init__(
        self,
        chunk_size: int = DB_WRITE_CHUNK_SIZE,
        max_retries: int = DB_WRITE_MAX_RETRIES,
    ) -> None:
        """Настраивает размер транзакции и число повторов временных ошибок PostgreSQL."""
        if chunk_size <= 0:
            raise ValueError("Размер DB-батча Order Feed должен быть больше нуля.")
        if max_retries <= 0:
            raise ValueError("Число попыток записи Order Feed должно быть больше нуля.")
        self.chunk_size = chunk_size
        self.max_retries = max_retries

    def save(
        self,
        rows: Sequence[OrderFeedDatabaseRow],
        *,
        account: str,
        offset: int,
    ) -> OrderFeedSaveResult:
        """Сохраняет страницу сразу после получения, чтобы сбой не потерял предыдущие страницы."""
        input_rows = len(rows)
        deduplicated, collapsed = self._deduplicate_by_keys(rows)
        if not deduplicated:
            return OrderFeedSaveResult(input_rows, 0, 0, collapsed)
        written_rows = 0
        for chunk_start in range(0, len(deduplicated), self.chunk_size):
            chunk = deduplicated[chunk_start : chunk_start + self.chunk_size]
            self._upsert_with_retry(chunk, account=account, offset=offset)
            written_rows += len(chunk)
        logger.info(
            "Страница Order Feed сохранена через upsert | table=%s | rows=%s",
            TABLE_NAME,
            written_rows,
        )
        return OrderFeedSaveResult(input_rows, written_rows, 0, collapsed)

    def create_table(self) -> None:
        """Создаёт таблицу, PostgreSQL enum-типы и аналитические индексы из ORM-модели."""
        try:
            OrderFeedBase.metadata.create_all(
                Database.get_engine(),
                tables=[WBOrderFeedRecord.__table__],
                checkfirst=True,
            )
        except SQLAlchemyError as error:
            logger.exception(
                "Не удалось подготовить таблицу Order Feed | table=%s", TABLE_NAME
            )
            raise OrderFeedRepositoryError(
                f"Не удалось создать или проверить таблицу {TABLE_NAME}."
            ) from error
        logger.info(
            "Таблица Order Feed и связанные enum-типы готовы | table=%s",
            TABLE_NAME,
        )

    def _upsert_with_retry(
        self,
        rows: Sequence[OrderFeedDatabaseRow],
        *,
        account: str,
        offset: int,
    ) -> None:
        """Повторяет один идемпотентный DB-батч только после временного сбоя соединения."""
        for attempt in range(1, self.max_retries + 1):
            logger.info(
                "Запись DB-батча Order Feed | account=%s | offset=%s | "
                "db_batch_size=%s | attempt=%s/%s",
                account,
                offset,
                len(rows),
                attempt,
                self.max_retries,
            )
            try:
                self._upsert_chunk(rows)
                return
            except SQLAlchemyError as error:
                is_transient = self._is_transient_database_error(error)
                if not is_transient or attempt >= self.max_retries:
                    logger.exception(
                        "DB-батч Order Feed не сохранён | account=%s | offset=%s | "
                        "db_batch_size=%s | attempt=%s/%s | transient=%s",
                        account,
                        offset,
                        len(rows),
                        attempt,
                        self.max_retries,
                        is_transient,
                    )
                    raise OrderFeedRepositoryError(
                        "Не удалось выполнить upsert Order Feed: "
                        f"account={account} offset={offset} rows={len(rows)} "
                        f"attempt={attempt}/{self.max_retries}."
                    ) from error
                delay = min(
                    DB_WRITE_RETRY_BASE_SECONDS * 2 ** (attempt - 1),
                    DB_WRITE_RETRY_MAX_SECONDS,
                )
                logger.warning(
                    "Временная ошибка PostgreSQL, DB-батч Order Feed будет повторён "
                    "| account=%s | offset=%s | db_batch_size=%s | attempt=%s/%s "
                    "| sleep_seconds=%s | error=%s",
                    account,
                    offset,
                    len(rows),
                    attempt,
                    self.max_retries,
                    delay,
                    error,
                )
                Database.get_engine().dispose()
                time.sleep(delay)

    def _upsert_chunk(self, rows: Sequence[OrderFeedDatabaseRow]) -> None:
        """Обновляет один короткий транзакционный чанк по ключу `(account, srid)`."""
        records = [row.model_dump(mode="python") for row in rows]
        statement = insert(WBOrderFeedRecord).values(records)
        update_columns = {
            column_name: statement.excluded[column_name]
            for column_name in UPSERT_UPDATE_COLUMNS
        }
        upsert_statement = statement.on_conflict_do_update(
            index_elements=list(KEY_COLUMNS),
            index_where=WBOrderFeedRecord.account.is_not(None),
            set_=update_columns,
        )
        with Database.get_engine().begin() as connection:
            connection.execute(upsert_statement)

    def _is_transient_database_error(self, error: SQLAlchemyError) -> bool:
        """Отличает обрыв соединения от ошибок данных, ограничений и SQL-схемы."""
        if isinstance(error, (OperationalError, DisconnectionError)):
            return True
        if isinstance(error, DBAPIError) and error.connection_invalidated:
            return True
        sqlstate = getattr(getattr(error, "orig", None), "pgcode", None)
        return isinstance(sqlstate, str) and sqlstate.startswith("08")

    def _deduplicate_by_keys(
        self,
        rows: Sequence[OrderFeedDatabaseRow],
    ) -> tuple[list[OrderFeedDatabaseRow], int]:
        """Оставляет самый новый статус заказа при дублях внутри одной страницы WB."""
        rows_by_key: dict[tuple[str, str], OrderFeedDatabaseRow] = {}
        for row in rows:
            key = (row.account, row.srid)
            current = rows_by_key.get(key)
            if current is None or row.updated_at >= current.updated_at:
                rows_by_key[key] = row
        result = list(rows_by_key.values())
        collapsed = len(rows) - len(result)
        if collapsed:
            logger.warning(
                "Дубли Order Feed внутри страницы свёрнуты по новейшему статусу | rows=%s",
                collapsed,
            )
        return result, collapsed
