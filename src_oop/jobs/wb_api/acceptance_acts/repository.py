"""Repository-слой для записи актов WB в PostgreSQL."""

from __future__ import annotations

from dataclasses import asdict
import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd
from sqlalchemy import BigInteger, Date, Integer, String, text

from src_oop.core.database import Database
from src_oop.jobs.wb_api.acceptance_acts.config import (
    DB_WRITE_CHUNK_SIZE,
    FBO_TABLE_NAME,
    FBO_UNIQUE_KEYS,
    FBS_TABLE_NAME,
    FBS_UNIQUE_KEYS,
    REFRESH_FBS_MATERIALIZED_VIEW_SQL,
)
from src_oop.jobs.wb_api.acceptance_acts.models import (
    DBWriteResult,
    NormalizedFboRow,
    NormalizedFbsRow,
)

logger = logging.getLogger(__name__)

FBO_SCHEMA_DEFINITION = {
    "num": Integer,
    "product_name": String(255),
    "unit": String(50),
    "barcode": String(50),
    "vendor_code": String(50),
    "size": String(50),
    "kiz": String(255),
    "box_barcode": String(50),
    "quantity": Integer,
    "document": String(255),
    "document_number": String(50),
    "date": Date,
    "shk_id": BigInteger,
    "account": String(100),
}

FBS_SCHEMA_DEFINITION = {
    "num": Integer,
    "order_number": String(255),
    "unit": String(50),
    "sticker": String(255),
    "quantity": Integer,
    "document": String(255),
    "document_number": String(50),
    "date": Date,
    "account": String(50),
}


class AcceptanceActsRepository:
    """Слой записи нормализованных строк актов в существующие таблицы БД."""

    def __init__(self, database_cls: Any = Database) -> None:
        """Инициализирует repository.

        `database_cls` оставлен инъекцией зависимости, чтобы repository можно
        было проверять с fake/mock Database без реального PostgreSQL.
        """
        self.database_cls = database_cls

    def save_fbo_rows(self, rows: Sequence[NormalizedFboRow]) -> DBWriteResult:
        """Обёртка над chunk-wise записью ФБО.

        Repository рассчитан на chunk-wise pipeline. Этот wrapper не должен
        использоваться как место для накопления всех строк job в одном списке,
        но может безопасно агрегировать уже переданный ограниченный набор.
        """
        return self._save_rows_via_wrapper(
            rows=rows,
            save_chunk_method=self.save_fbo_rows_chunk,
            table_name=FBO_TABLE_NAME,
        )

    def save_fbs_rows(self, rows: Sequence[NormalizedFbsRow]) -> DBWriteResult:
        """Обёртка над chunk-wise записью ФБС.

        Repository работает chunk-wise. Этот wrapper предназначен только для
        совместимости и агрегации результата по уже ограниченному набору строк.
        """
        return self._save_rows_via_wrapper(
            rows=rows,
            save_chunk_method=self.save_fbs_rows_chunk,
            table_name=FBS_TABLE_NAME,
        )

    def save_fbo_rows_chunk(
        self,
        rows: Sequence[NormalizedFboRow],
        chunk_size: int = DB_WRITE_CHUNK_SIZE,
    ) -> DBWriteResult:
        """Записывает один chunk строк ФБО через upsert."""
        return self._save_rows_chunk(
            rows=rows,
            table_name=FBO_TABLE_NAME,
            unique_keys=FBO_UNIQUE_KEYS,
            schema_definition=FBO_SCHEMA_DEFINITION,
            act_type="fbo",
            chunk_size=chunk_size,
        )

    def save_fbs_rows_chunk(
        self,
        rows: Sequence[NormalizedFbsRow],
        chunk_size: int = DB_WRITE_CHUNK_SIZE,
    ) -> DBWriteResult:
        """Записывает один chunk строк ФБС через upsert."""
        return self._save_rows_chunk(
            rows=rows,
            table_name=FBS_TABLE_NAME,
            unique_keys=FBS_UNIQUE_KEYS,
            schema_definition=FBS_SCHEMA_DEFINITION,
            act_type="fbs",
            chunk_size=chunk_size,
        )

    def refresh_fbs_check_mv(self) -> DBWriteResult:
        """Выполняет non-fatal refresh materialized view для ФБС."""
        logger.info(
            "Запуск refresh materialized view для ФБС: sql=%s",
            REFRESH_FBS_MATERIALIZED_VIEW_SQL,
        )

        try:
            engine = self.database_cls.get_engine()
            with engine.begin() as connection:
                connection.execute(text(REFRESH_FBS_MATERIALIZED_VIEW_SQL))

            logger.info("Refresh materialized view public.check_act_fbs выполнен успешно.")
            return DBWriteResult(
                table_name="public.check_act_fbs",
                input_rows=0,
                written_rows=0,
                status="success",
            )
        except Exception as error:
            logger.warning(
                "Ошибка refresh materialized view public.check_act_fbs: %s",
                error,
            )
            return DBWriteResult(
                table_name="public.check_act_fbs",
                input_rows=0,
                written_rows=0,
                status="partial",
                warnings=["REFRESH MATERIALIZED VIEW завершился с ошибкой."],
                errors=[str(error)],
            )

    def _save_rows_via_wrapper(
        self,
        rows: Sequence[NormalizedFboRow] | Sequence[NormalizedFbsRow],
        save_chunk_method,
        table_name: str,
    ) -> DBWriteResult:
        """Агрегирует результат wrapper-метода поверх chunk saver."""
        if not rows:
            return DBWriteResult(
                table_name=table_name,
                input_rows=0,
                written_rows=0,
                status="success",
                warnings=["Пустой набор строк: SQL не выполнялся."],
            )

        aggregate_result = save_chunk_method(rows)
        return aggregate_result

    def _save_rows_chunk(
        self,
        rows: Sequence[NormalizedFboRow] | Sequence[NormalizedFbsRow],
        table_name: str,
        unique_keys: tuple[str, ...],
        schema_definition: dict[str, Any],
        act_type: str,
        chunk_size: int,
    ) -> DBWriteResult:
        """Общий путь записи одного chunk в PostgreSQL."""
        if not rows:
            logger.info(
                "Пропуск пустого chunk записи: table=%s act_type=%s unique_keys=%s",
                table_name,
                act_type,
                unique_keys,
            )
            return DBWriteResult(
                table_name=table_name,
                input_rows=0,
                written_rows=0,
                status="success",
                warnings=["Пустой chunk: SQL не выполнялся."],
            )

        records = self._rows_to_records(rows)
        dataframe = self._records_to_dataframe(records)

        logger.info(
            "Запись chunk в PostgreSQL: table=%s act_type=%s chunk_size=%s unique_keys=%s",
            table_name,
            act_type,
            len(rows),
            unique_keys,
        )

        try:
            self.database_cls.sync_data_to_postgres(
                table_name=table_name,
                data=dataframe,
                schema_definition=schema_definition,
                unique_keys=unique_keys,
                chunk_size=chunk_size,
            )
            logger.info(
                "Chunk успешно записан: table=%s act_type=%s rows=%s",
                table_name,
                act_type,
                len(rows),
            )
            return DBWriteResult(
                table_name=table_name,
                input_rows=len(rows),
                written_rows=len(rows),
                status="success",
            )
        except Exception as error:
            logger.exception(
                "Ошибка записи chunk: table=%s act_type=%s rows=%s error=%s",
                table_name,
                act_type,
                len(rows),
                error,
            )
            return DBWriteResult(
                table_name=table_name,
                input_rows=len(rows),
                written_rows=0,
                status="failed",
                errors=[str(error)],
            )

    def _rows_to_records(
        self,
        rows: Sequence[NormalizedFboRow] | Sequence[NormalizedFbsRow],
    ) -> list[dict[str, Any]]:
        """Преобразует dataclass-строки в список словарей для database adapter."""
        return [asdict(row) for row in rows]

    def _records_to_dataframe(self, records: list[dict[str, Any]]) -> pd.DataFrame:
        """Создаёт маленький DataFrame из одного chunk."""
        return pd.DataFrame.from_records(records)


class DryRunAcceptanceActsRepository:
    """No-op repository для безопасного dry-run без записи в PostgreSQL.

    Класс повторяет минимальный интерфейс, который использует service-слой:
    `save_fbo_rows_chunk(...)`, `save_fbs_rows_chunk(...)` и
    `refresh_fbs_check_mv()`.
    """

    def save_fbo_rows_chunk(
        self,
        rows: Sequence[NormalizedFboRow],
        chunk_size: int = DB_WRITE_CHUNK_SIZE,
    ) -> DBWriteResult:
        """Имитирует запись chunk ФБО без обращения к БД."""
        return self._build_dry_run_result(
            table_name=FBO_TABLE_NAME,
            rows_count=len(rows),
            operation=f"save_fbo_rows_chunk(chunk_size={chunk_size})",
        )

    def save_fbs_rows_chunk(
        self,
        rows: Sequence[NormalizedFbsRow],
        chunk_size: int = DB_WRITE_CHUNK_SIZE,
    ) -> DBWriteResult:
        """Имитирует запись chunk ФБС без обращения к БД."""
        return self._build_dry_run_result(
            table_name=FBS_TABLE_NAME,
            rows_count=len(rows),
            operation=f"save_fbs_rows_chunk(chunk_size={chunk_size})",
        )

    def refresh_fbs_check_mv(self) -> DBWriteResult:
        """Пропускает refresh MV в dry-run режиме."""
        logger.info(
            "Dry-run: REFRESH MATERIALIZED VIEW пропущен: sql=%s",
            REFRESH_FBS_MATERIALIZED_VIEW_SQL,
        )
        return DBWriteResult(
            table_name="public.check_act_fbs",
            input_rows=0,
            written_rows=0,
            status="success",
            warnings=["dry-run: refresh skipped"],
        )

    def _build_dry_run_result(
        self,
        table_name: str,
        rows_count: int,
        operation: str,
    ) -> DBWriteResult:
        """Собирает единый результат пропуска DB write в dry-run режиме."""
        logger.info(
            "Dry-run: запись в БД пропущена: table=%s rows=%s operation=%s",
            table_name,
            rows_count,
            operation,
        )
        return DBWriteResult(
            table_name=table_name,
            input_rows=rows_count,
            written_rows=0,
            status="success",
            warnings=["dry-run: database write skipped"],
        )
