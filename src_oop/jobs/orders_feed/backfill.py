"""Разовый пакетный перенос legacy-данных orders и sales в WB Order Feed."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import StrEnum
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from src_oop.core.database import Database
from src_oop.jobs.orders_feed.exceptions import OrderFeedRepositoryError
from src_oop.jobs.orders_feed.models import WBOrderFeedRecord
from src_oop.jobs.orders_feed.run import MOSCOW_TZ, _parse_datetime
from src_oop.jobs.orders_feed.schemas.backfill import LegacyOrderFeedRow
from src_oop.jobs.orders_feed.schemas.enums import DataSource

logger = logging.getLogger(__name__)

# Один row содержит много bind-параметров; размер оставляет запас до лимита PostgreSQL.
DEFAULT_BATCH_SIZE = 2_000

SOURCE_COLUMNS = """
    source.srid,
    source.article_id,
    source.date_from,
    source.last_change_date,
    source.warehouse_name,
    source.warehouse_type,
    source.country_name,
    source.oblast_okrug_name,
    source.region_name,
    source.total_price,
    source.discount_percent,
    source.order_type
"""

LEGACY_UPDATE_COLUMNS = (
    "nm_id",
    "created_at",
    "updated_at",
    "status",
    "cancel_type",
    "warehouse_name",
    "warehouse_region",
    "warehouse_type",
    "destination_city",
    "destination_district",
    "seller_price",
    "currency",
    "sale_type",
    "snapshot_time",
    "loaded_at",
)


class BackfillSource(StrEnum):
    """Разрешённые legacy-источники для управляемого ручного переноса."""

    ORDERS = "orders"
    SALES = "sales"

    @property
    def data_source(self) -> DataSource:
        """Связывает CLI-источник с типизированным значением целевой таблицы."""
        return DataSource(self.value)


@dataclass(slots=True)
class BackfillResult:
    """Сводит объём прочитанных и записанных строк одного backfill."""

    source: BackfillSource
    batches: int = 0
    source_rows: int = 0
    written_rows: int = 0


class OrderFeedBackfill:
    """Читает большие legacy-таблицы keyset-батчами и преобразует их в Python."""

    def run(
        self,
        source: BackfillSource,
        batch_size: int = DEFAULT_BATCH_SIZE,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> BackfillResult:
        """Переносит выбранный источник, сохраняя каждый Python-батч отдельно.

        В память загружается не более `batch_size` исходных строк. Повторный
        запуск безопасен: legacy-строки обновляются по `(data_source, srid)`.
        """
        if batch_size <= 0:
            raise ValueError("Размер батча backfill должен быть больше нуля.")
        period_start, period_end = self._resolve_period(date_from, date_to)
        result = BackfillResult(source=source)
        cursor: int | tuple[date, str] = (
            0 if source is BackfillSource.SALES else (date.min, "")
        )
        while True:
            source_rows = self._fetch_batch(
                source,
                cursor,
                batch_size,
                period_start,
                period_end,
            )
            if not source_rows:
                break
            cursor = self._next_cursor(source, source_rows)
            transformed = self._transform_batch(source, source_rows)
            written_rows = self._upsert_batch(transformed)
            result.batches += 1
            result.source_rows += len(source_rows)
            result.written_rows += written_rows
            logger.info(
                "Батч legacy Order Feed сохранён | source=%s | batch=%s | "
                "source_rows=%s | written_rows=%s | cursor=%s",
                source.value,
                result.batches,
                len(source_rows),
                written_rows,
                cursor,
            )
        logger.info(
            "Backfill Order Feed завершён | source=%s | batches=%s | "
            "source_rows=%s | written_rows=%s | date_from=%s | date_to=%s",
            source.value,
            result.batches,
            result.source_rows,
            result.written_rows,
            period_start,
            period_end,
        )
        return result

    def _resolve_period(
        self,
        date_from: datetime | None,
        date_to: datetime | None,
    ) -> tuple[datetime | None, datetime | None]:
        """Готовит включительные пользовательские даты к полуоткрытому SQL-периоду.

        `date_to` включает весь указанный календарный день. Поэтому в SQL
        передаётся начало следующего дня и используется строгое условие `<`.
        """
        start = date_from
        end_exclusive = date_to
        if start is not None and start.tzinfo is None:
            start = start.replace(tzinfo=MOSCOW_TZ)
        if end_exclusive is not None:
            if end_exclusive.tzinfo is None:
                end_exclusive = end_exclusive.replace(tzinfo=MOSCOW_TZ)
            end_exclusive = datetime.combine(
                end_exclusive.date() + timedelta(days=1),
                time.min,
                tzinfo=end_exclusive.tzinfo,
            )
        if start is not None and end_exclusive is not None and start >= end_exclusive:
            raise ValueError("Начало периода backfill не может быть позже конца.")
        return start, end_exclusive

    def _fetch_batch(
        self,
        source: BackfillSource,
        cursor: int | tuple[date, str],
        batch_size: int,
        date_from: datetime | None,
        date_to: datetime | None,
    ) -> list[Mapping[str, Any]]:
        """Читает только один keyset-батч и отбрасывает строки без article/FK и ключей."""
        query, parameters = self._build_select_query(
            source,
            cursor,
            batch_size,
            date_from,
            date_to,
        )
        try:
            with Database.get_engine().connect() as connection:
                return list(connection.execute(text(query), parameters).mappings())
        except SQLAlchemyError as error:
            raise OrderFeedRepositoryError(
                f"Не удалось прочитать батч из {source.value}: cursor={cursor}."
            ) from error

    def _transform_batch(
        self,
        source: BackfillSource,
        rows: Sequence[Mapping[str, Any]],
    ) -> list[LegacyOrderFeedRow]:
        """Выполняет в Python все бизнес-преобразования и сворачивает дубли srid."""
        rows_by_srid: dict[str, LegacyOrderFeedRow] = {}
        for source_row in rows:
            row = LegacyOrderFeedRow.from_source(source_row, source.data_source)
            current = rows_by_srid.get(row.srid)
            if current is None or row.updated_at >= current.updated_at:
                rows_by_srid[row.srid] = row
        return list(rows_by_srid.values())

    def _upsert_batch(self, rows: Sequence[LegacyOrderFeedRow]) -> int:
        """Сохраняет преобразованный Python-батч одной транзакцией PostgreSQL."""
        if not rows:
            return 0
        records = [row.model_dump(mode="python") for row in rows]
        statement = insert(WBOrderFeedRecord).values(records)
        update_columns = {
            column_name: statement.excluded[column_name]
            for column_name in LEGACY_UPDATE_COLUMNS
        }
        upsert_statement = statement.on_conflict_do_update(
            index_elements=["data_source", "srid"],
            index_where=WBOrderFeedRecord.account.is_(None),
            set_=update_columns,
        )
        try:
            with Database.get_engine().begin() as connection:
                connection.execute(upsert_statement)
        except SQLAlchemyError as error:
            logger.exception(
                "Upsert legacy Order Feed отменён; предыдущие батчи сохранены "
                "| rows=%s",
                len(records),
            )
            raise OrderFeedRepositoryError(
                f"Не удалось сохранить legacy-батч: rows={len(records)}."
            ) from error
        return len(records)

    def _build_select_query(
        self,
        source: BackfillSource,
        cursor: int | tuple[date, str],
        batch_size: int,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> tuple[str, dict[str, object]]:
        """Формирует параметризованный SELECT без бизнес-преобразований и OFFSET."""
        common_filter = """
            source.srid IS NOT NULL
            AND BTRIM(source.srid) <> ''
            AND source.date_from IS NOT NULL
            AND (:date_from IS NULL OR source.date_from >= :date_from)
            AND (:date_to IS NULL OR source.date_from < :date_to)
        """
        period_parameters: dict[str, object] = {
            "date_from": date_from,
            "date_to": date_to,
        }
        if source is BackfillSource.SALES:
            query = f"""
SELECT source.id AS cursor_id, {SOURCE_COLUMNS}, source.sale_id
FROM sales AS source
JOIN article ON article.nm_id = source.article_id
WHERE source.id > :cursor_id AND {common_filter}
ORDER BY source.id
LIMIT :batch_size
"""
            return query, {
                "cursor_id": cursor,
                "batch_size": batch_size,
                **period_parameters,
            }
        cursor_date, cursor_srid = cursor
        query = f"""
SELECT source.date AS cursor_date, {SOURCE_COLUMNS}, source.is_cancel
FROM orders AS source
JOIN article ON article.nm_id = source.article_id
WHERE (source.date, source.srid) > (:cursor_date, :cursor_srid)
  AND source.date IS NOT NULL
  AND {common_filter}
ORDER BY source.date, source.srid
LIMIT :batch_size
"""
        return query, {
            "cursor_date": cursor_date,
            "cursor_srid": cursor_srid,
            "batch_size": batch_size,
            **period_parameters,
        }

    def _next_cursor(
        self,
        source: BackfillSource,
        rows: Sequence[Mapping[str, Any]],
    ) -> int | tuple[date, str]:
        """Берёт keyset-курсор последней исходной строки до дедупликации батча."""
        last_row = rows[-1]
        if source is BackfillSource.SALES:
            return int(last_row["cursor_id"])
        return last_row["cursor_date"], str(last_row["srid"])


def main() -> None:
    """Запускает ручной Python-backfill выбранной legacy-таблицы батчами."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Backfill WB Order Feed из legacy-таблиц"
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=[source.value for source in BackfillSource],
        help="Источник истории: orders или sales",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Число исходных строк в памяти, по умолчанию {DEFAULT_BATCH_SIZE}",
    )
    parser.add_argument(
        "--date-from",
        help="Начало включительно: YYYY-MM-DD, 'YYYY-MM-DD HH:MM' или ISO",
    )
    parser.add_argument(
        "--date-to",
        help="Конечный календарный день включительно в удобном формате даты",
    )
    args = parser.parse_args()
    OrderFeedBackfill().run(
        source=BackfillSource(args.source),
        batch_size=args.batch_size,
        date_from=_parse_datetime(args.date_from),
        date_to=_parse_datetime(args.date_to),
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Backfill Order Feed остановлен пользователем; сохранённые батчи остаются в БД.")
