"""Сохранение агрегированных дневных остатков WMS в PostgreSQL."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Column, MetaData, Table, UniqueConstraint, inspect, text
from sqlalchemy.sql.type_api import TypeEngine

from src_oop.core.database import Database
from src_oop.jobs.wms_stocks.config import (
    WMS_STOCK_KEY_COLUMNS,
    WMS_STOCK_LEGACY_COLUMN_RENAMES,
    WMS_STOCK_SCHEMA_DEFINITION,
    WMS_STOCK_TABLE_NAME,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WMSStockSaveResult:
    """Итог подготовки и записи дневных WMS-остатков в PostgreSQL.

    Бизнес-сценарий:
    после каждого запуска важно видеть, сколько строк пришло из WMS, сколько
    было отброшено из-за пустых ключей и сколько реально ушло в витрину
    `public.wms_stock`.
    """

    input_rows: int
    rows_after_missing_keys_filter: int
    rows_after_key_deduplication: int
    dropped_missing_key_rows: int
    collapsed_duplicate_rows: int
    written_rows: int


class WMSStockRepository:
    """Записывает агрегированные дневные остатки WMS в `public.wms_stock`.

    Бизнес-сценарий:
    витрина нужна как единый дневной срез суммарных остатков по `product_id`,
    поэтому повторный запуск за последние дни должен обновлять существующую
    строку, а не создавать дубли.
    """

    def __init__(self, database_cls: type[Database] = Database) -> None:
        """Подключает общий Database-слой проекта без локального состояния job."""
        self.database_cls = database_cls

    def save(self, dataframe: pd.DataFrame) -> WMSStockSaveResult:
        """Готовит данные и выполняет upsert дневных остатков в PostgreSQL.

        Бизнес-сценарий:
        ежедневная и backfill-выгрузки используют один и тот же путь записи,
        чтобы таблица `public.wms_stock` всегда соблюдала уникальность
        `(balance_date, product_id)` и не расходилась по правилам обработки.
        """
        input_rows = len(dataframe.index)
        prepared_df, dropped_missing_key_rows = self._drop_rows_with_missing_keys(dataframe)
        rows_after_missing_keys_filter = len(prepared_df.index)
        deduplicated_df, collapsed_duplicate_rows = self._deduplicate_by_keys(prepared_df)
        rows_after_key_deduplication = len(deduplicated_df.index)

        if deduplicated_df.empty:
            logger.warning(
                "После подготовки дневных WMS-остатков не осталось строк для записи в PostgreSQL."
            )
            return WMSStockSaveResult(
                input_rows=input_rows,
                rows_after_missing_keys_filter=rows_after_missing_keys_filter,
                rows_after_key_deduplication=rows_after_key_deduplication,
                dropped_missing_key_rows=dropped_missing_key_rows,
                collapsed_duplicate_rows=collapsed_duplicate_rows,
                written_rows=0,
            )

        self._ensure_database_table()
        self._rename_legacy_database_columns()
        self._ensure_database_columns()
        self.database_cls.sync_data_to_postgres(
            table_name=WMS_STOCK_TABLE_NAME,
            data=self._prepare_dataframe_for_database(deduplicated_df),
            schema_definition=WMS_STOCK_SCHEMA_DEFINITION,
            unique_keys=list(WMS_STOCK_KEY_COLUMNS),
        )
        logger.info(
            "Upsert дневных WMS-остатков завершен | table=%s | written_rows=%s",
            WMS_STOCK_TABLE_NAME,
            len(deduplicated_df.index),
        )
        return WMSStockSaveResult(
            input_rows=input_rows,
            rows_after_missing_keys_filter=rows_after_missing_keys_filter,
            rows_after_key_deduplication=rows_after_key_deduplication,
            dropped_missing_key_rows=dropped_missing_key_rows,
            collapsed_duplicate_rows=collapsed_duplicate_rows,
            written_rows=len(deduplicated_df.index),
        )

    def _drop_rows_with_missing_keys(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Удаляет строки без даты или `product_id`, чтобы batch upsert не падал на ключах.

        Бизнес-правило:
        витрина `public.wms_stock` хранит только записи, которые можно надежно
        идентифицировать по дню и товару; строки без этих полей не должны
        попадать в БД даже при частично корректном ответе API.
        """
        if dataframe.empty:
            return dataframe.copy(), 0

        missing_key_mask = dataframe.loc[:, list(WMS_STOCK_KEY_COLUMNS)].isnull().any(axis=1)
        missing_count = int(missing_key_mask.sum())
        if missing_count:
            sample_rows = (
                dataframe.loc[missing_key_mask, list(WMS_STOCK_KEY_COLUMNS)]
                .head(10)
                .to_dict(orient="records")
            )
            logger.warning(
                "Удаляются дневные WMS-остатки с пустыми ключами | key_columns=%s | rows=%s | sample_rows=%s",
                WMS_STOCK_KEY_COLUMNS,
                missing_count,
                sample_rows,
            )
        return dataframe.loc[~missing_key_mask].copy(), missing_count

    def _deduplicate_by_keys(self, dataframe: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Сворачивает дубли по `(balance_date, product_id)` перед upsert.

        Бизнес-правило:
        даже если API или промежуточная агрегация вернули повторную строку по
        тому же товару и дню, в БД должна остаться только последняя подготовленная
        версия записи.
        """
        if dataframe.empty:
            return dataframe.copy(), 0

        duplicate_mask = dataframe.duplicated(subset=list(WMS_STOCK_KEY_COLUMNS), keep=False)
        duplicate_rows = dataframe.loc[duplicate_mask].copy()
        duplicate_count = len(duplicate_rows.index)
        if duplicate_count:
            top_duplicate_keys = (
                duplicate_rows.groupby(list(WMS_STOCK_KEY_COLUMNS), dropna=False)
                .size()
                .reset_index(name="duplicate_count")
                .sort_values(by="duplicate_count", ascending=False)
                .head(10)
                .to_dict(orient="records")
            )
            logger.warning(
                "Найдены дубли дневных WMS-остатков по ключу, сохраняется последняя строка | key_columns=%s | duplicate_rows=%s | top_duplicate_keys=%s",
                WMS_STOCK_KEY_COLUMNS,
                duplicate_count,
                top_duplicate_keys,
            )

        deduplicated_df = dataframe.drop_duplicates(
            subset=list(WMS_STOCK_KEY_COLUMNS),
            keep="last",
        ).copy()
        collapsed_rows = duplicate_count - len(
            duplicate_rows.drop_duplicates(subset=list(WMS_STOCK_KEY_COLUMNS), keep="last").index
        )
        return deduplicated_df, max(collapsed_rows, 0)

    def _prepare_dataframe_for_database(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Готовит записи к PostgreSQL, заменяя pandas-пропуски на SQL `NULL`.

        Бизнес-правило:
        регулярное обновление за последние 7 дней и исторический backfill
        должны одинаково записывать даты и числа без `NaN` и `NaT`, иначе
        массовый upsert будет нестабилен на пустых полях.
        """
        db_dataframe = dataframe.loc[:, list(WMS_STOCK_SCHEMA_DEFINITION)].copy()
        return db_dataframe.astype(object).where(pd.notna(db_dataframe), None)

    def _ensure_database_table(self) -> None:
        """Создает `public.wms_stock`, если таблица еще не была создана раньше.

        Бизнес-сценарий:
        новая выгрузка не должна зависеть от ручной миграции. Таблица с
        ключом `(balance_date, product_id)` должна появляться автоматически при
        первом штатном запуске или backfill.
        """
        engine = self.database_cls.get_engine()
        metadata = MetaData()
        table = Table(
            WMS_STOCK_TABLE_NAME,
            metadata,
            *(Column(column_name, column_type) for column_name, column_type in WMS_STOCK_SCHEMA_DEFINITION.items()),
            UniqueConstraint(*WMS_STOCK_KEY_COLUMNS, name=f"uq_{WMS_STOCK_TABLE_NAME}_keys"),
        )
        metadata.create_all(engine, tables=[table])

    def _ensure_database_columns(self) -> None:
        """Добавляет недостающие колонки в `public.wms_stock` после расширения витрины.

        Бизнес-сценарий:
        таблица могла быть создана ранней версией job без части полей зон.
        Метод безопасно доводит схему до актуального состояния, чтобы повторный
        запуск начал писать остатки по всем зонам без ручной правки PostgreSQL.
        """
        engine = self.database_cls.get_engine()
        existing_columns = set(self._get_existing_column_types())

        for column_name, column_type in WMS_STOCK_SCHEMA_DEFINITION.items():
            if column_name in existing_columns:
                continue

            compiled_type = self._compile_sqlalchemy_type(
                column_type=column_type,
                dialect=engine.dialect,
            )
            alter_sql = text(
                f'ALTER TABLE "{WMS_STOCK_TABLE_NAME}" '
                f'ADD COLUMN IF NOT EXISTS "{column_name}" {compiled_type}'
            )
            with engine.begin() as connection:
                connection.execute(alter_sql)
            logger.info(
                "В таблицу wms_stock добавлена недостающая колонка | column=%s | type=%s",
                column_name,
                compiled_type,
            )

    def _rename_legacy_database_columns(self) -> None:
        """Переименовывает старые транслитерированные поля зон без потери данных.

        Бизнес-правило:
        после уточнения имен колонок витрина должна использовать понятные
        англоязычные названия, но накопленные остатки нельзя переносить в новую
        таблицу или терять. Переименование выполняется только когда нового поля
        еще нет; при конфликте схема остается без изменений для ручной проверки.
        """
        engine = self.database_cls.get_engine()
        existing_columns = set(self._get_existing_column_types())

        for legacy_name, actual_name in WMS_STOCK_LEGACY_COLUMN_RENAMES.items():
            if legacy_name not in existing_columns:
                continue
            if actual_name in existing_columns:
                logger.warning(
                    "Колонки wms_stock не переименованы из-за конфликта имен | old=%s | new=%s",
                    legacy_name,
                    actual_name,
                )
                continue

            rename_sql = text(
                f'ALTER TABLE "{WMS_STOCK_TABLE_NAME}" '
                f'RENAME COLUMN "{legacy_name}" TO "{actual_name}"'
            )
            with engine.begin() as connection:
                connection.execute(rename_sql)
            existing_columns.remove(legacy_name)
            existing_columns.add(actual_name)
            logger.info(
                "Колонка wms_stock переименована без потери данных | old=%s | new=%s",
                legacy_name,
                actual_name,
            )

    def _get_existing_column_types(self) -> dict[str, TypeEngine]:
        """Читает фактическую схему `public.wms_stock` перед точечным расширением колонок.

        Бизнес-сценарий:
        job должна понимать, какие поля уже есть в БД, чтобы дозаводить только
        недостающие колонки и не вмешиваться в существующую рабочую таблицу.
        """
        inspector = inspect(self.database_cls.get_engine())
        return {
            column["name"]: column["type"]
            for column in inspector.get_columns(WMS_STOCK_TABLE_NAME)
        }

    def _compile_sqlalchemy_type(self, column_type, dialect) -> str:
        """Преобразует тип SQLAlchemy в SQL-строку для `ALTER TABLE`.

        Бизнес-сценарий:
        автоматическое расширение `public.wms_stock` должно одинаково работать
        и для классов типов SQLAlchemy, и для их экземпляров без ручного
        составления SQL под каждую новую колонку витрины.
        """
        resolved_type = column_type
        if isinstance(column_type, type) and issubclass(column_type, TypeEngine):
            resolved_type = column_type()
        return resolved_type.compile(dialect=dialect)
