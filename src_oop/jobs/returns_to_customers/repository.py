"""Запись витрины возвратов покупателей в Google Sheets."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Column, MetaData, Table, UniqueConstraint, inspect, text
from sqlalchemy.dialects.postgresql import ARRAY, insert
from sqlalchemy.sql.type_api import TypeEngine

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.returns_to_customers.config import (
    DB_KEY_COLUMNS,
    DB_SCHEMA_DEFINITION,
    DB_TABLE_NAME,
    DB_TABLE_SCHEMA,
    SHEET_CONFIG,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BuyersReturnsSaveResult:
    """Итог записи витрины возвратов, который нужен для финального лога job."""

    written_rows: int
    db_written_rows: int
    reconciled_rows: int = 0
    dry_run: bool = False


class BuyersReturnsRepository:
    """Публикует витрину возвратов в Google Sheets и синхронизирует техническую таблицу `public.claims`."""

    def __init__(self, database_cls: type[Database] = Database) -> None:
        """Подключает Google Sheets и PostgreSQL без глобального состояния внутри job возвратов."""
        self.database_cls = database_cls

    def save(
        self,
        google_dataframe: pd.DataFrame,
        database_dataframe: pd.DataFrame,
        dry_run: bool = False,
        write_to_google: bool = False,
    ) -> BuyersReturnsSaveResult:
        """Обновляет `public.claims` всегда, а запись в Google Sheets включает только по флагу."""
        if dry_run:
            logger.info(
                "Dry-run возвратов покупателей: запись в Google Sheets и PostgreSQL пропущена | gs_rows=%s | db_rows=%s | db_columns=%s | write_to_google=%s",
                len(google_dataframe.index),
                len(database_dataframe.index),
                list(database_dataframe.columns),
                write_to_google,
            )
            return BuyersReturnsSaveResult(
                written_rows=0,
                db_written_rows=0,
                dry_run=True,
            )

        db_written_rows = self._save_to_database(database_dataframe)
        reconciled_rows = self.reconcile_approved_returns_with_fin_reports()
        written_rows = 0
        if write_to_google:
            written_rows = self._save_to_google(google_dataframe)
        else:
            logger.info(
                "Запись возвратов покупателей в Google Sheets отключена флагом | table=%s | sheet=%s | rows_ready=%s",
                SHEET_CONFIG.table_title,
                SHEET_CONFIG.sheet_title,
                len(google_dataframe.index),
            )
        return BuyersReturnsSaveResult(
            written_rows=written_rows,
            db_written_rows=db_written_rows,
            reconciled_rows=reconciled_rows,
            dry_run=False,
        )

    def _save_to_google(self, dataframe: pd.DataFrame) -> int:
        """Перезаписывает вкладку возвратов актуальным срезом по всем кабинетам за доступные 14 дней."""
        connector = GoogleTabs(
            table_title=SHEET_CONFIG.table_title,
            sheet_title=SHEET_CONFIG.sheet_title,
        )
        connector._update_df_in_google(df=dataframe, sheet=connector.sheet_title)
        logger.info(
            "Витрина возвратов покупателей обновлена в Google Sheets | table=%s | sheet=%s | rows=%s",
            SHEET_CONFIG.table_title,
            SHEET_CONFIG.sheet_title,
            len(dataframe.index),
        )
        return len(dataframe.index)

    def _save_to_database(self, dataframe: pd.DataFrame) -> int:
        """Записывает возвраты в `public.claims`, добавляя недостающие колонки перед upsert."""
        if dataframe.empty:
            logger.warning(
                "Запись возвратов покупателей в PostgreSQL пропущена: после нормализации нет строк."
            )
            self._ensure_database_table()
            return 0

        self._ensure_database_table()
        self._ensure_database_columns()
        self._ensure_database_unique_index()

        engine = self.database_cls.get_engine()
        existing_column_types = self._get_existing_column_types()
        prepared_dataframe = self._prepare_dataframe_for_database_write(
            dataframe=dataframe,
            existing_column_types=existing_column_types,
        )
        records = self._build_database_records(prepared_dataframe)
        metadata = MetaData()
        table = Table(
            DB_TABLE_NAME,
            metadata,
            *(Column(column_name, column_type) for column_name, column_type in DB_SCHEMA_DEFINITION.items()),
            UniqueConstraint(*DB_KEY_COLUMNS, name=f"uq_{DB_TABLE_NAME}_account_claim_id"),
            schema=DB_TABLE_SCHEMA,
        )

        with engine.begin() as connection:
            stmt = insert(table).values(records)
            available_columns = set(records[0].keys())
            update_columns = {
                column.name: getattr(stmt.excluded, column.name)
                for column in table.c
                if column.name not in DB_KEY_COLUMNS and column.name in available_columns
            }
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=list(DB_KEY_COLUMNS),
                set_=update_columns,
            )
            connection.execute(upsert_stmt)

        logger.info(
            "Возвраты покупателей синхронизированы в PostgreSQL | schema=%s | table=%s | rows=%s",
            DB_TABLE_SCHEMA,
            DB_TABLE_NAME,
            len(dataframe.index),
        )
        return len(prepared_dataframe.index)

    def reconcile_approved_returns_with_fin_reports(self) -> int:
        """Сверяет одобренные возвраты с `daily_fin_reports_full` и обновляет признаки компенсации.

        Бизнес-правило: надёжная связь между заявкой WB и финансовым отчётом строится по
        `account + srid`. Операции с `supplier_oper_name`, содержащим `компенсац` или `возмещ`,
        считаются компенсационно-подобными, а операция `Возврат` сохраняется отдельным признаком,
        чтобы аналитика видела и факт возврата, и возможное возмещение со стороны площадки.
        """
        self._ensure_database_columns()
        claims = self._get_approved_claims_for_fin_reconciliation()
        updates = self._build_fin_reconciliation_updates(claims)
        reconciled_rows = self._update_claims_fin_reconciliation(updates)
        logger.info(
            "Сверка одобренных возвратов с daily_fin_reports_full завершена | rows=%s | key=account+srid",
            reconciled_rows,
        )
        return reconciled_rows

    def _get_approved_claims_for_fin_reconciliation(self) -> list[dict[str, object]]:
        """Возвращает одобренные возвраты, для которых можно искать финансовые операции по `account + srid`.

        Бизнес-правило: сверка компенсаций выполняется только для заявок со статусом `Одобрено`,
        заполненным кабинетом, `srid` и датой заказа. Заявки без этих полей не имеют надёжного
        ключа связи с `daily_fin_reports_full` и не должны притягиваться по слабым совпадениям.
        """
        query = text(
            """
            SELECT
                id,
                account,
                srid,
                order_dt::date - 14 AS date_from
            FROM public.claims
            WHERE status = 2
              AND account IS NOT NULL
              AND srid IS NOT NULL
              AND order_dt IS NOT NULL
            """
        )
        with self.database_cls.get_engine().connect() as connection:
            return [dict(row) for row in connection.execute(query).mappings()]

    def _build_fin_reconciliation_updates(
        self,
        claims: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Собирает порционные итоги финопераций для одобренных возвратов.

        Бизнес-правило: вместо тяжёлого join по всей `daily_fin_reports_full` каждая заявка
        проверяется маленьким запросом по уже индексируемому окну `account + date_from`, а `srid`
        остаётся строгим ключом внутри этого окна.
        """
        if not claims:
            return []

        query = text(
            """
            SELECT
                COUNT(*) AS matched_rows,
                COUNT(*) FILTER (
                    WHERE supplier_oper_name = 'Возврат'
                ) AS return_rows,
                COALESCE(SUM(ppvz_for_pay) FILTER (
                    WHERE supplier_oper_name = 'Возврат'
                ), 0) AS return_sum,
                COUNT(*) FILTER (
                    WHERE supplier_oper_name ILIKE '%компенсац%'
                       OR supplier_oper_name ILIKE '%возмещ%'
                ) AS compensation_rows,
                COALESCE(SUM(COALESCE(ppvz_for_pay, 0) + COALESCE(additional_payment, 0)) FILTER (
                    WHERE supplier_oper_name ILIKE '%компенсац%'
                       OR supplier_oper_name ILIKE '%возмещ%'
                ), 0) AS compensation_sum,
                STRING_AGG(
                    DISTINCT COALESCE(supplier_oper_name, '') || ' / ' || COALESCE(doc_type_name, ''),
                    '; ' ORDER BY COALESCE(supplier_oper_name, '') || ' / ' || COALESCE(doc_type_name, '')
                ) AS operation_names
            FROM public.daily_fin_reports_full
            WHERE account = :account
              AND srid = :srid
              AND date_from BETWEEN :date_from AND CURRENT_DATE
            """
        )
        updates: list[dict[str, object]] = []
        with self.database_cls.get_engine().connect() as connection:
            for claim in claims:
                row = connection.execute(
                    query,
                    {
                        "account": claim["account"],
                        "srid": claim["srid"],
                        "date_from": claim["date_from"],
                    },
                ).mappings().one()
                matched_rows = int(row["matched_rows"] or 0)
                return_rows = int(row["return_rows"] or 0)
                compensation_rows = int(row["compensation_rows"] or 0)
                updates.append(
                    {
                        "id": claim["id"],
                        "fin_srid_matched": matched_rows > 0,
                        "fin_matched_rows": matched_rows,
                        "fin_return_rows": return_rows,
                        "fin_return_sum": row["return_sum"],
                        "fin_compensation_rows": compensation_rows,
                        "fin_compensation_sum": row["compensation_sum"],
                        "fin_has_return": return_rows > 0,
                        "fin_has_compensation": compensation_rows > 0,
                        "fin_operation_names": row["operation_names"],
                    }
                )

        return updates

    def _update_claims_fin_reconciliation(
        self,
        updates: list[dict[str, object]],
    ) -> int:
        """Записывает результат сверки с финтаблицей обратно в `public.claims`.

        Бизнес-правило: признаки компенсации хранятся рядом с заявкой на возврат, чтобы последующая
        аналитика могла быстро отобрать одобренные возвраты с найденным или отсутствующим возмещением.
        """
        if not updates:
            logger.info(
                "Сверка одобренных возвратов с daily_fin_reports_full пропущена: нет заявок с надёжным ключом account+srid."
            )
            return 0

        query = text(
            """
            UPDATE public.claims
            SET
                fin_srid_matched = :fin_srid_matched,
                fin_matched_rows = :fin_matched_rows,
                fin_return_rows = :fin_return_rows,
                fin_return_sum = :fin_return_sum,
                fin_compensation_rows = :fin_compensation_rows,
                fin_compensation_sum = :fin_compensation_sum,
                fin_has_return = :fin_has_return,
                fin_has_compensation = :fin_has_compensation,
                fin_operation_names = :fin_operation_names,
                fin_checked_at = NOW()
            WHERE id = :id
            """
        )
        with self.database_cls.get_engine().begin() as connection:
            result = connection.execute(query, updates)

        return result.rowcount if result.rowcount is not None else len(updates)

    def _ensure_database_table(self) -> None:
        """Создаёт таблицу `public.claims`, если её ещё нет, чтобы job не зависела от ручной миграции."""
        engine = self.database_cls.get_engine()
        metadata = MetaData()
        table = Table(
            DB_TABLE_NAME,
            metadata,
            *(Column(column_name, column_type) for column_name, column_type in DB_SCHEMA_DEFINITION.items()),
            UniqueConstraint(*DB_KEY_COLUMNS, name=f"uq_{DB_TABLE_NAME}_account_claim_id"),
            schema=DB_TABLE_SCHEMA,
        )
        metadata.create_all(engine, tables=[table])

    def _ensure_database_columns(self) -> None:
        """Добавляет недостающие колонки в `public.claims`, если таблица была создана ранее с меньшей схемой."""
        engine = self.database_cls.get_engine()
        existing_columns = set(self._get_existing_column_types())

        for column_name, column_type in DB_SCHEMA_DEFINITION.items():
            if column_name in existing_columns:
                continue

            compiled_type = self._compile_sqlalchemy_type(
                column_type=column_type,
                dialect=engine.dialect,
            )
            alter_sql = text(
                f'ALTER TABLE "{DB_TABLE_SCHEMA}"."{DB_TABLE_NAME}" '
                f'ADD COLUMN IF NOT EXISTS "{column_name}" {compiled_type}'
            )
            with engine.begin() as connection:
                connection.execute(alter_sql)
            logger.info(
                "В таблицу claims добавлена недостающая колонка | column=%s | type=%s",
                column_name,
                compiled_type,
            )

    def _ensure_database_unique_index(self) -> None:
        """Гарантирует уникальность `account + claim_id`, чтобы upsert обновлял заявку вместо дублирования."""
        if DB_KEY_COLUMNS == ("id",):
            logger.info(
                "Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Р№ СѓРЅРёРєР°Р»СЊРЅС‹Р№ РёРЅРґРµРєСЃ РґР»СЏ claims РЅРµ С‚СЂРµР±СѓРµС‚СЃСЏ: upsert РёРґС‘С‚ РїРѕ РєР»СЋС‡Сѓ id."
            )
            return
        index_sql = text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS ix_{DB_TABLE_NAME}_account_claim_id '
            f'ON "{DB_TABLE_SCHEMA}"."{DB_TABLE_NAME}" ("account", "claim_id")'
        )
        with self.database_cls.get_engine().begin() as connection:
            connection.execute(index_sql)

    def _compile_sqlalchemy_type(self, column_type, dialect) -> str:
        """Преобразует тип SQLAlchemy в SQL-строку для `ALTER TABLE`, поддерживая и классы, и экземпляры типов."""
        resolved_type = column_type
        if isinstance(column_type, type) and issubclass(column_type, TypeEngine):
            resolved_type = column_type()
        return resolved_type.compile(dialect=dialect)

    def _get_existing_column_types(self) -> dict[str, TypeEngine]:
        """Читает фактические типы колонок `public.claims`, чтобы запись учитывала уже существующую схему таблицы."""
        inspector = inspect(self.database_cls.get_engine())
        return {
            column["name"]: column["type"]
            for column in inspector.get_columns(DB_TABLE_NAME, schema=DB_TABLE_SCHEMA)
        }

    def _prepare_dataframe_for_database_write(
        self,
        dataframe: pd.DataFrame,
        existing_column_types: dict[str, TypeEngine],
    ) -> pd.DataFrame:
        """Подстраивает значения под реальные типы колонок PostgreSQL, чтобы старые колонки не ломали upsert."""
        prepared_dataframe = dataframe.copy()
        for column_name in prepared_dataframe.columns:
            column_type = existing_column_types.get(column_name)
            if column_type is None:
                continue

            prepared_dataframe[column_name] = prepared_dataframe[column_name].map(
                lambda value: self._adapt_value_to_database_column(
                    value=value,
                    column_type=column_type,
                )
            )
        return prepared_dataframe.astype(object).where(pd.notna(prepared_dataframe), None)

    def _build_database_records(self, dataframe: pd.DataFrame) -> list[dict[str, object]]:
        """Р¤РёРЅР°Р»СЊРЅРѕ РѕС‡РёС‰Р°РµС‚ Р·Р°РїРёСЃРё РїРµСЂРµРґ upsert РІ `public.claims`.

        Р‘РёР·РЅРµСЃ-РїСЂР°РІРёР»Рѕ: РїСѓСЃС‚С‹Рµ РґР°С‚С‹ Рё РґСЂСѓРіРёРµ pandas-РїСЂРѕРїСѓСЃРєРё РЅРµ РґРѕР»Р¶РЅС‹ РїРѕРїР°РґР°С‚СЊ РІ PostgreSQL
        РєР°Рє `NaT`, `NA` РёР»Рё СЃС‚СЂРѕРєР° `"NaT"`, РёРЅР°С‡Рµ РІС‹РіСЂСѓР·РєР° РїР°РґР°РµС‚ РЅР° РїСѓСЃС‚С‹С… РїРѕР»СЏС….
        """
        records = dataframe.to_dict(orient="records")
        sanitized_records: list[dict[str, object]] = []

        for record in records:
            sanitized_records.append(
                {
                    column_name: self._normalize_record_value(value)
                    for column_name, value in record.items()
                }
            )

        return sanitized_records

    def _adapt_value_to_database_column(
        self,
        value: object,
        column_type: TypeEngine,
    ) -> object:
        """Приводит значение к формату колонки PostgreSQL, чтобы списки шли либо как массивы, либо как текст."""
        if value is None:
            return None

        if isinstance(value, list):
            if isinstance(column_type, ARRAY):
                return value
            return json.dumps(value, ensure_ascii=False)

        if pd.isna(value):
            return None

        if isinstance(column_type, ARRAY):
            return [value]

        return value

    def _normalize_record_value(self, value: object) -> object:
        """РџСЂРµРѕР±СЂР°Р·СѓРµС‚ РѕРґРЅРѕ Р·РЅР°С‡РµРЅРёРµ Рє Р±РµР·РѕРїР°СЃРЅРѕРјСѓ РІРёРґСѓ РґР»СЏ psycopg2.

        Р‘РёР·РЅРµСЃ-РїСЂР°РІРёР»Рѕ: РїСѓСЃС‚С‹Рµ РґР°С‚С‹ Рё РїСЂРѕРїСѓС‰РµРЅРЅС‹Рµ РїРѕР»СЏ WB РґРѕР»Р¶РЅС‹ СѓС…РѕРґРёС‚СЊ РІ Р‘Р” РєР°Рє `NULL`,
        Р° РЅРµ РєР°Рє СЃР»СѓР¶РµР±РЅС‹Рµ РјР°СЂРєРµСЂС‹ pandas, РёР·-Р·Р° РєРѕС‚РѕСЂС‹С… РїР°РґР°РµС‚ РјР°СЃСЃРѕРІС‹Р№ upsert.
        """
        if value is None:
            return None

        if isinstance(value, list):
            return [self._normalize_record_value(item) for item in value]

        if isinstance(value, str) and value.strip().lower() == "nat":
            return None

        if pd.isna(value):
            return None

        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()

        return value
