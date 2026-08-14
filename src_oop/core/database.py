import logging
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Column, MetaData, Table, UniqueConstraint, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

load_dotenv()

logger = logging.getLogger(__name__)


class Database:
    """Единая точка доступа к PostgreSQL для чтения и batch upsert в проекте."""

    _engine = None
    _SessionFactory = None

    @classmethod
    def get_engine(cls):
        """
        Возвращает singleton engine для подключения к PostgreSQL.

        Метод обслуживает все сценарии проекта, где чтение и запись идут через
        общий слой `Database`. Один engine переиспользуется между задачами, чтобы
        не создавать новые подключения на каждый запрос и сохранять единые
        настройки пула соединений.
        """
        if cls._engine is None:
            url = URL.create(
                drivername="postgresql",
                username=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                database=os.getenv("DB_NAME"),
            )

            cls._engine = create_engine(
                url,
                pool_pre_ping=True,
                pool_size=10,
                max_overflow=20,
                echo=False,
            )

        return cls._engine

    @classmethod
    def get_session(cls):
        """
        Возвращает ORM-сессию, привязанную к общему engine проекта.

        Метод нужен для задач, которые работают через SQLAlchemy session вместо
        прямого `connection.execute`, и сохраняет единый способ инициализации
        подключения к PostgreSQL.
        """
        if cls._SessionFactory is None:
            engine = cls.get_engine()
            cls._SessionFactory = sessionmaker(bind=engine)

        return cls._SessionFactory()

    @classmethod
    def read_sql_to_dataframe(cls, query, params=None):
        """
        Выполняет SQL-запрос и возвращает результат в DataFrame.

        Метод обслуживает сценарии чтения аналитических и справочных данных из
        PostgreSQL, когда следующая стадия обработки выполняется в pandas.
        """
        with cls.get_engine().connect() as connection:
            return pd.read_sql(query, connection, params=params)

    @classmethod
    def read_sql_to_dict(cls, query, params=None):
        """
        Выполняет SQL-запрос и возвращает результат в виде списка словарей.

        Метод нужен для сценариев, где данные читаются из PostgreSQL в
        сериализуемом виде и затем передаются дальше без промежуточного
        DataFrame.
        """
        with cls.get_engine().connect() as connection:
            result = connection.execute(text(query), params or {})
            return [dict(row) for row in result.mappings()]

    @staticmethod
    def _safe_chunk_preview(
        chunk: list[dict],
        preview_columns: list[str],
        max_rows: int = 3,
    ) -> list[dict[str, object]]:
        """
        Возвращает безопасный preview батча для логов записи в PostgreSQL.

        Функция нужна для диагностики batch upsert: показывает только несколько
        строк и ограниченный набор колонок, чтобы при падении соединения можно
        было быстро понять, какой фрагмент данных записывался в БД.
        """
        if not chunk:
            return []

        preview_rows: list[dict[str, object]] = []
        for row in chunk[:max_rows]:
            preview_row: dict[str, object] = {}
            for column_name in preview_columns:
                if column_name not in row:
                    continue
                value = row[column_name]
                if isinstance(value, pd.Timestamp):
                    preview_row[column_name] = str(value)
                elif pd.isna(value):
                    preview_row[column_name] = None
                else:
                    preview_row[column_name] = value
            preview_rows.append(preview_row)
        return preview_rows

    @classmethod
    def sync_data_to_postgres(
        cls,
        table_name,
        data,
        schema_definition,
        unique_keys,
        chunk_size=30000,
    ):
        """
        Синхронизирует данные с PostgreSQL, выполняя batch upsert по unique_keys.

        Метод обслуживает финальный этап загрузки витрин и справочников в БД.
        Данные пишутся батчами, чтобы не перегружать один INSERT большим объёмом
        строк. При ошибке логируется номер батча, диапазон строк и preview данных,
        чтобы можно было точно понять, на каком куске записи оборвался сценарий.
        """
        engine = cls.get_engine()
        metadata = MetaData()

        columns = []
        for col_name, col_type in schema_definition.items():
            columns.append(Column(col_name, col_type))

        if unique_keys:
            columns.append(UniqueConstraint(*unique_keys, name=f"uq_{table_name}_keys"))

        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)

        if data is None or (hasattr(data, "empty") and data.empty):
            logger.info(
                "Синхронизация с PostgreSQL пропущена: нет данных для записи | table=%s",
                table_name,
            )
            return

        if hasattr(data, "to_dict"):
            data_to_insert = data.to_dict(orient="records")
        else:
            data_to_insert = data

        total = len(data_to_insert)
        inserted = 0
        total_batches = (total + chunk_size - 1) // chunk_size if total else 0

        logger.info(
            "Начата batch синхронизация с PostgreSQL | table=%s | total_rows=%s | chunk_size=%s | total_batches=%s | unique_keys=%s",
            table_name,
            total,
            chunk_size,
            total_batches,
            unique_keys,
        )

        with engine.begin() as conn:
            for start_index in range(0, total, chunk_size):
                chunk = data_to_insert[start_index:start_index + chunk_size]
                batch_index = start_index // chunk_size + 1
                row_start = start_index
                row_end = start_index + len(chunk) - 1

                logger.info(
                    "Начата запись batch в PostgreSQL | table=%s | batch_index=%s | total_batches=%s | row_start=%s | row_end=%s | batch_rows=%s | columns=%s",
                    table_name,
                    batch_index,
                    total_batches,
                    row_start,
                    row_end,
                    len(chunk),
                    sorted(chunk[0].keys()) if chunk else [],
                )

                stmt = insert(table).values(chunk)
                available_cols = set(chunk[0].keys()) if chunk else set()
                update_cols = {
                    col.name: getattr(stmt.excluded, col.name)
                    for col in table.c
                    if col.name not in unique_keys and col.name in available_cols
                }

                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=unique_keys,
                    set_=update_cols,
                )

                try:
                    conn.execute(upsert_stmt)
                except Exception as error:
                    logger.exception(
                        "Ошибка записи batch в PostgreSQL | table=%s | batch_index=%s | total_batches=%s | row_start=%s | row_end=%s | batch_rows=%s | error_type=%s | error=%s | preview=%s",
                        table_name,
                        batch_index,
                        total_batches,
                        row_start,
                        row_end,
                        len(chunk),
                        type(error).__name__,
                        error,
                        cls._safe_chunk_preview(
                            chunk,
                            preview_columns=[
                                "date",
                                "article_id",
                                "account",
                                "local_vendor_code",
                                "orders_count",
                                "sales_count",
                                "views",
                                "clicks",
                                "sales_count_rep",
                                "returns_count_rep",
                            ],
                            max_rows=3,
                        ),
                    )
                    raise

                inserted += len(chunk)
                logger.info(
                    "Batch успешно записан в PostgreSQL | table=%s | batch_index=%s | total_batches=%s | inserted_rows=%s | inserted_total=%s",
                    table_name,
                    batch_index,
                    total_batches,
                    len(chunk),
                    inserted,
                )
                print(
                    f"[OK] Успешно {start_index}-{start_index + len(chunk)} из {total} синхронизировано"
                )

        logger.info(
            "Batch синхронизация с PostgreSQL завершена | table=%s | inserted_total=%s | total_rows=%s",
            table_name,
            inserted,
            total,
        )
        print(f" Таблица '{table_name}': успешно синхронизирована")
        print(f" Всего обработано {inserted} строк.")
