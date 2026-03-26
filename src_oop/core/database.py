import os
from sqlalchemy import create_engine, Column, MetaData, Table, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


class Database:
    _engine = None
    _SessionFactory = None

    @classmethod
    def get_engine(cls):
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
        if cls._SessionFactory is None:
            engine = cls.get_engine()
            cls._SessionFactory = sessionmaker(bind=engine)

        return cls._SessionFactory()
    
    @classmethod
    def read_sql_to_dataframe(cls, query, params=None):
        with cls.get_engine().connect() as connection:
            return pd.read_sql(query, connection, params=params)

    @classmethod
    def sync_data_to_postgres(cls, table_name, data, schema_definition, unique_keys, chunk_size=30000):
        engine = cls.get_engine()
        metadata = MetaData()

        columns = []
        for col_name, col_type in schema_definition.items():
            columns.append(Column(col_name, col_type))

        if unique_keys:
            columns.append(UniqueConstraint(*unique_keys, name=f"uq_{table_name}_keys"))

        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)

        if data is None or data.empty:
            return

        if hasattr(data, "to_dict"):
            data_to_insert = data.to_dict(orient="records")
        else:
            data_to_insert = data

        # 🔥 РАЗБИВАЕМ НА ЧАНКИ
        total = len(data_to_insert)
        inserted = 0
        
        with engine.begin() as conn:
            for i in range(0, total, chunk_size):
                chunk = data_to_insert[i:i + chunk_size]
                
                stmt = insert(table).values(chunk)
                
                # update_cols = {
                #     col.name: col
                #     for col in stmt.excluded
                #     if col.name not in unique_keys
                # }
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

                conn.execute(upsert_stmt)
                inserted += len(chunk)
                print(f"[OK] Успешно {i}-{i+len(chunk)} из {total} синхронизировано")
        
        print(f" Таблица '{table_name}': успешно синхронизирована")
        print(f" Всего обработано {inserted} строк.")
