from sqlalchemy import create_engine, Column, BigInteger, String, Boolean, DateTime, MetaData, Table, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL
import os
from dotenv import load_dotenv

# Загружаем переменные (если еще не загружены)
load_dotenv()

def get_db_engine():
    """Создает и возвращает объект engine для работы с базой данных."""
    
    # Собираем URL подключения через специальный конструктор SQLAlchemy
    # Это надежнее, чем просто склеивать строки
    url_object = URL.create(
        "postgresql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
    )

    # Создаем engine. 
    # pool_pre_ping=True — проверяет "живое" ли соединение перед использованием
    engine = create_engine(url_object, pool_pre_ping=True)
    
    return engine

def sync_data_to_postgres(engine, table_name, data, schema_definition, unique_keys):
    """
    Универсальная функция с поддержкой UniqueConstraint.
    
    :param engine: Объект соединения engine
    :param table_name: Имя таблицы
    :param data: Список словарей с данными
    :param schema_definition: Словарь {имя_поля: тип_данных_sqlalchemy}
    :param unique_keys: Список строк (названия полей для UniqueConstraint)
    """
    # Реестр таблиц
    metadata = MetaData()
    
    # 1. Формируем колонки
    columns = []
    for col_name, col_type in schema_definition.items():
        columns.append(Column(col_name, col_type))

    # 2. Добавляем ограничение уникальности (UniqueConstraint)
    # Это создаст в базе правило: комбинация полей в unique_keys должна быть уникальной
    if unique_keys:
        columns.append(UniqueConstraint(*unique_keys, name=f'uq_{table_name}_keys'))

    # Определяем таблицу
    table = Table(table_name, metadata, *columns)

    # Создаем таблицу, если её нет
    metadata.create_all(engine)

    if not data:
        return

    # 3. Выполнение Upsert
    with engine.begin() as conn:
        stmt = insert(table).values(data)
        
        # Определяем, какие поля обновлять (все, кроме тех, что в UniqueConstraint)
        update_cols = {
            col.name: col 
            for col in stmt.excluded 
            if col.name not in unique_keys
        }

        # Указываем index_elements — те самые поля из UniqueConstraint
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=unique_keys, 
            set_=update_cols
        )

        conn.execute(upsert_stmt)
        print(f"✅ Таблица '{table_name}': успешно синхронизирована по ключам {unique_keys}")

        result = conn.execute(upsert_stmt)
        print(f"✅ Таблица '{table_name}': обработано {len(data)} строк.")

