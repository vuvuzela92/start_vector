import os
# главная точка подключения SQLAlchemy к базе
from sqlalchemy import create_engine, Column, MetaData, Table, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
# URL — специальный конструктор строки подключения.
from sqlalchemy.engine import URL
# создаёт фабрику сессий
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()


class Database:
    """Класс для управления подключением к базе данных."""
    _engine = None
    _SessionFactory = None

    @classmethod
    def get_engine(cls):
        """Возвращает singleton engine."""

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
                # Проверяет состояние соединения. Если отключено, подключается автоматически
                pool_pre_ping=True,
                # Размер пула соединений
                pool_size=10,
                # Если пул переполнен, можно открыть ещё 20 временных соединений.
                max_overflow=20,
                # Параметр отслеживания. Если поставить True, SQLAlchemy будет писать в лог все SQL запросы.
                echo=False
            )

        return cls._engine


    @classmethod
    def get_session(cls):
        """Возвращает новую сессию БД."""

        # Если фабрика ещё не создана — создаём.
        if cls._SessionFactory is None:
            # Получаем engine
            engine = cls.get_engine()
            # Создает сессию
            cls._SessionFactory = sessionmaker(bind=engine)

        return cls._SessionFactory()
    


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

        if data is None or data.empty:
            return
        
        # Если data — это DataFrame, конвертируем в список словарей
        if hasattr(data, "to_dict"):
            data_to_insert = data.to_dict(orient="records")
        else:
            data_to_insert = data

        # 3. Выполнение Upsert
        with engine.begin() as conn:
            stmt = insert(table).values(data_to_insert)
            
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
            print(f"✅ Таблица '{table_name}': успешно синхронизирована по ключам {unique_keys}")
            print(f"✅ Таблица '{table_name}': обработано {len(data_to_insert)} строк.")