import os
# главная точка подключения SQLAlchemy к базе
from sqlalchemy import create_engine
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