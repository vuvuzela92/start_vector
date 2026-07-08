from __future__ import annotations
"""Минимальная проектная обёртка для чтения данных из ClickHouse.

Этот модуль нужен для unit-джобы конкурентов. Исторически соответствующий
скрипт читал данные из ClickHouse, а в `src_oop` своей обёртки над
ClickHouse раньше не было.

Назначение модуля намеренно узкое:
- собрать настройки из окружения;
- создать `clickhouse_driver.Client`;
- выполнить запрос;
- вернуть `pandas.DataFrame`.

Мы сознательно не превращаем его в большой универсальный слой доступа к данным,
потому что текущей задаче нужен только read-only сценарий.
"""

import logging
import os
from dataclasses import dataclass

import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ClickHouseSettings:
    """Настройки подключения к ClickHouse.

    Поля соответствуют уже существующим переменным окружения проекта, чтобы не
    вводить новый формат конфигурации и не дублировать секреты в коде.
    """

    host: str
    user: str
    password: str
    database: str
    port: int = 9000

    @classmethod
    def from_env(cls) -> "ClickHouseSettings":
        """Собирает настройки подключения из переменных окружения.

        Обязательные переменные:
        - `CLICKHOUSE_HOST`
        - `CLICKHOUSE_ADMIN_USER`
        - `CLICKHOUSE_ADMIN_PASSWORD`
        - `CLICKHOUSE_DB`

        Необязательная:
        - `CLICKHOUSE_PORT` (по умолчанию `9000`)

        Если хотя бы одной обязательной переменной нет, падаем сразу с явной
        ошибкой. Это удобнее для отладки, чем получать сетевой сбой уже в
        момент выполнения SQL.
        """

        host = os.getenv("CLICKHOUSE_HOST", "").strip()
        user = os.getenv("CLICKHOUSE_ADMIN_USER", "").strip()
        password = os.getenv("CLICKHOUSE_ADMIN_PASSWORD", "").strip()
        database = os.getenv("CLICKHOUSE_DB", "").strip()
        port_raw = os.getenv("CLICKHOUSE_PORT", "").strip()

        missing = [
            name
            for name, value in (
                ("CLICKHOUSE_HOST", host),
                ("CLICKHOUSE_ADMIN_USER", user),
                ("CLICKHOUSE_ADMIN_PASSWORD", password),
                ("CLICKHOUSE_DB", database),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "Не заданы обязательные переменные окружения ClickHouse: "
                f"{', '.join(missing)}"
            )

        port = int(port_raw) if port_raw else 9000
        return cls(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
        )


class ClickHouseDatabase:
    """Тонкий read-only клиент для ClickHouse.

    Класс делает только то, что требуется текущей джобе:
    - создаёт клиент;
    - выполняет запрос;
    - возвращает результат в виде `DataFrame`.
    """

    def __init__(self, settings: ClickHouseSettings | None = None) -> None:
        self.settings = settings or ClickHouseSettings.from_env()

    def _create_client(self) -> Client:
        """Создаёт новый клиент ClickHouse.

        Для пакетной джобы это простой и достаточно надёжный подход:
        запросов немного, а жизненный цикл соединения полностью прозрачен.
        """

        return Client(
            host=self.settings.host,
            user=self.settings.user,
            password=self.settings.password,
            database=self.settings.database,
            port=self.settings.port,
        )

    def read_sql_to_dataframe(self, query: str) -> pd.DataFrame:
        """Выполняет SQL и возвращает результат как `pandas.DataFrame`.

        Используем `with_column_types=True`, чтобы получить имена колонок из
        ответа ClickHouse. Соединение закрываем в `finally`, даже если запрос
        завершился ошибкой.
        """

        client = self._create_client()
        try:
            result, meta = client.execute(query, with_column_types=True)
        finally:
            client.disconnect()

        columns = [column_name for column_name, _ in meta]
        dataframe = pd.DataFrame(result, columns=columns)
        logger.info(
            "ClickHouse query loaded: rows=%s, columns=%s",
            len(dataframe),
            columns,
        )
        return dataframe
