"""Конфигурация служебной базы сервиса управления доступами."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from dotenv import load_dotenv
from sqlalchemy.engine import URL

_SCHEMA_NAME_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


@dataclass(frozen=True, slots=True)
class DatabaseAccessManagementSettings:
    """Настройки из окружения для хранения заявок и аудита.

    Сервис использует отдельную служебную базу, чтобы бизнес-данные целевых БД
    не смешивались с историей выдачи прав. Строка подключения не записывается
    в логи, поскольку содержит чувствительные данные.
    """

    database_url: str
    schema_name: str

    @classmethod
    def from_env(cls) -> DatabaseAccessManagementSettings:
        """Собирает обязательную строку подключения к служебной PostgreSQL-базе.

        При наличии отдельной строки подключения она имеет приоритет. Для MVP
        допускается использование существующих `DB_*` переменных проекта: в этом
        случае служебные таблицы будут изолированы в отдельной схеме, а не
        смешаны с бизнес-таблицами в `public`.
        """

        load_dotenv()
        database_url = os.getenv("DATABASE_ACCESS_MANAGEMENT_DATABASE_URL", "").strip()
        if not database_url:
            database_url = cls._build_database_url_from_project_environment()

        schema_name = os.getenv("DATABASE_ACCESS_MANAGEMENT_SCHEMA", "access_management")
        schema_name = schema_name.strip()
        if not _SCHEMA_NAME_PATTERN.fullmatch(schema_name):
            raise ValueError(
                "Переменная DATABASE_ACCESS_MANAGEMENT_SCHEMA должна содержать "
                "только строчные латинские буквы, цифры и символ подчёркивания."
            )
        return cls(database_url=database_url, schema_name=schema_name)

    @staticmethod
    def _build_database_url_from_project_environment() -> str:
        """Собирает URL служебного хранилища из существующих `DB_*` переменных.

        Функция обслуживает MVP с отдельной схемой в `vector_db`. Она не пишет
        URL и пароль в логи или исключения: при неполной конфигурации сообщает
        только имена отсутствующих переменных.
        """

        database_name = os.getenv("DB_NAME", "").strip()
        user = os.getenv("DB_USER", "").strip()
        password = os.getenv("DB_PASSWORD", "").strip()
        host = os.getenv("DB_HOST", "").strip()
        port_raw = os.getenv("DB_PORT", "").strip()
        missing_names = [
            name
            for name, value in (
                ("DB_NAME", database_name),
                ("DB_USER", user),
                ("DB_PASSWORD", password),
                ("DB_HOST", host),
            )
            if not value
        ]
        if missing_names:
            raise ValueError(
                "Не заданы обязательные переменные окружения PostgreSQL: "
                f"{', '.join(missing_names)}"
            )

        try:
            port = int(port_raw) if port_raw else 5432
        except ValueError as error:
            raise ValueError("Переменная DB_PORT должна содержать номер порта.") from error

        return URL.create(
            drivername="postgresql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database_name,
        ).render_as_string(hide_password=False)
