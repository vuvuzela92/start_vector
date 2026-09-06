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
            database_url = cls.build_database_url_from_environment()

        schema_name = os.getenv("DATABASE_ACCESS_MANAGEMENT_SCHEMA", "access_management")
        schema_name = schema_name.strip()
        if not _SCHEMA_NAME_PATTERN.fullmatch(schema_name):
            raise ValueError(
                "Переменная DATABASE_ACCESS_MANAGEMENT_SCHEMA должна содержать "
                "только строчные латинские буквы, цифры и символ подчёркивания."
            )
        return cls(database_url=database_url, schema_name=schema_name)

    @staticmethod
    def build_database_url_from_environment(variable_suffix: str = "") -> str:
        """Собирает URL PostgreSQL из согласованного набора переменных окружения.

        Функция обслуживает служебную БД без суффикса и зарегистрированные цели
        с суффиксами, например `_FBS`. Она не пишет URL и пароль в логи или
        исключения: при неполной конфигурации сообщает только имена отсутствующих
        переменных. Суффикс принимает только заглавные латинские буквы, цифры и
        подчёркивания, чтобы не читать произвольные переменные процесса.
        """

        load_dotenv()
        if variable_suffix and not re.fullmatch(r"_[A-Z0-9_]{1,64}", variable_suffix):
            raise ValueError("Суффикс переменных PostgreSQL содержит недопустимые символы.")

        variable_names = {
            "database_name": f"DB_NAME{variable_suffix}",
            "user": f"DB_USER{variable_suffix}",
            "password": f"DB_PASSWORD{variable_suffix}",
            "host": f"DB_HOST{variable_suffix}",
            "port": f"DB_PORT{variable_suffix}",
        }
        database_name = os.getenv(variable_names["database_name"], "").strip()
        user = os.getenv(variable_names["user"], "").strip()
        password = os.getenv(variable_names["password"], "").strip()
        host = os.getenv(variable_names["host"], "").strip()
        port_raw = os.getenv(variable_names["port"], "").strip()
        missing_names = [
            name
            for name, value in (
                (variable_names["database_name"], database_name),
                (variable_names["user"], user),
                (variable_names["password"], password),
                (variable_names["host"], host),
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
            raise ValueError(
                f"Переменная {variable_names['port']} должна содержать номер порта."
            ) from error

        return URL.create(
            drivername="postgresql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database_name,
        ).render_as_string(hide_password=False)
