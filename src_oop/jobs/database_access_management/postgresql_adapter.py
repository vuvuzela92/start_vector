"""Безопасное формирование и применение ролей доступа в PostgreSQL."""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src_oop.jobs.database_access_management.models import (
    AccessGrantRequest,
    AccessLevel,
)

logger = logging.getLogger(__name__)

# PostgreSQL-идентификаторы ограничены безопасным подмножеством, чтобы не
# подставлять введённые пользователем имена объектов в SQL без проверки.
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_SYSTEM_SCHEMAS = frozenset({"information_schema", "pg_catalog", "access_management"})
# Корпоративная техническая роль всегда становится владельцем объектов уволенного пользователя.
_RETIRED_USER_OBJECT_OWNER = "vector_admin"
# Целевая PostgreSQL-база MVP обслуживает production-контур; среда отражается в имени роли.
_ROLE_ENVIRONMENT = "prod"


@dataclass(frozen=True, slots=True)
class PostgreSQLGrantPlan:
    """Набор SQL-команд для одной заявки без выполнения в целевой БД.

    План отделяет проверяемую бизнес-логику назначения прав от сетевого вызова.
    Благодаря этому список будущих действий можно проверить, залогировать в
    безопасном виде и только затем применить в транзакции.
    """

    role_name: str
    statements: tuple[str, ...]


class PostgreSQLAccessAdapter:
    """Применяет утверждённые роли доступа в одной целевой PostgreSQL-базе.

    Адаптер создаёт только групповые роли с `NOLOGIN` и не выдаёт системные
    привилегии PostgreSQL. Пользователь получает доступ через членство в роли,
    поэтому один и тот же набор прав можно безопасно назначить нескольким
    сотрудникам и централизованно отозвать.
    """

    def __init__(self, database_url: str) -> None:
        """Создаёт подключение к целевой PostgreSQL-базе без выполнения SQL.

        URL передаётся из будущего реестра целевых БД и не пишется в логи, так
        как может содержать пароль администратора. Соединение открывается лишь
        при исполнении одобренного плана прав.
        """

        self._engine: Engine = create_engine(database_url, pool_pre_ping=True)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Проверяет и безопасно экранирует имя объекта PostgreSQL.

        Метод защищает основной сценарий выдачи прав от SQL-инъекции: параметры
        SQL нельзя использовать для имён ролей, схем и таблиц, поэтому допускаем
        только согласованный формат идентификатора.
        """

        if not _IDENTIFIER_PATTERN.fullmatch(identifier):
            raise ValueError(
                "Имя объекта PostgreSQL должно содержать только латинские буквы, "
                "цифры и символ подчёркивания и начинаться с буквы или подчёркивания."
            )
        return f'"{identifier}"'

    def build_create_login_statement(self, login_name: str) -> str:
        """Формирует SQL создания логина с явным плейсхолдером пароля.

        Метод обслуживает ручной MVP-сценарий: бот показывает команду, но не
        получает пароль. Администратор заменяет плейсхолдер перед запуском SQL.
        """

        quoted_login = self._quote_identifier(login_name)
        return (
            f"CREATE ROLE {quoted_login} LOGIN NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE INHERIT PASSWORD '<УКАЖИТЕ_ПАРОЛЬ>';"
        )

    @classmethod
    def _build_role_name(cls, request: AccessGrantRequest) -> str:
        """Строит стабильное имя групповой роли для одинаковой области прав.

        Имя отражает базу, схему, production-контур и уровень доступа, например
        `vector_db_public_prod_manage`. Это реализует бизнес-правило понятных
        переиспользуемых ролей: сотрудники с одинаковыми правами получают
        членство в одной роли, а не индивидуальные `GRANT`. Для набора отдельных
        таблиц имена таблиц добавляются в роль; хэш используется только если
        читаемое имя превысило ограничение PostgreSQL в 63 символа.
        """

        access_name = {
            AccessLevel.READ_ALL: "read",
            AccessLevel.WRITE: "write",
            AccessLevel.FULL_ACCESS: "manage",
            AccessLevel.READ_TABLES: "read",
        }[request.level]
        database_part = re.sub(r"[^a-z0-9_]", "_", request.scope.database.lower())
        schema_part = (request.scope.schema_name or "all_schemas").lower()
        role_parts = [database_part, schema_part, _ROLE_ENVIRONMENT, access_name]
        if request.level is AccessLevel.READ_TABLES:
            role_parts.extend(sorted(table_name.lower() for table_name in request.scope.tables))
        readable_name = "_".join(role_parts)
        if len(readable_name) <= 63:
            return readable_name

        scope_key = ":".join(role_parts)
        digest = hashlib.sha256(scope_key.encode("utf-8")).hexdigest()[:12]
        return f"{readable_name[:50]}_{digest}"

    def _list_user_schemas(self) -> tuple[str, ...]:
        """Возвращает прикладные схемы текущей целевой базы.

        Метод нужен для ролей уровня базы: права выдаются на все существующие
        пользовательские схемы, но никогда не на системные `pg_catalog` и
        `information_schema`, служебную `access_management` и временные
        `pg_temp_*` / `pg_toast*`-схемы. Это защищает от выдачи прав на
        краткоживущие объекты технических сессий PostgreSQL.
        Будущие схемы настраиваются отдельным процессом сверки, поскольку их ещё
        нет во время выдачи заявки.
        """

        query = text(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'access_management')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast%'
            ORDER BY schema_name
            """
        )
        with self._engine.connect() as connection:
            schema_names = tuple(row.schema_name for row in connection.execute(query))
        return tuple(
            name
            for name in schema_names
            if name not in _SYSTEM_SCHEMAS
            and not name.startswith("pg_temp_")
            and not name.startswith("pg_toast")
        )

    def list_user_schemas(self) -> tuple[str, ...]:
        """Возвращает схемы, доступные для выбора в интерфейсе руководителя.

        Метод обслуживает форму Telegram-бота и использует тот же фильтр, что
        и планировщик прав: системные и временные схемы никогда не показываются
        как возможная область корпоративного доступа.
        """

        return self._list_user_schemas()

    def build_grant_plan(
        self,
        request: AccessGrantRequest,
        schema_names: Sequence[str] | None = None,
    ) -> PostgreSQLGrantPlan:
        """Формирует идемпотентный план выдачи роли без исполнения команд.

        Для доступа на уровне базы метод получает список существующих схем из
        PostgreSQL либо принимает его извне для тестов. Для чтения, записи и
        управления добавляются default privileges на будущие объекты текущего
        технического администратора; объекты других владельцев требуют такой же
        настройки от их имени.
        """

        if request.engine.value != "postgresql":
            raise ValueError("Адаптер PostgreSQL принимает только заявки PostgreSQL.")

        role_name = self._build_role_name(request)
        quoted_role = self._quote_identifier(role_name)
        quoted_database = self._quote_identifier(request.scope.database)
        quoted_login = self._quote_identifier(request.principal.login_name)

        if request.scope.schema_name:
            resolved_schemas = (request.scope.schema_name,)
        elif schema_names is not None:
            resolved_schemas = tuple(schema_names)
        else:
            resolved_schemas = self._list_user_schemas()

        if not resolved_schemas and request.level is not AccessLevel.FULL_ACCESS:
            raise ValueError("В целевой PostgreSQL-базе не найдены пользовательские схемы.")

        statements = [
            (
                "DO $$ BEGIN "
                f"CREATE ROLE {quoted_role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE; "
                "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
            ),
            f"GRANT CONNECT ON DATABASE {quoted_database} TO {quoted_role}",
        ]

        for schema_name in resolved_schemas:
            quoted_schema = self._quote_identifier(schema_name)
            statements.append(f"GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_role}")

            if request.level is AccessLevel.READ_ALL:
                statements.append(
                    f"GRANT SELECT ON ALL TABLES IN SCHEMA {quoted_schema} TO {quoted_role}"
                )
                statements.append(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA "
                    f"{quoted_schema} GRANT SELECT ON TABLES TO {quoted_role}"
                )
            elif request.level is AccessLevel.WRITE:
                statements.extend(
                    (
                        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {quoted_schema} TO {quoted_role}",
                        f"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {quoted_schema} TO {quoted_role}",
                        "ALTER DEFAULT PRIVILEGES IN SCHEMA "
                        f"{quoted_schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {quoted_role}",
                        "ALTER DEFAULT PRIVILEGES IN SCHEMA "
                        f"{quoted_schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {quoted_role}",
                    )
                )
            elif request.level is AccessLevel.FULL_ACCESS:
                statements.extend(
                    (
                        f"GRANT CREATE ON SCHEMA {quoted_schema} TO {quoted_role}",
                        f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {quoted_schema} TO {quoted_role}",
                        f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {quoted_schema} TO {quoted_role}",
                        "ALTER DEFAULT PRIVILEGES IN SCHEMA "
                        f"{quoted_schema} GRANT ALL PRIVILEGES ON TABLES TO {quoted_role}",
                        "ALTER DEFAULT PRIVILEGES IN SCHEMA "
                        f"{quoted_schema} GRANT ALL PRIVILEGES ON SEQUENCES TO {quoted_role}",
                    )
                )

        if request.level is AccessLevel.READ_TABLES:
            quoted_schema = self._quote_identifier(request.scope.schema_name or "")
            quoted_tables = ", ".join(
                f"{quoted_schema}.{self._quote_identifier(table_name)}"
                for table_name in sorted(request.scope.tables)
            )
            statements.append(f"GRANT SELECT ON TABLE {quoted_tables} TO {quoted_role}")

        statements.append(f"GRANT {quoted_role} TO {quoted_login}")
        return PostgreSQLGrantPlan(role_name=role_name, statements=tuple(statements))

    def apply_grant_plan(self, plan: PostgreSQLGrantPlan) -> None:
        """Применяет подготовленный план выдачи прав в одной транзакции.

        Метод вызывается техническим исполнителем после принятия распоряжения. Если любая команда
        завершается ошибкой, транзакция откатывается и пользователь не получает
        частичный набор прав; вызывающий слой должен перевести заявку в `failed`.
        """

        with self._engine.begin() as connection:
            for statement in plan.statements:
                connection.execute(text(statement))

        logger.info("Права PostgreSQL применены | role_name=%s", plan.role_name)

    def ensure_login_role(self, login_name: str, password: str) -> None:
        """Создаёт или обновляет персональную учётную запись PostgreSQL.

        Метод обслуживает выдачу доступа сотруднику или сервису. Пароль передаётся
        параметром запроса и никогда не включается в SQL-строку или логи; роль
        создаётся без административных привилегий и может получать права только
        через утверждённые групповые роли.
        """

        quoted_login = self._quote_identifier(login_name)
        with self._engine.begin() as connection:
            exists = connection.execute(
                text("SELECT 1 FROM pg_roles WHERE rolname = :login_name"),
                {"login_name": login_name},
            ).scalar_one_or_none()
            if exists is None:
                connection.execute(
                    text(
                        f"CREATE ROLE {quoted_login} LOGIN NOSUPERUSER "
                        "NOCREATEDB NOCREATEROLE INHERIT PASSWORD :password"
                    ),
                    {"password": password},
                )
            else:
                connection.execute(
                    text(f"ALTER ROLE {quoted_login} LOGIN PASSWORD :password"),
                    {"password": password},
                )

        logger.info("Проверена учётная запись PostgreSQL | login_name=%s", login_name)

    def revoke_role_membership(self, login_name: str, role_name: str) -> None:
        """Отзывает у учётной записи членство в технической роли PostgreSQL.

        Метод обслуживает отзыв доступа и не удаляет саму групповую роль: она
        может быть назначена другим сотрудникам и сервисам и остаётся
        переиспользуемой для будущих согласованных заявок с тем же набором прав.
        """

        quoted_login = self._quote_identifier(login_name)
        quoted_role = self._quote_identifier(role_name)
        with self._engine.begin() as connection:
            connection.execute(text(f"REVOKE {quoted_role} FROM {quoted_login}"))

        logger.info(
            "Членство учётной записи в роли PostgreSQL отозвано | login_name=%s | role_name=%s",
            login_name,
            role_name,
        )

    def delete_login_role(self, login_name: str) -> None:
        """Удаляет непривилегированную учётную запись PostgreSQL по логину.

        Метод обслуживает безвозвратное закрытие доступа сотрудника или сервиса.
        Перед удалением все объекты текущей базы передаются корпоративной роли
        `vector_admin`, затем отзываются прямые права логина. Операции выполняются
        одной транзакцией: при невозможности удалить роль передача владения и
        отзыв прав откатываются. Системные и административные роли удалять
        запрещено независимо от команды из Telegram.
        """

        quoted_login = self._quote_identifier(login_name)
        quoted_owner = self._quote_identifier(_RETIRED_USER_OBJECT_OWNER)
        if login_name == _RETIRED_USER_OBJECT_OWNER:
            raise PermissionError("Нельзя удалить корпоративную роль vector_admin.")
        with self._engine.begin() as connection:
            role = connection.execute(
                text(
                    "SELECT rolcanlogin, rolsuper, rolcreaterole, rolcreatedb "
                    "FROM pg_roles WHERE rolname = :login_name"
                ),
                {"login_name": login_name},
            ).mappings().one_or_none()
            if role is None:
                raise LookupError("Пользователь PostgreSQL не найден.")
            if not role["rolcanlogin"]:
                raise ValueError("Можно удалить только учётную запись с возможностью входа.")
            if role["rolsuper"] or role["rolcreaterole"] or role["rolcreatedb"]:
                raise PermissionError("Нельзя удалить привилегированную учётную запись PostgreSQL.")
            owner_exists = connection.execute(
                text("SELECT 1 FROM pg_roles WHERE rolname = :owner_name"),
                {"owner_name": _RETIRED_USER_OBJECT_OWNER},
            ).scalar_one_or_none()
            if owner_exists is None:
                raise LookupError("Корпоративная роль vector_admin не найдена.")
            connection.execute(
                text(f"REASSIGN OWNED BY {quoted_login} TO {quoted_owner}")
            )
            connection.execute(text(f"DROP OWNED BY {quoted_login}"))
            connection.execute(text(f"DROP ROLE {quoted_login}"))

        logger.info(
            "Объекты переданы vector_admin, права отозваны и удалена учётная запись PostgreSQL | login_name=%s",
            login_name,
        )
