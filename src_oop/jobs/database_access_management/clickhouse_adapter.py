"""Безопасное формирование ролей доступа в ClickHouse."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from src_oop.jobs.database_access_management.models import AccessGrantRequest, AccessLevel

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,126}$")


@dataclass(frozen=True, slots=True)
class ClickHouseGrantPlan:
    """Хранит подготовленные команды ClickHouse для одной роли доступа."""

    role_name: str
    statements: tuple[str, ...]


class ClickHouseAccessAdapter:
    """Строит ограниченные RBAC-команды ClickHouse без выполнения SQL."""

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Проверяет имя объекта, защищая выдачу прав от SQL-инъекции."""

        if not _IDENTIFIER_PATTERN.fullmatch(identifier):
            raise ValueError("Имя объекта ClickHouse содержит недопустимые символы.")
        return f"`{identifier}`"

    @classmethod
    def build_grant_plan(cls, request: AccessGrantRequest) -> ClickHouseGrantPlan:
        """Создаёт переиспользуемую роль с утверждённой областью прав.

        Метод обслуживает выдачу доступа ClickHouse: роль не получает прав на
        системные объекты, а логин получает только членство в ней.
        """

        if request.engine.value != "clickhouse":
            raise ValueError("Адаптер ClickHouse принимает только заявки ClickHouse.")
        scope_key = ":".join(
            (request.scope.database, request.scope.schema_name or "", *sorted(request.scope.tables), request.level.value)
        )
        digest = hashlib.sha256(scope_key.encode("utf-8")).hexdigest()[:12]
        role_name = f"dam_{request.scope.database}_{request.level.value}_{digest}"[:128]
        quoted_role = cls._quote_identifier(role_name)
        quoted_database = cls._quote_identifier(request.scope.database)
        quoted_login = cls._quote_identifier(request.principal.login_name)
        statements = [f"CREATE ROLE IF NOT EXISTS {quoted_role}"]
        if request.level is AccessLevel.READ_TABLES:
            quoted_tables = ", ".join(
                f"{quoted_database}.{cls._quote_identifier(table_name)}"
                for table_name in sorted(request.scope.tables)
            )
            statements.append(f"GRANT SELECT ON {quoted_tables} TO {quoted_role}")
        elif request.level is AccessLevel.READ_ALL:
            statements.append(f"GRANT SELECT ON {quoted_database}.* TO {quoted_role}")
        else:
            statements.append(f"GRANT ALL ON {quoted_database}.* TO {quoted_role}")
        statements.append(f"GRANT {quoted_role} TO {quoted_login}")
        return ClickHouseGrantPlan(role_name=role_name, statements=tuple(statements))
