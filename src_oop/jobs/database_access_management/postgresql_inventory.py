"""Read-only инвентаризация существующих ролевых доступов PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import create_engine, text


@dataclass(frozen=True, slots=True)
class ExistingPostgreSQLAccess:
    """Описывает прикладной логин и его необязательное членство в роли."""

    login_name: str
    role_name: str | None


class PostgreSQLAccessInventory:
    """Читает существующие доступы, созданные до внедрения сервиса."""

    def __init__(self, database_url: str) -> None:
        """Подготавливает read-only подключение к целевой PostgreSQL-базе."""

        self._engine = create_engine(database_url, pool_pre_ping=True)

    def list_users_and_role_memberships(self) -> list[ExistingPostgreSQLAccess]:
        """Возвращает все прикладные логины и назначенные им роли без изменения прав.

        Метод обслуживает первичную инвентаризацию: включает пользователей без
        ролей, исключает системные роли PostgreSQL и не запрашивает пароли, хэши
        или строки подключений.
        """

        query = text(
            """
            SELECT member_role.rolname AS login_name, granted_role.rolname AS role_name
            FROM pg_roles member_role
            LEFT JOIN pg_auth_members membership ON membership.member = member_role.oid
            LEFT JOIN pg_roles granted_role ON granted_role.oid = membership.roleid
                AND granted_role.rolname NOT LIKE 'pg_%'
            WHERE member_role.rolcanlogin
              AND member_role.rolname NOT LIKE 'pg_%'
            ORDER BY member_role.rolname, granted_role.rolname NULLS FIRST
            """
        )
        with self._engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [
            ExistingPostgreSQLAccess(
                login_name=row["login_name"],
                role_name=row["role_name"],
            )
            for row in rows
        ]
