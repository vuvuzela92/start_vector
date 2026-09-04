"""Техническое применение распоряжений о доступе к PostgreSQL."""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.exc import SQLAlchemyError

from src_oop.jobs.database_access_management.config import DatabaseAccessManagementSettings
from src_oop.jobs.database_access_management.models import DatabaseEngine
from src_oop.jobs.database_access_management.postgresql_adapter import PostgreSQLAccessAdapter
from src_oop.jobs.database_access_management.repository import AccessGrantRepository

logger = logging.getLogger(__name__)

_ENVIRONMENT_SECRET_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")


class SecretResolver(Protocol):
    """Получает секреты по ссылкам, не раскрывая их вызывающему интерфейсу."""

    def resolve_database_url(self, secret_ref: str) -> str:
        """Возвращает URL административного подключения по безопасной ссылке."""

    def resolve_password(self, secret_ref: str) -> str:
        """Возвращает пароль учётной записи по безопасной ссылке."""


@dataclass(frozen=True, slots=True)
class EnvironmentSecretResolver:
    """Временный резолвер секретов из окружения до подключения Vault.

    Резолвер обслуживает MVP: `env://postgresql/admin` использует существующие
    `DB_*` настройки, а `env://ИМЯ_ПЕРЕМЕННОЙ` читает пароль только из окружения
    процесса. Значение секрета не попадает в логи или исключения.
    """

    def resolve_database_url(self, secret_ref: str) -> str:
        """Возвращает URL администратора PostgreSQL для зарегистрированной цели."""

        if secret_ref != "env://postgresql/admin":
            raise ValueError("Для PostgreSQL MVP поддерживается env://postgresql/admin.")
        return DatabaseAccessManagementSettings.from_env().database_url

    def resolve_password(self, secret_ref: str) -> str:
        """Возвращает пароль из явно разрешённой переменной окружения.

        Метод защищает выдачу доступа от передачи пароля через Telegram или API:
        допустимо только имя переменной в формате `env://ИМЯ_ПЕРЕМЕННОЙ`.
        """

        if not secret_ref.startswith("env://"):
            raise ValueError("Для MVP ссылка на пароль должна иметь формат env://ИМЯ_ПЕРЕМЕННОЙ.")
        variable_name = secret_ref.removeprefix("env://")
        if not _ENVIRONMENT_SECRET_PATTERN.fullmatch(variable_name):
            raise ValueError("Ссылка на пароль содержит недопустимое имя переменной.")
        password = os.getenv(variable_name, "")
        if not password:
            raise ValueError("Переменная с паролем учётной записи не задана.")
        return password


@dataclass(slots=True)
class PostgreSQLGrantExecutor:
    """Применяет одно распоряжение PostgreSQL с фиксацией результата в аудите."""

    repository: AccessGrantRepository
    secret_resolver: SecretResolver
    adapter_factory: Callable[[str], PostgreSQLAccessAdapter] = PostgreSQLAccessAdapter

    def execute(self, grant_id: str) -> bool:
        """Создаёт логин, выдаёт роль и завершает распоряжение.

        Метод обслуживает очередь технических работ: атомарно захватывает
        `pending`-распоряжение, применяет его только для PostgreSQL и переводит
        в `active` либо `failed`. Повторный запуск не повторяет уже захваченную
        работу, а детали ошибок не содержат секретов или строк подключения.
        """

        claimed_grant = self.repository.claim_pending_grant(grant_id)
        if claimed_grant is None:
            return False

        request, admin_secret_ref = claimed_grant
        try:
            if request.engine is not DatabaseEngine.POSTGRESQL:
                raise ValueError("Исполнитель PostgreSQL получил распоряжение другой СУБД.")
            if request.principal.secret_ref is None:
                raise ValueError("Для учётной записи отсутствует ссылка на пароль.")
            database_url = self.secret_resolver.resolve_database_url(admin_secret_ref)
            password = self.secret_resolver.resolve_password(request.principal.secret_ref)
            adapter = self.adapter_factory(database_url)
            adapter.ensure_login_role(request.principal.login_name, password)
            plan = adapter.build_grant_plan(request)
            adapter.apply_grant_plan(plan)
            self.repository.mark_grant_active(grant_id, plan.role_name)
        except (OSError, RuntimeError, ValueError) as error:
            failure_reason = "Не удалось применить доступ PostgreSQL."
            self.repository.mark_grant_failed(grant_id, failure_reason)
            logger.error(
                "Техническое применение доступа PostgreSQL завершилось ошибкой | "
                "grant_id=%s | error_type=%s",
                grant_id,
                type(error).__name__,
            )
            return False

        logger.info("Технически применён доступ PostgreSQL | grant_id=%s", grant_id)
        return True

    def execute_for_existing_login(self, grant_id: str) -> bool:
        """Выдаёт роль уже созданной вручную учётной записи PostgreSQL.

        Метод обслуживает Telegram-сценарий без передачи пароля боту: руководитель
        создаёт логин в защищённом SQL-клиенте, после чего сервис применяет только
        групповую роль и права от технического администратора. Если логина нет,
        PostgreSQL отклонит назначение роли, а распоряжение будет зафиксировано
        как неуспешное без раскрытия параметров подключения.
        """

        claimed_grant = self.repository.claim_pending_grant(grant_id)
        if claimed_grant is None:
            return False

        request, admin_secret_ref = claimed_grant
        try:
            if request.engine is not DatabaseEngine.POSTGRESQL:
                raise ValueError("Исполнитель PostgreSQL получил распоряжение другой СУБД.")
            database_url = self.secret_resolver.resolve_database_url(admin_secret_ref)
            adapter = self.adapter_factory(database_url)
            plan = adapter.build_grant_plan(request)
            adapter.apply_grant_plan(plan)
            self.repository.mark_grant_active(grant_id, plan.role_name)
        except (OSError, RuntimeError, ValueError, SQLAlchemyError) as error:
            self.repository.mark_grant_failed(grant_id, "Не удалось применить права PostgreSQL.")
            logger.error(
                "Техническая выдача прав существующему логину завершилась ошибкой | "
                "grant_id=%s | error_type=%s",
                grant_id,
                type(error).__name__,
            )
            return False

        logger.info("Права назначены существующему логину PostgreSQL | grant_id=%s", grant_id)
        return True

    def revoke(self, grant_id: str) -> bool:
        """Отзывает роль PostgreSQL у одного логина без удаления групповой роли.

        Метод обслуживает команду руководителя «забрать доступ». Он захватывает
        только активное распоряжение, поэтому повторный вызов не выполнит SQL
        повторно и не затронет других участников той же роли.
        """

        claimed_grant = self.repository.claim_active_grant_for_revocation(grant_id)
        if claimed_grant is None:
            return False

        request, admin_secret_ref = claimed_grant
        try:
            if request.engine is not DatabaseEngine.POSTGRESQL:
                raise ValueError("Исполнитель PostgreSQL получил распоряжение другой СУБД.")
            database_url = self.secret_resolver.resolve_database_url(admin_secret_ref)
            adapter = self.adapter_factory(database_url)
            role_name = self.repository.get_applied_role_name(grant_id)
            if role_name is None:
                role_name = adapter._build_role_name(request)
            adapter.revoke_role_membership(request.principal.login_name, role_name)
            self.repository.mark_grant_revoked(grant_id, role_name)
        except Exception as error:
            self.repository.mark_grant_failed(grant_id, "Не удалось отозвать доступ PostgreSQL.")
            logger.error(
                "Технический отзыв доступа PostgreSQL завершился ошибкой | "
                "grant_id=%s | error_type=%s",
                grant_id,
                type(error).__name__,
            )
            return False

        logger.info("Технически отозван доступ PostgreSQL | grant_id=%s", grant_id)
        return True

    def delete_user(self, login_name: str) -> bool:
        """Удаляет непривилегированного пользователя PostgreSQL по логину.

        Метод обслуживает команду руководителя удаления учётной записи. Перед
        `DROP ROLE` используется только административное подключение из
        окружения; пароль пользователя, его секретная ссылка и данные объектов
        не читаются и не попадают в журнал.
        """

        try:
            database_url = self.secret_resolver.resolve_database_url("env://postgresql/admin")
            adapter = self.adapter_factory(database_url)
            adapter.delete_login_role(login_name)
        except Exception as error:
            logger.error(
                "Удаление учётной записи PostgreSQL завершилось ошибкой | "
                "login_name=%s | error_type=%s",
                login_name,
                type(error).__name__,
            )
            return False
        return True
