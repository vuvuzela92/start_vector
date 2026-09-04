"""Проверки технического исполнителя PostgreSQL без подключения к СУБД."""

from dataclasses import dataclass, field

from sqlalchemy import select

from src_oop.jobs.database_access_management.executor import PostgreSQLGrantExecutor
from src_oop.jobs.database_access_management.models import (
    AccessGrantRequest,
    AccessGrantStatus,
    DatabaseTargetCreateRequest,
)
from src_oop.jobs.database_access_management.postgresql_adapter import PostgreSQLGrantPlan
from src_oop.jobs.database_access_management.repository import (
    AccessGrantRepository,
    access_grants,
)


@dataclass
class FakeSecretResolver:
    """Подставляет безопасные тестовые значения вместо настоящих секретов."""

    def resolve_database_url(self, secret_ref: str) -> str:
        """Возвращает фиктивный URL, не содержащий настоящих учётных данных."""

        assert secret_ref == "env://postgresql/admin"
        return "postgresql://example"

    def resolve_password(self, secret_ref: str) -> str:
        """Возвращает фиктивный пароль только для проверки передачи значения."""

        assert secret_ref == "env://TEST_HUMAN_PASSWORD"
        return "test-password"


@dataclass
class FakePostgreSQLAdapter:
    """Фиксирует вызовы исполнителя без применения SQL к реальной базе."""

    calls: list[str] = field(default_factory=list)

    def ensure_login_role(self, login_name: str, password: str) -> None:
        """Запоминает создание логина для проверки последовательности сценария."""

        assert login_name == "ivanov"
        assert password == "test-password"
        self.calls.append("login")

    def build_grant_plan(self, request: AccessGrantRequest) -> PostgreSQLGrantPlan:
        """Возвращает минимальный план роли для проверки аудита исполнителя."""

        assert request.principal.login_name == "ivanov"
        self.calls.append("plan")
        return PostgreSQLGrantPlan(role_name="dam_analytics_read_all", statements=())

    def apply_grant_plan(self, plan: PostgreSQLGrantPlan) -> None:
        """Запоминает применение роли, не выполняя SQL-команды."""

        assert plan.role_name == "dam_analytics_read_all"
        self.calls.append("grant")


def test_executor_activates_claimed_postgresql_grant() -> None:
    """Проверяет полный технический путь выдачи доступа PostgreSQL.

    Тест закрепляет бизнес-правило: одно ожидающее распоряжение должно создать
    логин, применить роль и стать `active` без раскрытия пароля в хранилище.
    """

    repository = AccessGrantRepository.from_database_url("sqlite://")
    repository.initialize_schema()
    repository.register_database_target(
        DatabaseTargetCreateRequest(
            target_id="analytics-postgresql-prod",
            display_name="Аналитика PostgreSQL, production",
            engine="postgresql",
            database_name="analytics",
            admin_secret_ref="env://postgresql/admin",
            created_by="petrova",
        )
    )
    grant = repository.create_pending_grant(
        AccessGrantRequest(
            principal={
                "principal_id": "ivanov",
                "principal_type": "human",
                "login_name": "ivanov",
                "display_name": "Иванов Иван",
                "secret_ref": "env://TEST_HUMAN_PASSWORD",
            },
            target_id="analytics-postgresql-prod",
            engine="postgresql",
            level="read_all",
            scope={"database": "analytics"},
            reason="Работа с отчётами",
            requested_by="petrova",
        )
    )
    adapter = FakePostgreSQLAdapter()
    executor = PostgreSQLGrantExecutor(
        repository=repository,
        secret_resolver=FakeSecretResolver(),
        adapter_factory=lambda _: adapter,
    )

    assert executor.execute(grant.id) is True
    assert adapter.calls == ["login", "plan", "grant"]
    with repository.engine.connect() as connection:
        current_status = connection.execute(
            select(access_grants.c.status).where(access_grants.c.id == grant.id)
        ).scalar_one()
    assert current_status == AccessGrantStatus.ACTIVE.value


def test_executor_applies_rights_without_receiving_manual_password() -> None:
    """Проверяет выдачу прав после ручного создания логина руководителем.

    Тест закрепляет правило Telegram-сценария: бот не получает пароль и не
    создаёт пользователя повторно, а применяет только групповые права.
    """

    repository = AccessGrantRepository.from_database_url("sqlite://")
    repository.initialize_schema()
    repository.register_database_target(
        DatabaseTargetCreateRequest(
            target_id="analytics-postgresql-prod",
            display_name="Аналитика PostgreSQL, production",
            engine="postgresql",
            database_name="analytics",
            admin_secret_ref="env://postgresql/admin",
            created_by="petrova",
        )
    )
    grant = repository.create_pending_grant(
        AccessGrantRequest(
            principal={
                "principal_id": "ivanov",
                "principal_type": "human",
                "login_name": "ivanov",
                "display_name": "Иванов Иван",
                "secret_ref": "manual://password",
            },
            target_id="analytics-postgresql-prod",
            engine="postgresql",
            level="read_all",
            scope={"database": "analytics"},
            reason="Работа с отчётами",
            requested_by="petrova",
        )
    )
    adapter = FakePostgreSQLAdapter()
    executor = PostgreSQLGrantExecutor(
        repository=repository,
        secret_resolver=FakeSecretResolver(),
        adapter_factory=lambda _: adapter,
    )

    assert executor.execute_for_existing_login(grant.id) is True
    assert adapter.calls == ["plan", "grant"]
