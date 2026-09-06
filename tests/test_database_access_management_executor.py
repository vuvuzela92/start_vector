"""Проверки технического исполнителя PostgreSQL без подключения к СУБД."""

from dataclasses import dataclass, field

from sqlalchemy import select

from src_oop.jobs.database_access_management.executor import (
    EnvironmentSecretResolver,
    PostgreSQLGrantExecutor,
)
from src_oop.jobs.database_access_management.models import (
    AccessGrantRequest,
    AccessGrantStatus,
    DatabaseTargetCreateRequest,
)
from src_oop.jobs.database_access_management.postgresql_adapter import PostgreSQLGrantPlan
from src_oop.jobs.database_access_management.repository import (
    AccessGrantRepository,
    access_audit_log,
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

    def delete_login_role(self, login_name: str) -> None:
        """Запоминает удаление логина для проверки закрытия связанных доступов."""

        assert login_name == "ivanov"
        self.calls.append("delete")


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


def test_executor_closes_active_grants_after_deleting_login() -> None:
    """Проверяет аудит и закрытие доступов после успешного удаления пользователя.

    Тест защищает сценарий увольнения: служебный журнал не должен показывать
    активный доступ для логина, который уже удалён в PostgreSQL.
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
    repository.claim_pending_grant(grant.id)
    repository.mark_grant_active(grant.id, "analytics_all_schemas_prod_read")
    adapter = FakePostgreSQLAdapter()
    executor = PostgreSQLGrantExecutor(
        repository=repository,
        secret_resolver=FakeSecretResolver(),
        adapter_factory=lambda _: adapter,
    )

    assert executor.delete_user("ivanov") is True
    assert adapter.calls == ["delete"]
    with repository.engine.connect() as connection:
        current_status = connection.execute(
            select(access_grants.c.status).where(access_grants.c.id == grant.id)
        ).scalar_one()
        audit_action = connection.execute(
            select(access_audit_log.c.action)
            .where(
                access_audit_log.c.grant_id == grant.id,
                access_audit_log.c.action == "grant_revoked_user_deleted",
            )
        ).scalar_one()
    assert current_status == AccessGrantStatus.REVOKED.value
    assert audit_action == "grant_revoked_user_deleted"


def test_environment_resolver_uses_fbs_environment_variables(monkeypatch) -> None:
    """Проверяет изолированное разрешение административной ссылки FBS.

    Тест защищает подключение второй базы: ссылка `fbs_admin` должна брать
    только переменные с суффиксом `_FBS`, не смешивая их с настройками служебной
    БД управления доступами.
    """

    monkeypatch.setenv("DB_NAME_FBS", "postgres_test")
    monkeypatch.setenv("DB_USER_FBS", "postgres_test")
    monkeypatch.setenv("DB_PASSWORD_FBS", "test-password")
    monkeypatch.setenv("DB_HOST_FBS", "fbs.example")
    monkeypatch.setenv("DB_PORT_FBS", "5432")

    database_url = EnvironmentSecretResolver().resolve_database_url(
        "env://postgresql/fbs_admin"
    )

    assert database_url.startswith("postgresql://postgres_test:")
    assert "@fbs.example:5432/postgres_test" in database_url
