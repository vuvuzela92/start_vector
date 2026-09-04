"""Проверки прямых распоряжений руководителя и неизменяемого аудита."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from src_oop.jobs.database_access_management.models import (
    AccessGrantRequest,
    AccessGrantStatus,
    DatabaseTargetCreateRequest,
)
from src_oop.jobs.database_access_management.repository import (
    AccessGrantRepository,
    access_audit_log,
    access_grants,
)


def create_repository_with_grant() -> tuple[AccessGrantRepository, str]:
    """Создаёт временное хранилище с новым распоряжением для проверки аудита.

    Вспомогательная функция изолирует бизнес-проверки от внешней PostgreSQL и
    гарантирует, что каждый тест начинает работу с одним распоряжением `pending`.
    """

    repository = AccessGrantRepository.from_database_url("sqlite://")
    repository.initialize_schema()
    repository.register_database_target(
        DatabaseTargetCreateRequest(
            target_id="analytics-postgresql-prod",
            display_name="Аналитика PostgreSQL, production",
            engine="postgresql",
            database_name="analytics",
            admin_secret_ref="vault://database-access/analytics/postgresql",
            created_by="petrova",
        )
    )
    request = AccessGrantRequest(
        principal={
            "principal_id": "ivanov",
            "principal_type": "human",
            "login_name": "ivanov",
            "display_name": "Иванов Иван",
            "secret_ref": "env://TEST_HUMAN_PASSWORD",
        },
        target_id="analytics-postgresql-prod",
        engine="postgresql",
        level="read_tables",
        scope={"database": "analytics", "schema_name": "public", "tables": ["orders"]},
        expires_at=datetime(2026, 10, 1, tzinfo=UTC),
        reason="Подготовка отчёта",
        requested_by="petrova",
    )
    grant = repository.create_pending_grant(request)
    return repository, grant.id


def test_service_principal_requires_operational_metadata() -> None:
    """Проверяет обязательные реквизиты сервисной учётной записи.

    Тест защищает аудит и ротацию пароля: без среды, команды-владельца и ссылки
    на секрет нельзя создать обезличенную сервисную учётную запись.
    """

    with pytest.raises(ValueError, match="обязательны сервис, среда"):
        AccessGrantRequest(
            principal={
                "principal_id": "orders",
                "principal_type": "service",
                "login_name": "svc_orders_prod",
                "display_name": "Orders production",
                "service_name": "orders",
                "secret_ref": "env://TEST_SERVICE_PASSWORD",
            },
            target_id="analytics-postgresql-prod",
            engine="postgresql",
            level="read_all",
            scope={"database": "analytics"},
            reason="Чтение витрины заказов",
            requested_by="petrova",
        )


def test_repository_stores_service_principal_without_secret_in_audit() -> None:
    """Проверяет учёт сервисного доступа без раскрытия ссылки на секрет в аудите.

    Ссылка на секрет нужна для операционного сопровождения и хранится в заявке,
    но не попадает в неизменяемый журнал действий, который обычно доступен более
    широкому кругу аудиторов.
    """

    repository = AccessGrantRepository.from_database_url("sqlite://")
    repository.initialize_schema()
    repository.register_database_target(
        DatabaseTargetCreateRequest(
            target_id="analytics-postgresql-prod",
            display_name="Аналитика PostgreSQL, production",
            engine="postgresql",
            database_name="analytics",
            admin_secret_ref="vault://database-access/analytics/postgresql",
            created_by="petrova",
        )
    )
    secret_ref = "vault://database-access/orders/prod/postgresql"
    grant = repository.create_pending_grant(
        AccessGrantRequest(
            principal={
                "principal_id": "orders-prod",
                "principal_type": "service",
                "login_name": "svc_orders_prod",
                "display_name": "Orders production",
                "service_name": "orders",
                "environment": "prod",
                "owner_team": "commerce",
                "secret_ref": secret_ref,
            },
            target_id="analytics-postgresql-prod",
            engine="postgresql",
            level="read_all",
            scope={"database": "analytics"},
            reason="Чтение витрины заказов",
            requested_by="petrova",
        )
    )

    with repository.engine.connect() as connection:
        grant_row = connection.execute(
            select(
                access_grants.c.principal_type,
                access_grants.c.login_name,
                access_grants.c.secret_ref,
            ).where(access_grants.c.id == grant.id)
        ).mappings().one()
        audit_details = connection.execute(
            select(access_audit_log.c.details).where(
                access_audit_log.c.grant_id == grant.id
            )
        ).scalar_one()

    assert grant_row["principal_type"] == "service"
    assert grant_row["login_name"] == "svc_orders_prod"
    assert grant_row["secret_ref"] == secret_ref
    assert "secret_ref" not in audit_details


def test_grant_requires_registered_matching_target() -> None:
    """Проверяет, что распоряжение нельзя направить в произвольную базу.

    Тест защищает основной сценарий Telegram-бота: менеджер выбирает цель из
    реестра, а не передаёт имя базы, в которой сервис не должен управлять
    доступами.
    """

    repository = AccessGrantRepository.from_database_url("sqlite://")
    repository.initialize_schema()
    request = AccessGrantRequest(
        principal={
            "principal_id": "ivanov",
            "principal_type": "human",
            "login_name": "ivanov",
            "display_name": "Иванов Иван",
            "secret_ref": "env://TEST_HUMAN_PASSWORD",
        },
        target_id="unknown-target",
        engine="postgresql",
        level="read_all",
        scope={"database": "analytics"},
        reason="Работа с отчётами",
        requested_by="petrova",
    )

    with pytest.raises(LookupError, match="не зарегистрирована"):
        repository.create_pending_grant(request)


def test_manager_request_is_queued_without_second_approval() -> None:
    """Проверяет прямую постановку распоряжения в техническую очередь.

    Тест закрепляет выбранный пользовательский путь: руководитель не ожидает
    решения другого руководителя, а созданное им распоряжение сразу доступно
    техническому исполнителю.
    """

    repository, grant_id = create_repository_with_grant()

    with repository.engine.connect() as connection:
        current_status = connection.execute(
            select(access_grants.c.status).where(access_grants.c.id == grant_id)
        ).scalar_one()
        actions = connection.execute(
            select(access_audit_log.c.action)
            .where(access_audit_log.c.grant_id == grant_id)
            .order_by(access_audit_log.c.created_at)
        ).scalars().all()
    assert current_status == AccessGrantStatus.PENDING.value
    assert actions == ["grant_requested"]


def test_pending_grant_can_be_cancelled_before_sql_execution() -> None:
    """Проверяет отмену формы до ручного выполнения SQL-команд.

    Тест защищает интерфейсный сценарий «Вернуться назад»: незавершённая заявка
    не должна остаться в очереди и быть ошибочно применена позднее.
    """

    repository, grant_id = create_repository_with_grant()

    cancelled = repository.cancel_pending_grant(grant_id, actor_id="petrova")

    with repository.engine.connect() as connection:
        status = connection.execute(
            select(access_grants.c.status).where(access_grants.c.id == grant_id)
        ).scalar_one()
    assert cancelled is True
    assert status == AccessGrantStatus.CANCELLED.value


def test_repository_returns_role_name_recorded_at_activation() -> None:
    """Проверяет получение исторического имени роли для безопасного отзыва.

    Тест защищает действующие доступы после изменения формата именования: отзыв
    должен использовать роль, реально назначенную при выдаче, а не новое имя.
    """

    repository, grant_id = create_repository_with_grant()
    claimed = repository.claim_pending_grant(grant_id)
    assert claimed is not None
    repository.mark_grant_active(grant_id, "dam_legacy_role")

    assert repository.get_applied_role_name(grant_id) == "dam_legacy_role"
