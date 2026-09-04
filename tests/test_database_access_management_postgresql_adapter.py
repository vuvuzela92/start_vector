"""Проверки построения планов прав для PostgreSQL без подключения к БД."""

from datetime import UTC, datetime

import pytest

from src_oop.jobs.database_access_management.models import AccessGrantRequest
from src_oop.jobs.database_access_management.postgresql_adapter import PostgreSQLAccessAdapter


def build_request(
    level: str,
    scope: dict[str, object],
) -> AccessGrantRequest:
    """Создаёт заявку для проверки бизнес-правил планировщика PostgreSQL.

    Вспомогательная функция защищает тестовый сценарий от дублирования полей
    заявки и позволяет явно проверять, какие SQL-права соответствуют каждому
    утверждённому уровню доступа.
    """

    return AccessGrantRequest(
        principal={
            "principal_id": "ivanov",
            "principal_type": "human",
            "login_name": "ivanov",
            "display_name": "Иванов Иван",
            "secret_ref": "env://TEST_HUMAN_PASSWORD",
        },
        target_id="analytics-postgresql-prod",
        engine="postgresql",
        level=level,
        scope=scope,
        expires_at=datetime(2026, 10, 1, tzinfo=UTC),
        reason="Подготовка отчёта",
        requested_by="petrova",
    )


def test_read_tables_plan_grants_only_requested_tables() -> None:
    """Проверяет, что роль чтения таблиц не получает более широкие права."""

    adapter = PostgreSQLAccessAdapter("sqlite://")
    request = build_request(
        "read_tables",
        {"database": "analytics", "schema_name": "public", "tables": ["orders", "sales"]},
    )

    plan = adapter.build_grant_plan(request)

    assert any(
        statement.startswith('GRANT SELECT ON TABLE "public"."orders", "public"."sales"')
        for statement in plan.statements
    )
    assert not any("ALL TABLES" in statement for statement in plan.statements)
    assert plan.statements[-1].endswith('TO "ivanov"')


def test_read_all_plan_grants_select_for_each_supplied_schema() -> None:
    """Проверяет, что чтение всей базы охватывает все переданные схемы."""

    adapter = PostgreSQLAccessAdapter("sqlite://")
    request = build_request("read_all", {"database": "analytics"})

    plan = adapter.build_grant_plan(request, schema_names=("mart", "public"))

    assert any(
        statement.startswith('GRANT SELECT ON ALL TABLES IN SCHEMA "mart"')
        for statement in plan.statements
    )
    assert any(
        statement.startswith('GRANT SELECT ON ALL TABLES IN SCHEMA "public"')
        for statement in plan.statements
    )


def test_service_plan_grants_role_to_service_login() -> None:
    """Проверяет выдачу роли выделенной сервисной учётной записи.

    Тест защищает правило изоляции микросервисов: роль должна назначаться
    конкретному логину сервиса, а не общему пользователю команды.
    """

    adapter = PostgreSQLAccessAdapter("sqlite://")
    request = AccessGrantRequest(
        principal={
            "principal_id": "orders-prod",
            "principal_type": "service",
            "login_name": "svc_orders_prod",
            "display_name": "Orders production",
            "service_name": "orders",
            "environment": "prod",
            "owner_team": "commerce",
            "secret_ref": "vault://database-access/orders/prod/postgresql",
        },
        target_id="analytics-postgresql-prod",
        engine="postgresql",
        level="read_all",
        scope={"database": "analytics"},
        reason="Чтение витрины заказов",
        requested_by="petrova",
    )

    plan = adapter.build_grant_plan(request, schema_names=("public",))

    assert plan.statements[-1].endswith('TO "svc_orders_prod"')


def test_invalid_postgresql_identifier_is_rejected() -> None:
    """Проверяет защиту от небезопасных имён объектов в SQL-командах."""

    adapter = PostgreSQLAccessAdapter("sqlite://")
    request = build_request(
        "read_tables",
        {
            "database": "analytics",
            "schema_name": "public",
            "tables": ["orders; DROP TABLE users"],
        },
    )

    with pytest.raises(ValueError, match="Имя объекта PostgreSQL"):
        adapter.build_grant_plan(request)


def test_create_login_statement_enables_role_inheritance() -> None:
    """Проверяет, что новый логин получает назначенные роли сразу после входа.

    Тест защищает сценарий руководителя: после ручного создания учётной записи
    не должно требоваться отдельное выполнение `SET ROLE` для доступа к данным.
    """

    adapter = PostgreSQLAccessAdapter("sqlite://")

    statement = adapter.build_create_login_statement("ivanov")

    assert " INHERIT " in statement
    assert " NOINHERIT " not in statement


def test_schema_role_name_is_readable_for_production_access() -> None:
    """Проверяет понятное имя роли для доступа к отдельной схеме production.

    Тест защищает соглашение сопровождения: по имени роли должно быть видно
    целевую базу, схему, среду и уровень прав без расшифровки технического хэша.
    """

    adapter = PostgreSQLAccessAdapter("sqlite://")
    request = build_request(
        "read_all",
        {"database": "vector_db", "schema_name": "public"},
    )

    plan = adapter.build_grant_plan(request)

    assert plan.role_name == "vector_db_public_prod_read"


def test_write_schema_plan_includes_default_privileges() -> None:
    """Проверяет автоматические права на новые объекты технического владельца.

    Тест защищает рабочий сценарий записи: после появления новой таблицы в схеме
    роль записи должна получать тот же набор DML-прав без ручной донастройки.
    """

    adapter = PostgreSQLAccessAdapter("sqlite://")
    request = build_request(
        "write",
        {"database": "vector_db", "schema_name": "public"},
    )

    plan = adapter.build_grant_plan(request)

    assert plan.role_name == "vector_db_public_prod_write"
    assert any(
        statement.startswith('ALTER DEFAULT PRIVILEGES IN SCHEMA "public" GRANT SELECT, INSERT')
        for statement in plan.statements
    )


def test_full_access_plan_includes_default_privileges_for_all_schemas() -> None:
    """Проверяет наследование полного доступа на будущие объекты всех схем."""

    adapter = PostgreSQLAccessAdapter("sqlite://")
    request = build_request("full_access", {"database": "vector_db"})

    plan = adapter.build_grant_plan(request, schema_names=("public", "supplier"))

    assert plan.role_name == "vector_db_all_schemas_prod_manage"
    assert any(
        statement == 'ALTER DEFAULT PRIVILEGES IN SCHEMA "public" GRANT ALL PRIVILEGES ON TABLES TO "vector_db_all_schemas_prod_manage"'
        for statement in plan.statements
    )
