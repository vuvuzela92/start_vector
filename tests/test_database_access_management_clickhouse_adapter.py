"""Проверки планировщика ролей ClickHouse без сетевого подключения."""

from src_oop.jobs.database_access_management.clickhouse_adapter import ClickHouseAccessAdapter
from src_oop.jobs.database_access_management.models import AccessGrantRequest


def test_read_tables_plan_limits_clickhouse_role_to_selected_table() -> None:
    """Проверяет, что роль чтения ClickHouse не получает доступ ко всей базе."""

    request = AccessGrantRequest(
        principal={
            "principal_id": "orders-prod",
            "principal_type": "service",
            "login_name": "svc_orders_prod",
            "display_name": "Orders production",
            "service_name": "orders",
            "environment": "prod",
            "owner_team": "commerce",
            "secret_ref": "env://TEST_SERVICE_PASSWORD",
        },
        target_id="mpstats-clickhouse-prod",
        engine="clickhouse",
        level="read_tables",
        scope={"database": "mpstats", "schema_name": "default", "tables": ["orders"]},
        reason="Чтение заказов",
        requested_by="petrova",
    )

    plan = ClickHouseAccessAdapter.build_grant_plan(request)

    assert any("GRANT SELECT ON `mpstats`.`orders`" in item for item in plan.statements)
    assert not any("`mpstats`.*" in item for item in plan.statements)
    assert plan.statements[-1].endswith("TO `svc_orders_prod`")
