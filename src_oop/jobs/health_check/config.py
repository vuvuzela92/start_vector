from __future__ import annotations

from src_oop.jobs.fbo_supplies.config import SHIPMENTS_FBO_TABLE
from src_oop.jobs.funnel_sales.config import TABLE_NAME as FUNNEL_SALES_TABLE
from src_oop.jobs.health_check.models import HealthCheckTarget, HealthSourceType
from src_oop.jobs.orders_feed.config import TABLE_NAME as ORDER_FEED_TABLE


# Стартовый набор включает только выгрузки, у которых в коде есть понятный
# источник фактического результата и стабильное правило свежести.
DEFAULT_HEALTH_CHECK_TARGETS: tuple[HealthCheckTarget, ...] = (
    HealthCheckTarget(
        task_name="funnel_sales",
        display_name="Ежедневная воронка продаж WB",
        source_type=HealthSourceType.POSTGRES,
        table_name=FUNNEL_SALES_TABLE,
        date_column="date",
        max_age_hours=36,
        min_rows=1,
        critical=True,
    ),
    HealthCheckTarget(
        task_name="order_feed",
        display_name="WB Order Feed",
        source_type=HealthSourceType.POSTGRES,
        table_name=ORDER_FEED_TABLE,
        date_column="loaded_at",
        max_age_hours=6,
        min_rows=1,
        critical=True,
    ),
    HealthCheckTarget(
        task_name="fbo_supplies_run",
        display_name="Отгрузка ФБО: заказы по округам",
        source_type=HealthSourceType.GOOGLE_SHEETS,
        spreadsheet_title=SHIPMENTS_FBO_TABLE["title"],
        worksheet_title=SHIPMENTS_FBO_TABLE["orders_by_region_sheet"],
        updated_at_column="updated_at",
        max_age_hours=36,
        min_rows=1,
        critical=True,
    ),
)

