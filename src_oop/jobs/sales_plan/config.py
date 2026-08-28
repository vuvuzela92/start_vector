from __future__ import annotations

from sqlalchemy import Boolean, Date, DateTime, Numeric, String, text

annual_procurement_plan = {
    "title": "Годовой план закупа 2026",
    "orders_sheet": "БД_ЗАКАЗЫ",
    "unit_sheet": "Данные_Юнитки",
    "supply_sheet": "Данные_Поставки",
    "parfume_sheet": "Данные_Парфюм",
    "quarter_sheet": "Поквартально",
}

# Источник категорийного справочника менеджеров для плана продаж.
sales_plan_manager_reference_sheet = {
    "title": "Панель управления продажами Вектор",
    "sheet_title": "Справочник Категория-Менеджер",
    "spreadsheet_id": "1j2H1sGdhuQYsMDs8JkDQ4DEL27pNxXtKVE6JcJbyvzo",
}

# Таблица хранит ежедневные снимки справочника, чтобы дальше можно было
# строить историю назначения менеджера и анализа категорий по дате.
SALES_PLAN_MANAGER_REFERENCE_TABLE = "sales_plan_category_manager_reference"
SALES_PLAN_MANAGER_REFERENCE_KEY_COLUMNS: tuple[str, ...] = (
    "snapshot_date",
    "subject_name",
)
SALES_PLAN_MANAGER_REFERENCE_SCHEMA = {
    "snapshot_date": Date,
    "subject_name": String(255),
    "manager_name": String(255),
    "loaded_at": DateTime,
}

# Исходные заголовки листа, на которые опирается первая загрузка справочника.
SOURCE_SUBJECT_COLUMN = "Предмет"
SOURCE_MANAGER_COLUMN = "Менеджер"
SOURCE_WILD_COLUMN = "Артикул"

# Источник учетной категории для итогового плана продаж.
SALES_PLAN_ACCOUNTING_CATEGORY_TABLE = "sales_plan_accounting_category_reference"
SALES_PLAN_ACCOUNTING_CATEGORY_KEY_COLUMNS: tuple[str, ...] = ("wild",)
SALES_PLAN_ACCOUNTING_CATEGORY_SCHEMA = {
    "wild": String(255),
    "subject_name": String(255),
    "quarter_3_units_2026": Numeric(14, 4),
    "plan_price": Numeric(14, 2),
    "created_at": DateTime,
}
QUARTER_PLAN_SUBJECT_COLUMN = "предмет"
QUARTER_PLAN_WILD_COLUMN = "wild"
QUARTER_PLAN_3Q_2026_UNITS_COLUMN = "3 квартал, шт 2026"
QUARTER_PLAN_WILD_STATUS_COLUMN = "Статус вилд"
QUARTER_PLAN_PRICE_COLUMN = "цена продажная плановая"

SALES_WILD_STATUS_DAILY_TABLE = "sales_wild_status_daily"
SALES_WILD_STATUS_DAILY_KEY_COLUMNS: tuple[str, ...] = ("date", "wild")
SALES_WILD_STATUS_DAILY_SCHEMA = {
    "wild": String(255),
    "is_active": Boolean,
    "date": Date,
    "created_at": DateTime,
}

# Базовое бизнес-правило для стартового накопления дней наличия:
# только статус "активно" считается днём присутствия товара в продаже.
ACTIVE_WILD_STATUSES: tuple[str, ...] = ("активно",)

funnel_query = text(
    """
    SELECT
"""
)
