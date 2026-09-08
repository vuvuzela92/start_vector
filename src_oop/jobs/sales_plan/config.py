from __future__ import annotations

from datetime import date

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

# Целевая витрина с расчетом плана продаж по каждому wild.
sales_plan_report_sheet = {
    "title": "План продаж",
    "sheet_title": "План продаж v2.0",
    "spreadsheet_id": "1_ZFKBxelzM3tJB9TTwab1watklasAtSoWSHhUsAZC7E",
}

# В витрине первые две строки заняты итогами и заголовками, а столбец U — документацией.
SALES_PLAN_REPORT_FIRST_DATA_ROW = 3
SALES_PLAN_REPORT_LAST_DATA_COLUMN = "T"
SALES_PLAN_REPORT_DOCUMENTATION_COLUMN = "U"
SALES_PLAN_REPORT_COLUMN_COUNT = 20
SALES_PLAN_REPORT_UPDATED_AT_COLUMN = "updatet_at"
SALES_PLAN_REPORT_HEADERS: tuple[str, ...] = (
    "Месяц",
    "Менеджер",
    "WILD",
    "Предмет",
    "Дней с остатком",
    "Количество план",
    "Количество факт",
    "Цена плановая",
    "Цена фактическая",
    "План заказов, руб",
    "План заказов на дату, руб",
    "Выполнение плана на дату, %",
    "План заказов, руб с правилом 15 дней",
    "Факт заказов, руб",
    "Факт заказов, %",
    "Линейный прогноз, руб",
    "Линейны прогноз, %",
    "Факт заказов, % с правилом 15 дней",
    "Прогноз, % с правилом 15 дней",
    SALES_PLAN_REPORT_UPDATED_AT_COLUMN,
)

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

# Служебная метка источника означает отсутствие доступной плановой цены.
MISSING_PLAN_PRICE_VALUES: tuple[str, ...] = ("нет цены",)

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

# Одноразовое восстановление пропуска после сбоя cron в сентябре 2026 года.
# Границы snapshot-ов используются только как подтвержденные точки истории.
SALES_WILD_STATUS_BACKFILL_DATE_FROM = date(2026, 9, 2)
SALES_WILD_STATUS_BACKFILL_DATE_TO = date(2026, 9, 7)
SALES_WILD_STATUS_BACKFILL_PREVIOUS_SNAPSHOT_DATE = date(2026, 9, 1)
SALES_WILD_STATUS_BACKFILL_NEXT_SNAPSHOT_DATE = date(2026, 9, 8)

funnel_query = text(
    """
    SELECT
"""
)
