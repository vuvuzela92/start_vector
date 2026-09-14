from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum


class HealthStatus(StrEnum):
    """Описывает итоговый статус проверки выгрузки для управленческого контроля."""

    OK = "OK"
    WARNING = "WARNING"
    FAILED = "FAILED"


class HealthSourceType(StrEnum):
    """Перечисляет поддерживаемые источники результата регулярной выгрузки."""

    POSTGRES = "postgres"
    GOOGLE_SHEETS = "google_sheets"


@dataclass(frozen=True, slots=True)
class HealthCheckTarget:
    """Хранит паспорт одной выгрузки, которую нужно контролировать ежедневно.

    Бизнес-смысл паспорта в том, чтобы для каждой критичной выгрузки явно
    зафиксировать источник результата, допустимое отставание и минимальный
    объём данных без догадок внутри проверяющего сервиса.
    """

    task_name: str
    display_name: str
    source_type: HealthSourceType
    max_age_hours: int
    min_rows: int
    critical: bool
    table_name: str | None = None
    date_column: str | None = None
    spreadsheet_title: str | None = None
    worksheet_title: str | None = None
    updated_at_column: str = "updated_at"


@dataclass(frozen=True, slots=True)
class HealthCheckSnapshot:
    """Фиксирует фактическое состояние данных, найденное в БД или Google Sheets."""

    rows_count: int
    latest_value: date | datetime | None


@dataclass(frozen=True, slots=True)
class HealthCheckResult:
    """Передаёт итог проверки одной выгрузки в общий отчёт health-check."""

    target: HealthCheckTarget
    status: HealthStatus
    rows_count: int | None
    latest_value: date | datetime | None
    message: str


@dataclass(frozen=True, slots=True)
class HealthCheckReport:
    """Хранит общий результат контроля всех включённых ежедневных выгрузок."""

    status: HealthStatus
    checked_at: datetime
    results: tuple[HealthCheckResult, ...]

