"""Настройки таблиц для операционных сценариев сборочных заданий."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AssemblyMarketSheetsSettings:
    """Хранит названия таблиц и листов старых сценариев сборки."""

    market_table: str = "Для расчетов БД"
    market_orders_sheet: str = "БД 2 ( ТЕСТ )"
    market_status_sheet: str = "Статусы сборки 2"
    source_assembly_sheet: str = "Сборочные задания 2"
    hanging_table: str = "ВИСЯЧИЕ + ДОСТАВКА"
    destination_assembly_sheet: str = "Сборочные задания 2"
    orders_lookback_days: int = 11
    orders_time_zone_offset_hours: int = 3
    export_time_column: str = "updated_at"
    request_timeout_seconds: int = 30
    max_retries: int = 4
    max_concurrent_accounts: int = 3
    request_delay_seconds: float = 1.0
    sheets_write_chunk_rows: int = 500


REQUIRED_ASSEMBLY_COLUMNS: tuple[str, ...] = (
    "id", "wild", "Дата", "Время", "Статус", "уд", "Время московское",
    "Тест", "Стадия", "ЛК", "Прошло часов",
)
