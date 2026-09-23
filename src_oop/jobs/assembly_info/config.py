"""Настройки job синхронизации сборочных заданий."""

from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class AssemblyInfoSettings:
    """Хранит лимиты API и параметры целевой таблицы сборочных заданий."""

    orders_url: str = "https://marketplace-api.wildberries.ru/api/v3/orders"
    statuses_url: str = "https://marketplace-api.wildberries.ru/api/v3/orders/status"
    request_timeout_seconds: int = 10
    max_retries: int = 3
    concurrency: int = 8
    chunk_size: int = 1000
    request_delay_seconds: float = 0.2
    table_name: str = "assembly_task_status_model"
    db_chunk_size: int = 2000
    db_max_retries: int = 3
    db_statement_timeout_seconds: int = 300
    db_lock_timeout_seconds: int = 30
    status_lookback_days: int = 11
    terminal_wb_statuses: tuple[str, ...] = (
        "sold",
        "canceled",
        "canceled_by_client",
        "declined_by_client",
        "defect",
        "canceled_by_carrier",
    )

    @classmethod
    def from_env(cls) -> "AssemblyInfoSettings":
        """Читает переопределяемые лимиты job из окружения без чтения секретов."""
        terminal_statuses = tuple(
            status.strip()
            for status in os.getenv(
                "ASSEMBLY_INFO_TERMINAL_WB_STATUSES",
                "sold,canceled,canceled_by_client,declined_by_client,defect,canceled_by_carrier",
            ).split(",")
            if status.strip()
        )
        return cls(
            request_timeout_seconds=int(os.getenv("ASSEMBLY_INFO_TIMEOUT", "10")),
            max_retries=int(os.getenv("ASSEMBLY_INFO_MAX_RETRIES", "3")),
            concurrency=int(os.getenv("ASSEMBLY_INFO_CONCURRENCY", "8")),
            db_chunk_size=int(os.getenv("ASSEMBLY_INFO_DB_CHUNK_SIZE", "2000")),
            db_max_retries=int(os.getenv("ASSEMBLY_INFO_DB_MAX_RETRIES", "3")),
            db_statement_timeout_seconds=int(os.getenv("ASSEMBLY_INFO_DB_STATEMENT_TIMEOUT", "300")),
            db_lock_timeout_seconds=int(os.getenv("ASSEMBLY_INFO_DB_LOCK_TIMEOUT", "30")),
            status_lookback_days=int(os.getenv("ASSEMBLY_INFO_STATUS_LOOKBACK_DAYS", "11")),
            terminal_wb_statuses=terminal_statuses,
        )
