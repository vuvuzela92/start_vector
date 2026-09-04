"""Административный запуск первичной инициализации служебной схемы."""

from src_oop.jobs.database_access_management.config import DatabaseAccessManagementSettings
from src_oop.jobs.database_access_management.repository import AccessGrantRepository


def initialize_access_management_schema() -> None:
    """Создаёт таблицы для заявок и аудита в выделенной служебной базе.

    Entrypoint запускается администратором однократно при развёртывании сервиса.
    Он не выдаёт и не отзывает права в управляемых PostgreSQL или ClickHouse,
    а подготавливает только внутреннее хранилище управленческих решений.
    """

    settings = DatabaseAccessManagementSettings.from_env()
    repository = AccessGrantRepository.from_database_url(
        settings.database_url,
        schema_name=settings.schema_name,
    )
    repository.initialize_schema()


if __name__ == "__main__":
    initialize_access_management_schema()
