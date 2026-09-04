"""HTTP-вход в сервис управления доступами к PostgreSQL и ClickHouse."""

from fastapi import FastAPI, HTTPException, status

from src_oop.jobs.database_access_management.config import DatabaseAccessManagementSettings
from src_oop.jobs.database_access_management.models import (
    AccessGrantRequest,
    AccessGrantResponse,
    AccessLevel,
    AccessLevelInfo,
    DatabaseEngine,
    DatabaseTargetCreateRequest,
    DatabaseTargetResponse,
    HealthResponse,
)
from src_oop.jobs.database_access_management.repository import AccessGrantRepository

app = FastAPI(
    title="Access Management Service",
    description="Сервис централизованного управления доступами к корпоративным БД.",
    version="0.1.0",
)


def get_access_grant_repository() -> AccessGrantRepository:
    """Возвращает репозиторий служебной базы или безопасно сообщает о настройке.

    Функция обслуживает все endpoint-ы заявок и не позволяет перепутать ошибку
    инфраструктурной конфигурации с ошибкой бизнес-правила согласования. Строка
    подключения не включается в HTTP-ответ и логи, поскольку содержит секреты.
    """

    try:
        settings = DatabaseAccessManagementSettings.from_env()
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Служебная база сервиса управления доступами не настроена.",
        ) from error
    return AccessGrantRepository.from_database_url(
        settings.database_url,
        schema_name=settings.schema_name,
    )


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Возвращает состояние процесса для мониторинга доступности сервиса.

    Endpoint обслуживает инфраструктурный сценарий: оркестратор или балансировщик
    убеждается, что API запущен, до передачи ему заявок на управление доступами.
    """

    return HealthResponse(status="ok")


@app.get("/v1/access-levels", response_model=list[AccessLevelInfo])
def list_access_levels() -> list[AccessLevelInfo]:
    """Возвращает утверждённые уровни доступа для формы выдачи прав.

    Endpoint исключает ручной ввод технических прав в интерфейсе: администратор
    выбирает только роли, для которых сервис впоследствии построит безопасные
    команды PostgreSQL или ClickHouse.
    """

    return [
        AccessLevelInfo(
            code=AccessLevel.FULL_ACCESS,
            title="Полный доступ к базе",
            description="Управление объектами и данными в пределах выбранной базы.",
        ),
        AccessLevelInfo(
            code=AccessLevel.READ_ALL,
            title="Чтение всех данных",
            description="Чтение всех таблиц выбранной базы или схемы без изменения данных.",
        ),
        AccessLevelInfo(
            code=AccessLevel.WRITE,
            title="Запись в схему",
            description="Чтение и изменение данных выбранной схемы без управления её объектами.",
        ),
        AccessLevelInfo(
            code=AccessLevel.READ_TABLES,
            title="Чтение отдельных таблиц",
            description="Чтение только явно перечисленных таблиц без изменения данных.",
        ),
    ]


@app.get("/v1/database-targets", response_model=list[DatabaseTargetResponse])
def list_database_targets(
    engine: DatabaseEngine | None = None,
) -> list[DatabaseTargetResponse]:
    """Возвращает активные базы, разрешённые для выдачи доступов.

    Endpoint обслуживает выбор целевой базы в Telegram-боте. Фильтр по типу
    СУБД сокращает список для руководителя и не раскрывает ссылки на секреты.
    """

    repository = get_access_grant_repository()
    return repository.list_active_database_targets(
        engine_name=engine.value if engine is not None else None
    )


@app.post(
    "/v1/database-targets",
    response_model=DatabaseTargetResponse,
    status_code=status.HTTP_201_CREATED,
)
def register_database_target(
    request: DatabaseTargetCreateRequest,
) -> DatabaseTargetResponse:
    """Регистрирует базу как допустимую цель для будущих распоряжений.

    Endpoint предназначен для административного контура настройки. Он сохраняет
    только ссылку на административный секрет, а не пароль или строку подключения.
    Проверка прав вызывающего руководителя будет добавлена вместе с Telegram
    адаптером и общим слоем аутентификации.
    """

    repository = get_access_grant_repository()
    try:
        return repository.register_database_target(request)
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error


@app.post(
    "/v1/access-grants",
    response_model=AccessGrantResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_access_grant(request: AccessGrantRequest) -> AccessGrantResponse:
    """Принимает распоряжение руководителя для технической выдачи доступа.

    Endpoint обслуживает управленческий сценарий без второго подтверждения:
    распоряжение проходит проверку области доступа, сохраняется со статусом
    `pending` и попадает в аудит. На этом этапе команды `GRANT` и `REVOKE` ещё
    не запускаются: их выполнит отдельный технический исполнитель.
    """

    repository = get_access_grant_repository()
    return repository.create_pending_grant(request)
