"""Модели запросов и ответов API сервиса управления доступами."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class DatabaseEngine(StrEnum):
    """Поддерживаемые типы корпоративных СУБД."""

    POSTGRESQL = "postgresql"
    CLICKHOUSE = "clickhouse"


class PrincipalType(StrEnum):
    """Определяет владельца учётной записи в целевой СУБД."""

    HUMAN = "human"
    SERVICE = "service"


class AccessLevel(StrEnum):
    """Утверждённые уровни доступа, доступные для назначения сотруднику."""

    FULL_ACCESS = "full_access"
    READ_ALL = "read_all"
    WRITE = "write"
    READ_TABLES = "read_tables"


class AccessGrantStatus(StrEnum):
    """Статусы технического исполнения распоряжения о доступе."""

    PENDING = "pending"
    APPLYING = "applying"
    REVOKING = "revoking"
    ACTIVE = "active"
    REVOKED = "revoked"
    CANCELLED = "cancelled"
    FAILED = "failed"
    EXPIRED = "expired"


class AccessScope(BaseModel):
    """Описывает область базы данных, в которой действует назначаемое право."""

    database: str = Field(min_length=1, max_length=128)
    schema_name: str | None = Field(default=None, min_length=1, max_length=128)
    tables: list[str] = Field(default_factory=list, max_length=100)


class DatabaseTargetCreateRequest(BaseModel):
    """Описывает зарегистрированную корпоративную базу для выдачи доступов.

    Модель обслуживает реестр целей, из которого руководитель выбирает базу в
    Telegram-боте. Административный пароль не хранится: сохраняется лишь ссылка
    на него в хранилище секретов, доступная будущему техническому исполнителю.
    """

    target_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9_-]+$")
    display_name: str = Field(min_length=1, max_length=256)
    engine: DatabaseEngine
    database_name: str = Field(min_length=1, max_length=128)
    admin_secret_ref: str = Field(min_length=1, max_length=512)
    created_by: str = Field(min_length=1, max_length=128)


class DatabaseTargetResponse(BaseModel):
    """Безопасное представление цели для выбора в интерфейсе руководителя."""

    target_id: str
    display_name: str
    engine: DatabaseEngine
    database_name: str
    is_active: bool
    created_at: datetime


class AccessPrincipal(BaseModel):
    """Описывает владельца учётной записи, для которого выдаётся доступ.

    Модель разделяет персональные и сервисные учётные записи. Это исключает
    использование общего логина несколькими микросервисами и позволяет
    прослеживать владельца каждого доступа. Пароль не передаётся и не хранится:
    для сервисной учётной записи допускается только ссылка на секрет.
    """

    principal_id: str = Field(min_length=1, max_length=128)
    principal_type: PrincipalType
    login_name: str = Field(min_length=1, max_length=128)
    display_name: str = Field(min_length=1, max_length=256)
    service_name: str | None = Field(default=None, min_length=1, max_length=128)
    environment: str | None = Field(default=None, min_length=1, max_length=64)
    owner_team: str | None = Field(default=None, min_length=1, max_length=128)
    secret_ref: str | None = Field(default=None, min_length=1, max_length=512)

    @model_validator(mode="after")
    def validate_principal_metadata(self) -> AccessPrincipal:
        """Проверяет реквизиты владельца для безопасной выдачи доступа.

        Персональному доступу достаточно идентификатора и выделенного логина.
        Для сервисного обязательны сервис, среда, команда-владелец и ссылка на
        секрет: это обеспечивает учёт и последующую ротацию, не раскрывая пароль
        в управляющем сервисе.
        """

        service_metadata = (self.service_name, self.environment, self.owner_team)
        if not self.secret_ref:
            raise ValueError("Для учётной записи обязательна ссылка на секрет пароля")
        if self.principal_type is PrincipalType.HUMAN and any(service_metadata):
            raise ValueError(
                "Для персональной учётной записи нельзя указывать реквизиты сервиса"
            )
        if self.principal_type is PrincipalType.SERVICE and not all(service_metadata):
            raise ValueError(
                "Для сервисной учётной записи обязательны сервис, среда, "
                "команда-владелец и ссылка на секрет"
            )
        return self


class AccessGrantRequest(BaseModel):
    """Запрос на выдачу доступа с проверкой бизнес-правил области и роли.

    Модель защищает сценарий выдачи прав сотруднику или сервисной учётной записи:
        управление назначается на базу или схему, чтение и запись — на базу или
        схему, а доступ к отдельным таблицам требует явного списка таблиц.
    """

    principal: AccessPrincipal
    target_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9_-]+$")
    engine: DatabaseEngine
    level: AccessLevel
    scope: AccessScope
    expires_at: datetime | None = Field(
        default=None,
        description="Дата и время автоматического отзыва в ISO 8601.",
    )
    reason: str = Field(min_length=3, max_length=500)
    requested_by: str = Field(
        min_length=1,
        max_length=128,
        description="Идентификатор сотрудника, создавшего заявку.",
    )

    @model_validator(mode="after")
    def validate_scope_for_access_level(self) -> AccessGrantRequest:
        """Проверяет, что область доступа соответствует выбранному уровню прав.

        Проверка предотвращает опасный сценарий, когда оператор ожидает
        ограничить доступ таблицей или схемой, но по ошибке формирует запрос на
        полный доступ. В дальнейшем только прошедшие эту проверку заявки смогут
        попасть в очередь исполнения прав в PostgreSQL или ClickHouse.
        """

        has_tables = bool(self.scope.tables)
        has_schema = self.scope.schema_name is not None

        if self.level is AccessLevel.FULL_ACCESS and has_tables:
            raise ValueError(
                "Для управления схемой нельзя указывать отдельные таблицы."
            )

        if self.level is AccessLevel.READ_ALL and has_tables:
            raise ValueError(
                "Для чтения всех данных нельзя указывать отдельные таблицы."
            )

        if self.level is AccessLevel.WRITE and has_tables:
            raise ValueError(
                "Для записи в схему нельзя указывать отдельные таблицы."
            )

        if self.level is AccessLevel.READ_TABLES and not has_tables:
            raise ValueError(
                "Для чтения отдельных таблиц нужно указать хотя бы одну таблицу."
            )

        if self.level is AccessLevel.READ_TABLES and not has_schema:
            raise ValueError(
                "Для чтения отдельных таблиц нужно указать схему таблиц."
            )

        return self


class AccessLevelInfo(BaseModel):
    """Публичное описание роли, отображаемое в интерфейсе администратора."""

    code: AccessLevel
    title: str
    description: str


class HealthResponse(BaseModel):
    """Ответ проверки доступности процесса сервиса."""

    status: str


class AccessGrantResponse(BaseModel):
    """Сохранённая заявка, ожидающая применения прав в целевой СУБД."""

    id: str
    status: AccessGrantStatus
    created_at: datetime
