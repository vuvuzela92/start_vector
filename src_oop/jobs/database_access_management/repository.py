"""Хранилище заявок и журнала аудита сервиса управления доступами."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    MetaData,
    String,
    Table,
    create_engine,
    select,
    update,
)
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import text

from src_oop.jobs.database_access_management.models import (
    AccessGrantRequest,
    AccessGrantResponse,
    AccessGrantStatus,
    AccessLevel,
    AccessPrincipal,
    DatabaseTargetCreateRequest,
    DatabaseTargetResponse,
    PrincipalType,
)

logger = logging.getLogger(__name__)

metadata = MetaData()

database_targets = Table(
    "database_targets",
    metadata,
    Column("target_id", String(128), primary_key=True),
    Column("display_name", String(256), nullable=False),
    Column("engine", String(32), nullable=False),
    Column("database_name", String(128), nullable=False),
    Column("admin_secret_ref", String(512), nullable=False),
    Column("created_by", String(128), nullable=False),
    Column("is_active", Boolean, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

access_grants = Table(
    "access_grants",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("target_id", String(128), nullable=False, index=True),
    Column("principal_id", String(128), nullable=False, index=True),
    Column("principal_type", String(32), nullable=False),
    Column("login_name", String(128), nullable=False),
    Column("display_name", String(256), nullable=False),
    Column("service_name", String(128), nullable=True),
    Column("environment", String(64), nullable=True),
    Column("owner_team", String(128), nullable=True),
    Column("secret_ref", String(512), nullable=True),
    Column("engine", String(32), nullable=False),
    Column("access_level", String(32), nullable=False),
    Column("database_name", String(128), nullable=False),
    Column("schema_name", String(128), nullable=True),
    Column("table_names", JSON, nullable=False),
    Column("reason", String(500), nullable=False),
    Column("requested_by", String(128), nullable=False),
    Column("status", String(32), nullable=False, index=True),
    Column("expires_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("applied_at", DateTime(timezone=True), nullable=True),
    Column("revoked_at", DateTime(timezone=True), nullable=True),
    Column("failure_reason", String(500), nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

access_audit_log = Table(
    "access_audit_log",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("grant_id", String(36), nullable=False, index=True),
    Column("action", String(64), nullable=False),
    Column("actor_id", String(128), nullable=False),
    Column("details", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


@dataclass(slots=True)
class AccessGrantRepository:
    """Сохраняет заявки и аудит до фактического применения прав в СУБД.

    Репозиторий отделяет управленческое решение о выдаче доступа от выполнения
    технических SQL-команд. Благодаря этому каждая заявка имеет статус и
    аудит даже при временной недоступности PostgreSQL или ClickHouse.
    """

    engine: Engine
    schema_name: str | None = None

    def claim_pending_grant(self, grant_id: str) -> tuple[AccessGrantRequest, str] | None:
        """Атомарно забирает распоряжение для технического применения.

        Метод защищает от двух параллельных исполнителей: только один из них
        переводит запись из `pending` в `applying` и получает административную
        ссылку цели. Ссылка не попадает в аудит или логи.
        """

        updated_at = datetime.now(UTC)
        with self.engine.begin() as connection:
            result = connection.execute(
                update(access_grants)
                .where(
                    access_grants.c.id == grant_id,
                    access_grants.c.status == AccessGrantStatus.PENDING.value,
                )
                .values(status=AccessGrantStatus.APPLYING.value, updated_at=updated_at)
            )
            if result.rowcount != 1:
                return None
            row = connection.execute(
                select(access_grants, database_targets.c.admin_secret_ref)
                .join(
                    database_targets,
                    database_targets.c.target_id == access_grants.c.target_id,
                )
                .where(access_grants.c.id == grant_id)
            ).mappings().one()
            request = AccessGrantRequest(
                principal=AccessPrincipal(
                    principal_id=row["principal_id"],
                    principal_type=PrincipalType(row["principal_type"]),
                    login_name=row["login_name"],
                    display_name=row["display_name"],
                    service_name=row["service_name"],
                    environment=row["environment"],
                    owner_team=row["owner_team"],
                    secret_ref=row["secret_ref"],
                ),
                target_id=row["target_id"],
                engine=row["engine"],
                level=AccessLevel(row["access_level"]),
                scope={
                    "database": row["database_name"],
                    "schema_name": row["schema_name"],
                    "tables": row["table_names"],
                },
                expires_at=row["expires_at"],
                reason=row["reason"],
                requested_by=row["requested_by"],
            )
            return request, row["admin_secret_ref"]

    def mark_grant_active(self, grant_id: str, role_name: str) -> None:
        """Фиксирует успешное применение роли и событие аудита."""

        updated_at = datetime.now(UTC)
        self._finish_applying_grant(
            grant_id=grant_id,
            status=AccessGrantStatus.ACTIVE,
            action="grant_activated",
            updated_at=updated_at,
            details={"role_name": role_name},
            failure_reason=None,
        )

    def cancel_pending_grant(self, grant_id: str, actor_id: str) -> bool:
        """Отменяет неисполненное ручное распоряжение и сохраняет аудит.

        Метод обслуживает возврат в Telegram-форме до запуска SQL-команд. Он
        изменяет только статус `pending`, поэтому не может отменить уже
        применённый доступ или повлиять на существующие права в PostgreSQL.
        """

        updated_at = datetime.now(UTC)
        with self.engine.begin() as connection:
            result = connection.execute(
                update(access_grants)
                .where(
                    access_grants.c.id == grant_id,
                    access_grants.c.status == AccessGrantStatus.PENDING.value,
                )
                .values(status=AccessGrantStatus.CANCELLED.value, updated_at=updated_at)
            )
            if result.rowcount != 1:
                return False
            connection.execute(
                access_audit_log.insert().values(
                    id=str(uuid4()),
                    grant_id=grant_id,
                    action="grant_cancelled",
                    actor_id=actor_id,
                    details={"reason": "Руководитель вернулся из формы до выдачи прав."},
                    created_at=updated_at,
                )
            )
        return True

    def claim_active_grant_for_revocation(
        self,
        grant_id: str,
    ) -> tuple[AccessGrantRequest, str] | None:
        """Захватывает активный доступ для точечного технического отзыва.

        Метод защищает сценарий отзыва от повторных нажатий в Telegram: только
        один исполнитель переводит запись из `active` в `revoking` и получает
        ссылку на административный секрет, не раскрывая её в аудите.
        """

        updated_at = datetime.now(UTC)
        with self.engine.begin() as connection:
            result = connection.execute(
                update(access_grants)
                .where(
                    access_grants.c.id == grant_id,
                    access_grants.c.status == AccessGrantStatus.ACTIVE.value,
                )
                .values(status=AccessGrantStatus.REVOKING.value, updated_at=updated_at)
            )
            if result.rowcount != 1:
                return None
            row = connection.execute(
                select(access_grants, database_targets.c.admin_secret_ref)
                .join(
                    database_targets,
                    database_targets.c.target_id == access_grants.c.target_id,
                )
                .where(access_grants.c.id == grant_id)
            ).mappings().one()
            request = self._build_request_from_grant_row(row)
            return request, row["admin_secret_ref"]

    def mark_grant_revoked(self, grant_id: str, role_name: str) -> None:
        """Фиксирует успешный отзыв доступа и сохраняет аудит операции."""

        updated_at = datetime.now(UTC)
        with self.engine.begin() as connection:
            result = connection.execute(
                update(access_grants)
                .where(
                    access_grants.c.id == grant_id,
                    access_grants.c.status == AccessGrantStatus.REVOKING.value,
                )
                .values(status=AccessGrantStatus.REVOKED.value, revoked_at=updated_at, updated_at=updated_at)
            )
            if result.rowcount != 1:
                raise RuntimeError("Распоряжение не находится в техническом отзыве.")
            connection.execute(
                access_audit_log.insert().values(
                    id=str(uuid4()), grant_id=grant_id, action="grant_revoked",
                    actor_id="technical_executor", details={"role_name": role_name},
                    created_at=updated_at,
                )
            )

    def get_applied_role_name(self, grant_id: str) -> str | None:
        """Возвращает фактическое имя роли, назначенной по распоряжению.

        Метод защищает отзыв доступа при изменении правил именования ролей:
        используется имя из неизменяемого аудита успешной выдачи, а не новое
        вычисленное имя, которое могло измениться после создания распоряжения.
        """

        query = (
            select(access_audit_log.c.details)
            .where(
                access_audit_log.c.grant_id == grant_id,
                access_audit_log.c.action == "grant_activated",
            )
            .order_by(access_audit_log.c.created_at.desc())
            .limit(1)
        )
        with self.engine.connect() as connection:
            details = connection.execute(query).scalar_one_or_none()
        if not isinstance(details, dict):
            return None
        role_name = details.get("role_name")
        return role_name if isinstance(role_name, str) else None

    @staticmethod
    def _build_request_from_grant_row(row: object) -> AccessGrantRequest:
        """Восстанавливает контракт распоряжения из служебной записи для исполнителя."""

        return AccessGrantRequest(
            principal=AccessPrincipal(
                principal_id=row["principal_id"], principal_type=PrincipalType(row["principal_type"]),
                login_name=row["login_name"], display_name=row["display_name"],
                service_name=row["service_name"], environment=row["environment"],
                owner_team=row["owner_team"], secret_ref=row["secret_ref"],
            ), target_id=row["target_id"], engine=row["engine"],
            level=AccessLevel(row["access_level"]),
            scope={"database": row["database_name"], "schema_name": row["schema_name"], "tables": row["table_names"]},
            expires_at=row["expires_at"], reason=row["reason"], requested_by=row["requested_by"],
        )

    def mark_grant_failed(self, grant_id: str, failure_reason: str) -> None:
        """Фиксирует безопасную причину технической ошибки без секретов."""

        updated_at = datetime.now(UTC)
        self._finish_applying_grant(
            grant_id=grant_id,
            status=AccessGrantStatus.FAILED,
            action="grant_failed",
            updated_at=updated_at,
            details={"failure_reason": failure_reason},
            failure_reason=failure_reason,
        )

    def _finish_applying_grant(
        self,
        grant_id: str,
        status: AccessGrantStatus,
        action: str,
        updated_at: datetime,
        details: dict[str, str],
        failure_reason: str | None,
    ) -> None:
        """Завершает захваченное распоряжение и сохраняет неизменяемый аудит.

        Вспомогательный метод разрешает итоговый переход только из `applying`,
        чтобы устаревший исполнитель не перезаписал уже обработанный результат.
        """

        with self.engine.begin() as connection:
            result = connection.execute(
                update(access_grants)
                .where(
                    access_grants.c.id == grant_id,
                    access_grants.c.status.in_(
                        (AccessGrantStatus.APPLYING.value, AccessGrantStatus.REVOKING.value)
                    ),
                )
                .values(
                    status=status.value,
                    applied_at=updated_at if status is AccessGrantStatus.ACTIVE else None,
                    failure_reason=failure_reason,
                    updated_at=updated_at,
                )
            )
            if result.rowcount != 1:
                raise RuntimeError("Распоряжение не находится в техническом исполнении.")
            connection.execute(
                access_audit_log.insert().values(
                    id=str(uuid4()),
                    grant_id=grant_id,
                    action=action,
                    actor_id="technical_executor",
                    details=details,
                    created_at=updated_at,
                )
            )

    @classmethod
    def from_database_url(
        cls,
        database_url: str,
        schema_name: str | None = None,
    ) -> AccessGrantRepository:
        """Создаёт репозиторий для отдельной служебной базы.

        Метод используется API при создании заявки. Для PostgreSQL в MVP он
        настраивает `search_path` на выделенную служебную схему. Подключение не
        выводится в логах, чтобы не раскрывать учётные данные администратора базы.
        """

        database_engine = create_engine(database_url, pool_pre_ping=True)
        if schema_name and make_url(database_url).get_backend_name() == "postgresql":
            database_engine = create_engine(
                database_url,
                pool_pre_ping=True,
                connect_args={"options": f"-csearch_path={schema_name}"},
            )
        return cls(engine=database_engine, schema_name=schema_name)

    def initialize_schema(self) -> None:
        """Создаёт служебные таблицы заявок и аудита при первичной установке.

        Метод запускается отдельной административной командой, а не при старте
        API. Это защищает рабочую среду от неявных изменений схемы при обычном
        перезапуске сервиса.
        """

        if self.schema_name and self.engine.dialect.name == "postgresql":
            with self.engine.begin() as connection:
                connection.execute(
                    text(f'CREATE SCHEMA IF NOT EXISTS "{self.schema_name}"')
                )
        metadata.create_all(self.engine)
        logger.info(
            "Инициализированы таблицы управления доступами | schema_name=%s",
            self.schema_name or "по умолчанию",
        )

    def register_database_target(
        self,
        request: DatabaseTargetCreateRequest,
    ) -> DatabaseTargetResponse:
        """Регистрирует целевую базу для выбора в управленческом интерфейсе.

        Метод обслуживает настройку допустимых целей доступа. Вместо строки
        подключения он сохраняет только ссылку на административный секрет, а
        повторная регистрация того же идентификатора запрещена, чтобы не
        переназначить существующие распоряжения на другую базу.
        """

        created_at = datetime.now(UTC)
        target_values = {
            "target_id": request.target_id,
            "display_name": request.display_name,
            "engine": request.engine.value,
            "database_name": request.database_name,
            "admin_secret_ref": request.admin_secret_ref,
            "created_by": request.created_by,
            "is_active": True,
            "created_at": created_at,
            "updated_at": created_at,
        }
        with self.engine.begin() as connection:
            existing_target = connection.execute(
                select(database_targets.c.target_id).where(
                    database_targets.c.target_id == request.target_id
                )
            ).scalar_one_or_none()
            if existing_target is not None:
                raise ValueError("Цель с таким идентификатором уже зарегистрирована.")
            connection.execute(database_targets.insert().values(target_values))

        logger.info(
            "Зарегистрирована цель доступа | target_id=%s | engine=%s",
            request.target_id,
            request.engine.value,
        )
        return DatabaseTargetResponse(
            target_id=request.target_id,
            display_name=request.display_name,
            engine=request.engine,
            database_name=request.database_name,
            is_active=True,
            created_at=created_at,
        )

    def list_active_database_targets(
        self,
        engine_name: str | None = None,
    ) -> list[DatabaseTargetResponse]:
        """Возвращает активные цели, доступные для выбора руководителем.

        Метод обслуживает шаг выбора базы в Telegram-боте. Ссылка на
        административный секрет намеренно не выбирается и не возвращается в API,
        поскольку интерфейсу для выбора базы она не нужна.
        """

        query = select(
            database_targets.c.target_id,
            database_targets.c.display_name,
            database_targets.c.engine,
            database_targets.c.database_name,
            database_targets.c.is_active,
            database_targets.c.created_at,
        ).where(database_targets.c.is_active.is_(True))
        if engine_name is not None:
            query = query.where(database_targets.c.engine == engine_name)

        with self.engine.connect() as connection:
            target_rows = connection.execute(
                query.order_by(database_targets.c.display_name)
            ).mappings().all()
        return [DatabaseTargetResponse.model_validate(row) for row in target_rows]

    def list_active_grants(self, login_name: str | None = None) -> list[dict[str, str]]:
        """Возвращает активные PostgreSQL-доступы для просмотра и отзыва.

        Метод обслуживает Telegram-команду просмотра: без фильтра показывает
        все активные доступы, а с логином — только выбранную учётную запись. Он
        намеренно не возвращает ссылки на секреты, причины или другие
        чувствительные поля.
        """

        query = (
            select(
                access_grants.c.id,
                access_grants.c.login_name,
                access_grants.c.database_name,
                access_grants.c.access_level,
                access_grants.c.expires_at,
            )
            .where(
                access_grants.c.engine == "postgresql",
                access_grants.c.status == AccessGrantStatus.ACTIVE.value,
            )
            .order_by(access_grants.c.created_at.desc())
        )
        if login_name:
            query = query.where(access_grants.c.login_name == login_name)
        with self.engine.connect() as connection:
            rows = connection.execute(query).mappings().all()
        return [dict(row) for row in rows]

    def create_pending_grant(self, request: AccessGrantRequest) -> AccessGrantResponse:
        """Сохраняет новую заявку и аудит её создания в одной транзакции.

        Метод обслуживает прямое распоряжение руководителя: заявка сразу
        получает статус `pending`, то есть ожидает только технического
        применения команд в целевой СУБД. Аудит фиксирует инициатора и параметры
        доступа, чтобы позднее можно было восстановить причину каждого права.
        """

        grant_id = str(uuid4())
        created_at = datetime.now(UTC)
        status = AccessGrantStatus.PENDING
        grant_values = {
            "id": grant_id,
            "target_id": request.target_id,
            "principal_id": request.principal.principal_id,
            "principal_type": request.principal.principal_type.value,
            "login_name": request.principal.login_name,
            "display_name": request.principal.display_name,
            "service_name": request.principal.service_name,
            "environment": request.principal.environment,
            "owner_team": request.principal.owner_team,
            "secret_ref": request.principal.secret_ref,
            "engine": request.engine.value,
            "access_level": request.level.value,
            "database_name": request.scope.database,
            "schema_name": request.scope.schema_name,
            "table_names": request.scope.tables,
            "reason": request.reason,
            "requested_by": request.requested_by,
            "status": status.value,
            "expires_at": request.expires_at,
            "created_at": created_at,
            "applied_at": None,
            "revoked_at": None,
            "failure_reason": None,
            "updated_at": created_at,
        }
        audit_values = {
            "id": str(uuid4()),
            "grant_id": grant_id,
            "action": "grant_requested",
            "actor_id": request.requested_by,
            "details": {
                "principal_id": request.principal.principal_id,
                "principal_type": request.principal.principal_type.value,
                "login_name": request.principal.login_name,
                "target_id": request.target_id,
                "engine": request.engine.value,
                "access_level": request.level.value,
                "database_name": request.scope.database,
                "schema_name": request.scope.schema_name,
                "table_names": request.scope.tables,
            },
            "created_at": created_at,
        }

        with self.engine.begin() as connection:
            target_row = connection.execute(
                select(
                    database_targets.c.engine,
                    database_targets.c.database_name,
                    database_targets.c.is_active,
                ).where(database_targets.c.target_id == request.target_id)
            ).mappings().one_or_none()
            if target_row is None:
                raise LookupError("Выбранная цель доступа не зарегистрирована.")
            if not target_row["is_active"]:
                raise ValueError("Выбранная цель доступа отключена.")
            if target_row["engine"] != request.engine.value:
                raise ValueError("Тип СУБД распоряжения не соответствует выбранной цели.")
            if target_row["database_name"] != request.scope.database:
                raise ValueError("База в области доступа не соответствует выбранной цели.")
            connection.execute(access_grants.insert().values(grant_values))
            connection.execute(access_audit_log.insert().values(audit_values))

        logger.info(
            "Создана заявка на доступ | grant_id=%s | principal_id=%s | engine=%s | level=%s",
            grant_id,
            request.principal.principal_id,
            request.engine.value,
            request.level.value,
        )
        return AccessGrantResponse(id=grant_id, status=status, created_at=created_at)
