"""Репозиторий PostgreSQL для сервиса контроля рабочих чатов Bitrix24."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from sqlalchemy import Select, and_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from src_oop.core.database import Database
from src_oop.jobs.bitrix_chat_control.models import (
    AnalysisRun,
    AnalysisRunStatus,
    BitrixChatControlBase,
    BitrixMessage,
    BitrixMessageInput,
    BITRIX_CHAT_CONTROL_SCHEMA,
    MonitoredChat,
    Problem,
    ProblemMessage,
    ProblemMessageRelationType,
    ProblemStatus,
    TelegramReportLog,
)

logger = logging.getLogger(__name__)


class BitrixChatControlRepository:
    """Работает с PostgreSQL-состоянием chat-control сервиса.

    Репозиторий обслуживает три главных бизнес-сценария: идемпотентную
    синхронизацию сообщений, хранение жизненного цикла проблем и защиту от
    повторных Telegram-рассылок одинаковых отчётов.
    """

    _MANAGED_TABLE_NAMES: tuple[str, ...] = (
        "monitored_chats",
        "bitrix_messages",
        "problems",
        "problem_messages",
        "analysis_runs",
        "telegram_report_log",
    )

    def create_tables(self) -> None:
        """Создаёт схему и все таблицы Bitrix chat control в отдельном namespace.

        Метод обслуживает инициализацию и мягкую миграцию MVP: сначала
        гарантирует наличие схемы `bitrix_chat_control`, затем переносит в неё
        уже существующие таблицы модуля из `public`, и только после этого
        достраивает недостающие таблицы. Повторный запуск остаётся безопасным и
        не должен приводить к потере данных.
        """
        try:
            self._ensure_schema_exists()
            self._move_public_tables_to_module_schema()
            BitrixChatControlBase.metadata.create_all(Database.get_engine(), checkfirst=True)
        except SQLAlchemyError as error:
            logger.error(
                "Не удалось создать таблицы сервиса контроля чатов Bitrix24 | error_type=%s",
                type(error).__name__,
            )
            raise RuntimeError("Не удалось подготовить таблицы Bitrix chat control.") from None
        logger.info(
            "Таблицы сервиса контроля чатов Bitrix24 готовы к работе | schema=%s",
            BITRIX_CHAT_CONTROL_SCHEMA,
        )

    def _ensure_schema_exists(self) -> None:
        """Гарантирует наличие схемы модуля до создания или переноса таблиц.

        Это защищает боевой запуск от ситуации, когда ORM уже ожидает схему
        `bitrix_chat_control`, а сама схема ещё не создана в PostgreSQL.
        """
        with Database.get_engine().begin() as connection:
            connection.execute(
                text(f'CREATE SCHEMA IF NOT EXISTS "{BITRIX_CHAT_CONTROL_SCHEMA}"')
            )

    def _move_public_tables_to_module_schema(self) -> None:
        """Переносит legacy-таблицы модуля из `public` в выделенную схему.

        Миграция нужна для сохранения уже накопленных сообщений, проблем и
        служебных логов. Мы переносим только известные таблицы текущего модуля и
        не трогаем остальные объекты базы.
        """
        with Database.get_engine().begin() as connection:
            for table_name in self._MANAGED_TABLE_NAMES:
                exists_in_public = connection.execute(
                    text("SELECT to_regclass(:qualified_table_name)"),
                    {"qualified_table_name": f'public."{table_name}"'},
                ).scalar_one()
                if exists_in_public is None:
                    continue
                connection.execute(
                    text(
                        f'ALTER TABLE public."{table_name}" '
                        f'SET SCHEMA "{BITRIX_CHAT_CONTROL_SCHEMA}"'
                    )
                )
                logger.info(
                    "Таблица Bitrix chat control перенесена в отдельную схему | schema=%s | table=%s",
                    BITRIX_CHAT_CONTROL_SCHEMA,
                    table_name,
                )

    def ensure_monitored_chats(self, dialog_ids: Sequence[str]) -> int:
        """Создаёт или актуализирует monitored_chats по env-списку dialog id.

        Бизнес-смысл метода в том, чтобы MVP можно было запустить без отдельной
        админ-панели: оператор задаёт список Bitrix-чатов в env, а приложение
        создаёт для них записи мониторинга с активным статусом по умолчанию.
        """
        if not dialog_ids:
            return 0

        rows = [
            {
                "bitrix_dialog_id": dialog_id,
                "name": dialog_id,
                "is_active": True,
                "updated_at": datetime.now(tz=UTC),
                "created_at": datetime.now(tz=UTC),
            }
            for dialog_id in dialog_ids
        ]
        statement = insert(MonitoredChat).values(rows)
        upsert_statement = statement.on_conflict_do_update(
            index_elements=[MonitoredChat.bitrix_dialog_id],
            set_={
                "is_active": True,
                "updated_at": statement.excluded.updated_at,
            },
        )
        with Database.get_engine().begin() as connection:
            connection.execute(upsert_statement)
        return len(rows)

    def sync_monitored_chats(self, chats: Sequence[tuple[str, str]]) -> int:
        """Создаёт или актуализирует monitored_chats по discovery-списку Bitrix.

        Метод обслуживает основной бизнес-сценарий боевого запуска: техаккаунт
        видит все нужные рабочие чаты в Bitrix24, а приложение подтягивает их в
        мониторинг без ручного перечисления `BITRIX_CHAT_IDS`. Если запись уже
        существует, мы только обновляем название и активируем её обратно.
        """
        if not chats:
            return 0

        now = datetime.now(tz=UTC)
        rows = [
            {
                "bitrix_dialog_id": dialog_id,
                "name": name,
                "is_active": True,
                "updated_at": now,
                "created_at": now,
            }
            for dialog_id, name in chats
        ]
        statement = insert(MonitoredChat).values(rows)
        upsert_statement = statement.on_conflict_do_update(
            index_elements=[MonitoredChat.bitrix_dialog_id],
            set_={
                "name": statement.excluded.name,
                "is_active": True,
                "updated_at": statement.excluded.updated_at,
            },
        )
        with Database.get_engine().begin() as connection:
            connection.execute(upsert_statement)
        return len(rows)

    def list_active_chats(self) -> list[MonitoredChat]:
        """Возвращает все активные monitored_chats для sync и Telegram-выбора."""
        with Database.get_session() as session:
            return (
                session.execute(
                    select(MonitoredChat)
                    .where(MonitoredChat.is_active.is_(True))
                    .order_by(MonitoredChat.name.asc())
                )
                .scalars()
                .all()
            )

    def get_chat_by_id(self, chat_id: int) -> MonitoredChat | None:
        """Возвращает monitored_chat по внутреннему id для Telegram-обработчиков."""
        with Database.get_session() as session:
            return session.get(MonitoredChat, chat_id)

    def update_chat_metadata(
        self,
        chat_id: int,
        *,
        name: str | None = None,
        last_synced_message_id: str | None = None,
        last_synced_at: datetime | None = None,
    ) -> None:
        """Обновляет служебные поля monitored_chat после синхронизации Bitrix.

        Метод обслуживает инкрементальную загрузку: мы сохраняем границу
        последней синхронизации, чтобы следующие запуски могли читать только
        новое окно сообщений, а не всю историю переписки.
        """
        updates: dict[str, object] = {"updated_at": datetime.now(tz=UTC)}
        if name:
            updates["name"] = name
        if last_synced_message_id:
            updates["last_synced_message_id"] = last_synced_message_id
        if last_synced_at:
            updates["last_synced_at"] = last_synced_at.astimezone(UTC)

        with Database.get_engine().begin() as connection:
            connection.execute(
                update(MonitoredChat).where(MonitoredChat.id == chat_id).values(**updates)
            )

    def save_messages(
        self,
        messages: Sequence[BitrixMessageInput],
    ) -> list[BitrixMessage]:
        """Сохраняет сообщения Bitrix24 идемпотентно и возвращает только новые строки.

        Бизнес-правило: повторный запуск sync не должен создавать дубли и не
        должен повторно анализировать уже сохранённые сообщения. Поэтому метод
        возвращает только те записи, которые реально были вставлены в БД.
        """
        if not messages:
            return []

        rows = [message.model_dump(mode="python") for message in messages]
        statement = (
            insert(BitrixMessage)
            .values(rows)
            .on_conflict_do_nothing(index_elements=[BitrixMessage.bitrix_message_id])
            .returning(BitrixMessage.id)
        )
        with Database.get_engine().begin() as connection:
            inserted_ids = [row[0] for row in connection.execute(statement).all()]

        if not inserted_ids:
            return []

        with Database.get_session() as session:
            return (
                session.execute(
                    select(BitrixMessage)
                    .where(BitrixMessage.id.in_(inserted_ids))
                    .order_by(BitrixMessage.message_datetime.asc())
                )
                .scalars()
                .all()
            )

    def get_messages_for_period(
        self,
        *,
        dialog_id: str,
        period_start: datetime,
        period_end: datetime,
    ) -> list[BitrixMessage]:
        """Возвращает сообщения чата за период для ручного и weekly-отчёта."""
        with Database.get_session() as session:
            return (
                session.execute(
                    select(BitrixMessage)
                    .where(
                        BitrixMessage.dialog_id == dialog_id,
                        BitrixMessage.message_datetime >= period_start.astimezone(UTC),
                        BitrixMessage.message_datetime <= period_end.astimezone(UTC),
                    )
                    .order_by(BitrixMessage.message_datetime.asc())
                )
                .scalars()
                .all()
            )

    def list_open_problems(self, chat_id: int) -> list[Problem]:
        """Возвращает открытые проблемы чата для reconciliation и `/open`."""
        with Database.get_session() as session:
            return (
                session.execute(
                    select(Problem)
                    .where(
                        Problem.chat_id == chat_id,
                        Problem.status == ProblemStatus.OPEN.value,
                    )
                    .order_by(Problem.first_seen_at.asc())
                )
                .scalars()
                .all()
            )

    def get_problems_for_period(
        self,
        *,
        chat_id: int,
        period_start: datetime,
        period_end: datetime,
    ) -> list[Problem]:
        """Возвращает все релевантные проблемы чата для weekly/manual summary.

        В выборку входят проблемы, которые были открыты, обновлялись или
        закрывались в периоде. Это сохраняет целостную картину недели и не
        заставляет отчёт «забывать» об уже существовавших открытых проблемах.
        """
        with Database.get_session() as session:
            condition = and_(
                Problem.chat_id == chat_id,
                Problem.first_seen_at <= period_end.astimezone(UTC),
                (
                    (Problem.resolved_at.is_(None) & (Problem.last_seen_at >= period_start.astimezone(UTC)))
                    | (
                        Problem.resolved_at.is_not(None)
                        & (Problem.resolved_at >= period_start.astimezone(UTC))
                    )
                    | (Problem.first_seen_at >= period_start.astimezone(UTC))
                ),
            )
            return (
                session.execute(select(Problem).where(condition).order_by(Problem.first_seen_at.asc()))
                .scalars()
                .all()
            )

    def create_problem(
        self,
        *,
        chat_id: int,
        title: str,
        normalized_title: str,
        first_seen_at: datetime,
        last_seen_at: datetime,
        last_state_summary: str,
    ) -> Problem:
        """Создаёт новую открытую проблему по первому подтверждённому сигналу.

        Метод обслуживает бизнес-сценарий появления новой проблемы в чате: запись
        сразу попадает в жизненный цикл `open`, чтобы daily и weekly-отчёты могли
        видеть её состояние без повторного анализа прошлых сообщений.
        """
        problem = Problem(
            chat_id=chat_id,
            title=title,
            normalized_title=normalized_title,
            first_seen_at=first_seen_at.astimezone(UTC),
            last_seen_at=last_seen_at.astimezone(UTC),
            status=ProblemStatus.OPEN.value,
            last_state_summary=last_state_summary,
        )
        with Database.get_session() as session:
            session.add(problem)
            session.commit()
            session.refresh(problem)
            return problem

    def update_problem_state(
        self,
        problem_id: int,
        *,
        last_seen_at: datetime,
        last_state_summary: str,
        title: str | None = None,
        normalized_title: str | None = None,
    ) -> None:
        """Обновляет состояние уже известной открытой проблемы без её пересоздания.

        Это ключевое бизнес-правило «истории состояния»: если та же проблема
        продолжает обсуждаться, приложение должно обновить существующую запись, а
        не создавать новую проблему вместо старой.
        """
        values: dict[str, object] = {
            "last_seen_at": last_seen_at.astimezone(UTC),
            "last_state_summary": last_state_summary,
            "updated_at": datetime.now(tz=UTC),
        }
        if title:
            values["title"] = title
        if normalized_title:
            values["normalized_title"] = normalized_title
        with Database.get_engine().begin() as connection:
            connection.execute(update(Problem).where(Problem.id == problem_id).values(**values))

    def resolve_problem(
        self,
        problem_id: int,
        *,
        resolved_at: datetime,
        resolution_summary: str,
    ) -> None:
        """Переводит проблему в `resolved` только по явному подтверждению результата.

        Метод обслуживает главное правило резолюции: отсутствие новых сообщений и
        обещания действий не считаются решением. Статус меняется только после
        явного подтверждения исправления.
        """
        with Database.get_engine().begin() as connection:
            connection.execute(
                update(Problem)
                .where(Problem.id == problem_id)
                .values(
                    status=ProblemStatus.RESOLVED.value,
                    resolved_at=resolved_at.astimezone(UTC),
                    resolution_summary=resolution_summary,
                    updated_at=datetime.now(tz=UTC),
                )
            )

    def link_problem_messages(
        self,
        *,
        problem_id: int,
        message_ids: Sequence[int],
        relation_type: ProblemMessageRelationType,
    ) -> None:
        """Связывает проблему с исходными сообщениями без дублирования связей.

        Эта связь нужна для explainable-аудита: позже по ней можно показать,
        почему AI/эвристика решили, что проблема существует или была решена.
        """
        if not message_ids:
            return

        rows = [
            {
                "problem_id": problem_id,
                "message_id": message_id,
                "relation_type": relation_type.value,
            }
            for message_id in message_ids
        ]
        statement = insert(ProblemMessage).values(rows).on_conflict_do_nothing(
            index_elements=[
                ProblemMessage.problem_id,
                ProblemMessage.message_id,
                ProblemMessage.relation_type,
            ]
        )
        with Database.get_engine().begin() as connection:
            connection.execute(statement)

    def create_analysis_run(
        self,
        *,
        chat_id: int | None,
        run_type: str,
        period_start: datetime | None = None,
        period_end: datetime | None = None,
    ) -> int:
        """Создаёт analysis_run для идемпотентного аудита шагов pipeline.

        Это позволяет не терять историю sync/report-операций, разбирать сбои и
        отличать реальное отсутствие изменений от технического падения пайплайна.
        """
        run = AnalysisRun(
            chat_id=chat_id,
            run_type=run_type,
            period_start=period_start.astimezone(UTC) if period_start else None,
            period_end=period_end.astimezone(UTC) if period_end else None,
            status=AnalysisRunStatus.STARTED.value,
        )
        with Database.get_session() as session:
            session.add(run)
            session.commit()
            session.refresh(run)
            return int(run.id)

    def finish_analysis_run(
        self,
        run_id: int,
        *,
        status: AnalysisRunStatus,
        messages_scanned: int = 0,
        new_messages_count: int = 0,
        new_problems_count: int = 0,
        updated_problems_count: int = 0,
        resolved_problems_count: int = 0,
        report_hash: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Завершает analysis_run и сохраняет метрики результата.

        Метод обслуживает операционный контроль: по нему можно увидеть, сколько
        новых сообщений и проблем обработала job, а также чем закончился запуск.
        """
        with Database.get_engine().begin() as connection:
            connection.execute(
                update(AnalysisRun)
                .where(AnalysisRun.id == run_id)
                .values(
                    status=status.value,
                    messages_scanned=messages_scanned,
                    new_messages_count=new_messages_count,
                    new_problems_count=new_problems_count,
                    updated_problems_count=updated_problems_count,
                    resolved_problems_count=resolved_problems_count,
                    report_hash=report_hash,
                    error_message=error_message,
                    finished_at=datetime.now(tz=UTC),
                )
            )

    def is_report_already_sent(
        self,
        *,
        report_type: str,
        chat_id: int | None,
        period_start: datetime | None,
        period_end: datetime | None,
        recipient_chat_id: str,
        content: str,
    ) -> bool:
        """Проверяет, отправлялся ли уже такой же Telegram-отчёт.

        Бизнес-правило: повторный запуск daily/weekly job не должен слать один и
        тот же текст повторно. Дедупликация строится по хэшу контента и границам
        периода.
        """
        content_hash = self._content_hash(content)
        with Database.get_session() as session:
            query: Select[tuple[TelegramReportLog]] = select(TelegramReportLog).where(
                TelegramReportLog.report_type == report_type,
                TelegramReportLog.chat_id == chat_id,
                TelegramReportLog.period_start == (
                    period_start.astimezone(UTC) if period_start else None
                ),
                TelegramReportLog.period_end == (
                    period_end.astimezone(UTC) if period_end else None
                ),
                TelegramReportLog.recipient_chat_id == recipient_chat_id,
                TelegramReportLog.content_hash == content_hash,
            )
            return session.execute(query).scalar_one_or_none() is not None

    def mark_report_sent(
        self,
        *,
        report_type: str,
        chat_id: int | None,
        period_start: datetime | None,
        period_end: datetime | None,
        recipient_chat_id: str,
        content: str,
    ) -> None:
        """Фиксирует успешную отправку Telegram-отчёта для дедупликации повторов."""
        content_hash = self._content_hash(content)
        statement = insert(TelegramReportLog).values(
            {
                "report_type": report_type,
                "chat_id": chat_id,
                "period_start": period_start.astimezone(UTC) if period_start else None,
                "period_end": period_end.astimezone(UTC) if period_end else None,
                "recipient_chat_id": recipient_chat_id,
                "content_hash": content_hash,
            }
        ).on_conflict_do_nothing(
            constraint="uq_telegram_report_log_dedup"
        )
        with Database.get_engine().begin() as connection:
            connection.execute(statement)

    @staticmethod
    def _content_hash(content: str) -> str:
        """Строит устойчивый хэш текста отчёта для защиты от повторной отправки.

        Это защищает daily/weekly сценарии от дублей, даже если job перезапустили
        несколько раз с тем же периодом и тем же набором проблем.
        """
        return hashlib.sha256(content.encode("utf-8")).hexdigest()
