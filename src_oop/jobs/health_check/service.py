from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta

from src_oop.jobs.health_check.config import DEFAULT_HEALTH_CHECK_TARGETS
from src_oop.jobs.health_check.models import (
    HealthCheckReport,
    HealthCheckResult,
    HealthCheckSnapshot,
    HealthCheckTarget,
    HealthSourceType,
    HealthStatus,
)
from src_oop.jobs.health_check.repository import (
    GoogleSheetsHealthCheckRepository,
    PostgreSQLHealthCheckRepository,
)

logger = logging.getLogger(__name__)


class HealthCheckService:
    """Проверяет своевременность и полноту ежедневных бизнес-выгрузок."""

    def __init__(
        self,
        targets: tuple[HealthCheckTarget, ...] = DEFAULT_HEALTH_CHECK_TARGETS,
        postgres_repository: PostgreSQLHealthCheckRepository | None = None,
        google_sheets_repository: GoogleSheetsHealthCheckRepository | None = None,
    ) -> None:
        """Собирает зависимости health-check без привязки к конкретному расписанию."""
        self.targets = targets
        self.postgres_repository = postgres_repository or PostgreSQLHealthCheckRepository()
        self.google_sheets_repository = google_sheets_repository or GoogleSheetsHealthCheckRepository()

    def run(self, checked_at: datetime | None = None) -> HealthCheckReport:
        """Запускает полный контроль критичных выгрузок и возвращает сводный статус.

        Бизнес-сценарий: пройти по утверждённым паспортам выгрузок, проверить
        свежесть и минимальный объём результата, затем собрать единый статус,
        по которому entrypoint решает, нужно ли отправлять Telegram-тревогу.
        """
        resolved_checked_at = checked_at or datetime.now(UTC)
        logger.info(
            "Запущен health-check ежедневных выгрузок | targets=%s",
            len(self.targets),
        )
        results = tuple(
            self._check_target(target=target, checked_at=resolved_checked_at)
            for target in self.targets
        )
        report = HealthCheckReport(
            status=self._resolve_report_status(results),
            checked_at=resolved_checked_at,
            results=results,
        )
        logger.info(
            "Health-check ежедневных выгрузок завершён | status=%s | checked=%s",
            report.status,
            len(results),
        )
        return report

    def _check_target(
        self,
        target: HealthCheckTarget,
        checked_at: datetime,
    ) -> HealthCheckResult:
        """Проверяет одну выгрузку и превращает ошибку источника в понятный статус."""
        try:
            snapshot = self._fetch_snapshot(target)
        except Exception as error:
            status = HealthStatus.FAILED if target.critical else HealthStatus.WARNING
            logger.error(
                "Health-check выгрузки завершился ошибкой источника, сценарий продолжает проверку остальных выгрузок | task=%s | error_type=%s",
                target.task_name,
                type(error).__name__,
            )
            return HealthCheckResult(
                target=target,
                status=status,
                rows_count=None,
                latest_value=None,
                message="проверка источника завершилась ошибкой",
            )

        return self._evaluate_snapshot(
            target=target,
            snapshot=snapshot,
            checked_at=checked_at,
        )

    def _fetch_snapshot(self, target: HealthCheckTarget) -> HealthCheckSnapshot:
        """Выбирает репозиторий по типу источника результата выгрузки."""
        if target.source_type == HealthSourceType.POSTGRES:
            return self.postgres_repository.fetch_snapshot(target)
        if target.source_type == HealthSourceType.GOOGLE_SHEETS:
            return self.google_sheets_repository.fetch_snapshot(target)
        raise ValueError(f"Неподдерживаемый источник health-check: {target.source_type}")

    def _evaluate_snapshot(
        self,
        target: HealthCheckTarget,
        snapshot: HealthCheckSnapshot,
        checked_at: datetime,
    ) -> HealthCheckResult:
        """Сравнивает фактическую свежесть и объём с паспортом выгрузки."""
        problems: list[str] = []
        if snapshot.rows_count < target.min_rows:
            problems.append(
                f"строк меньше минимума: {snapshot.rows_count} < {target.min_rows}"
            )
        if snapshot.latest_value is None:
            problems.append("не найдено время последнего обновления")
        elif self._is_stale(
            latest_value=snapshot.latest_value,
            checked_at=checked_at,
            max_age_hours=target.max_age_hours,
        ):
            problems.append(
                f"данные устарели: последнее обновление {snapshot.latest_value}"
            )

        if problems:
            status = HealthStatus.FAILED if target.critical else HealthStatus.WARNING
            message = "; ".join(problems)
        else:
            status = HealthStatus.OK
            message = "данные свежие и объём достаточный"

        return HealthCheckResult(
            target=target,
            status=status,
            rows_count=snapshot.rows_count,
            latest_value=snapshot.latest_value,
            message=message,
        )

    @staticmethod
    def _is_stale(
        latest_value: date | datetime,
        checked_at: datetime,
        max_age_hours: int,
    ) -> bool:
        """Определяет, вышла ли выгрузка за допустимое окно свежести."""
        if isinstance(latest_value, datetime):
            latest_datetime = latest_value
        else:
            latest_datetime = datetime.combine(latest_value, datetime.min.time())

        if latest_datetime.tzinfo is None:
            latest_datetime = latest_datetime.replace(tzinfo=checked_at.tzinfo or UTC)
        normalized_checked_at = checked_at
        if normalized_checked_at.tzinfo is None:
            normalized_checked_at = normalized_checked_at.replace(tzinfo=latest_datetime.tzinfo)
        return normalized_checked_at - latest_datetime > timedelta(hours=max_age_hours)

    @staticmethod
    def _resolve_report_status(results: tuple[HealthCheckResult, ...]) -> HealthStatus:
        """Поднимает общий статус до самого серьёзного результата среди выгрузок."""
        if any(result.status == HealthStatus.FAILED for result in results):
            return HealthStatus.FAILED
        if any(result.status == HealthStatus.WARNING for result in results):
            return HealthStatus.WARNING
        return HealthStatus.OK


def format_health_check_alert(report: HealthCheckReport) -> str:
    """Формирует короткое Telegram-сообщение о проблемах ежедневных выгрузок."""
    problem_results = [
        result for result in report.results if result.status != HealthStatus.OK
    ]
    lines = [
        f"Health-check ежедневных выгрузок: {report.status}",
        f"Проверено: {report.checked_at.isoformat()}",
    ]
    for result in problem_results:
        latest_value = result.latest_value.isoformat() if result.latest_value else "не найдено"
        rows_count = result.rows_count if result.rows_count is not None else "неизвестно"
        lines.append(
            f"- {result.target.display_name}: {result.status}; {result.message}; "
            f"строк={rows_count}; последнее обновление={latest_value}"
        )
    return "\n".join(lines)
