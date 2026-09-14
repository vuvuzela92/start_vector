from __future__ import annotations

import asyncio
import logging

from src_oop.core.telegram.notifier import TelegramNotifier
from src_oop.jobs.health_check.models import HealthStatus
from src_oop.jobs.health_check.service import HealthCheckService, format_health_check_alert

logger = logging.getLogger(__name__)


async def health_check_run_async() -> None:
    """Запускает контроль ежедневных выгрузок и отправляет Telegram-тревогу при проблемах.

    Бизнес-сценарий: health-check должен тихо завершаться при зелёном статусе и
    активно уведомлять сервисные чаты только тогда, когда критичная ежедневная
    выгрузка просрочена, пуста или источник проверки недоступен.
    """
    report = HealthCheckService().run()
    if report.status == HealthStatus.OK:
        logger.info("Health-check ежедневных выгрузок прошёл без отклонений.")
        return

    message = format_health_check_alert(report)
    sent_count = await TelegramNotifier().send_to_service_chats(message)
    logger.warning(
        "Health-check обнаружил отклонения и отправил Telegram-уведомления | status=%s | sent_count=%s",
        report.status,
        sent_count,
    )


def health_check_run() -> None:
    """Синхронный entrypoint для запуска health-check ежедневных выгрузок из реестра задач."""
    asyncio.run(health_check_run_async())

