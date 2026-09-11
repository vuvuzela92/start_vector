from __future__ import annotations

import logging

from src_oop.jobs.bukh_docs.models import WeekNRedeemRunResult
from src_oop.jobs.bukh_docs.week_n_redeem_service import WeekNRedeemService

logger = logging.getLogger(__name__)


def update_week_n_redeem() -> WeekNRedeemRunResult:
    """Запускает выгрузку объединенной витрины week_n_redeem в Google Sheets.

    Бизнес-сценарий собирает данные по еженедельным отчетам реализации,
    финансовым отчетам и уведомлениям о выкупе, затем обновляет лист БД. Если
    запись в Google Sheets или подготовка данных завершилась ошибкой, функция
    поднимает исключение, чтобы CLI не показывал ложный успешный запуск.
    """
    service = WeekNRedeemService()
    result = service.run(write_to_google=True)
    logger.info(
        "Завершен запуск week_n_redeem: status=%s sql_rows=%s rows_after_processing=%s rows_after_filter=%s errors=%s",
        result.status,
        result.sql_rows,
        result.rows_after_processing,
        result.rows_after_filter,
        len(result.errors),
    )
    if result.status == "failed":
        raise RuntimeError(
            "Выгрузка week_n_redeem завершилась ошибкой: "
            f"sql_rows={result.sql_rows}, "
            f"rows_after_filter={result.rows_after_filter}, "
            f"errors={len(result.errors)}."
        )
    return result
