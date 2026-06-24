from __future__ import annotations

import logging

from src_oop.jobs.bukh_docs.models import WeekNRedeemRunResult
from src_oop.jobs.bukh_docs.week_n_redeem_service import WeekNRedeemService

logger = logging.getLogger(__name__)


def update_week_n_redeem() -> WeekNRedeemRunResult:
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
    return result
