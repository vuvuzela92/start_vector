from __future__ import annotations

import logging

from src_oop.jobs.bukh_docs.week_n_redeem_run import (
    update_week_n_redeem as _update_week_n_redeem,
)

logger = logging.getLogger(__name__)


def update_week_n_redeem():
    """Совместимая обертка над новой OOP-реализацией week_n_redeem."""
    logger.info(
        "Legacy entrypoint src.modules.GOOGLE_SHEETS.week_n_redeem.update_week_n_redeem перенаправлен в src_oop.jobs.bukh_docs."
    )
    return _update_week_n_redeem()
