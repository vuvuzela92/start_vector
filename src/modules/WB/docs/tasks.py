from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date

from src_oop.jobs.bukh_docs.run import get_bukh_docs_async as _get_bukh_docs_async

logger = logging.getLogger(__name__)


async def get_bukh_docs(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    tokens_by_account: Mapping[str, str] | None = None,
):
    """Совместимая обертка над новой OOP-реализацией бухгалтерских документов WB."""
    logger.info(
        "Legacy entrypoint src.modules.WB.docs.tasks.get_bukh_docs перенаправлен в src_oop.jobs.bukh_docs."
    )
    return await _get_bukh_docs_async(
        date_from=date_from,
        date_to=date_to,
        tokens_by_account=tokens_by_account,
    )
