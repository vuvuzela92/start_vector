from __future__ import annotations

import asyncio
import logging

from src_oop.jobs.advert_info.service import AdvertInfoService

logger = logging.getLogger(__name__)


async def advert_info_async(account: str | None = None) -> None:
    logger.info("Старт entrypoint advert_info_async | account=%s", account)

    service = AdvertInfoService()
    summary = await service.run(account=account)
    logger.info(
        "Завершён entrypoint advert_info_async | accounts_total=%s | accounts_processed=%s | accounts_without_campaigns=%s | raw_campaigns=%s | normalized_rows=%s | written_rows=%s | dropped_missing_key_rows=%s | collapsed_duplicate_rows=%s",
        summary.accounts_total,
        summary.accounts_processed,
        summary.accounts_without_campaigns,
        summary.raw_campaigns,
        summary.normalized_rows,
        summary.written_rows,
        summary.dropped_missing_key_rows,
        summary.collapsed_duplicate_rows,
    )


def advert_info(account: str | None = None) -> None:
    asyncio.run(advert_info_async(account=account))
