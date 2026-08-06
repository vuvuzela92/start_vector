from __future__ import annotations

import logging

from src_oop.jobs.add_new_items.repository import AddNewItemsRepository
from src_oop.jobs.add_new_items.service import AddNewItemsService

logger = logging.getLogger(__name__)


def add_new_items_run() -> None:
    """Точка входа для переноса новых товаров в рабочие таблицы."""

    logger.info("Инициализируем job add_new_items.")
    service = AddNewItemsService(repository=AddNewItemsRepository())
    service.run()
