"""Точки входа job учета дизайнерской выработки."""

from __future__ import annotations

import asyncio

from src_oop.jobs.yandex_designer_output.config import get_config
from src_oop.jobs.yandex_designer_output.service import DesignerOutputService


def yandex_designer_output_run() -> None:
    """Запускает полный сценарий Yandex Disk и Google Sheets.

    Бизнес-сценарий: создает папки дизайнеров, считает файлы за текущий день
    и публикует результат в управленческую таблицу.
    """

    asyncio.run(DesignerOutputService(get_config()).run())
