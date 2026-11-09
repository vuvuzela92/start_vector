"""Точки входа job учета дизайнерской выработки."""

from __future__ import annotations

import asyncio

from src_oop.jobs.yandex_designer_output.config import get_config
from src_oop.jobs.yandex_designer_output.service import DesignerOutputService


def yandex_designer_output_run(days: int = 10) -> None:
    """Запускает пересчет выработки дизайнеров за выбранный период.

    Бизнес-сценарий: создает папки на завтра, классифицирует материалы за
    последние `days` календарных дней и обновляет таблицу без дублирования.
    """

    asyncio.run(DesignerOutputService(get_config()).run(days=days))
