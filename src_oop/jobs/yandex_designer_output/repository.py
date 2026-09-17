"""Публикация статистики дизайнеров в Google Sheets."""

from __future__ import annotations

import logging

import pandas as pd

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.yandex_designer_output.config import YandexDesignerOutputConfig
from src_oop.jobs.yandex_designer_output.models import DesignerOutputRow

logger = logging.getLogger(__name__)


class DesignerOutputRepository:
    """Добавляет ежедневный срез дизайнерской выработки в Google Sheets."""

    def __init__(self, config: YandexDesignerOutputConfig) -> None:
        """Подключает репозиторий к заданной таблице и вкладке."""

        self.google_tabs = GoogleTabs(config.spreadsheet_title, config.worksheet_title)

    def save(self, rows: list[DesignerOutputRow]) -> int:
        """Добавляет строки статистики и возвращает их количество.

        Бизнес-сценарий: Google Sheets используется как журнал ежедневной
        выработки, поэтому новый срез добавляется, а прежние даты сохраняются.
        """

        if not rows:
            logger.warning("Статистика дизайнеров не получена, запись в Google Sheets пропущена")
            return 0
        dataframe = pd.DataFrame([row.as_dict() for row in rows])
        self.google_tabs._send_df_to_google(dataframe, self.google_tabs.sheet_title)
        logger.info("Статистика дизайнерской выработки записана в Google Sheets | rows=%s", len(rows))
        return len(rows)
