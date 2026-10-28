"""Клиент публикации витрины отгрузок в Google Sheets."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.fbs2_test.config import WMS_REMAINS_FBS2_COLUMN

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PurchasersDatabaseGoogleSheetsClient:
    """Полностью обновляет заданную вкладку карты БД закупщиков."""

    spreadsheet_id: str
    table_title: str
    sheet_title: str
    credentials_path: Path = (
        Path(__file__).resolve().parents[3] / "creds" / "creds.json"
    )

    def upload_dataframe(self, dataframe: pd.DataFrame) -> None:
        """Публикует витрину в заданную вкладку карты БД закупщиков.

        Метод обслуживает финальный этап сценария: лист перезаписывается целиком,
        поэтому после изменения данных в БД в нём не остаются устаревшие строки.
        Общий клиент Google Sheets выполняет retry при временных сетевых ошибках
        и HTTP 429/5xx.
        """

        logger.info(
            "Начинаем запись витрины закупщиков в Google Sheets: лист=%s строк=%s.",
            self.sheet_title,
            len(dataframe.index),
        )
        google_tabs = GoogleTabs(
            table_title=self.table_title,
            sheet_title=self.sheet_title,
            creds_file=self.credentials_path,
            spreadsheet_id=self.spreadsheet_id,
        )
        google_tabs.set_df_to_google(dataframe.copy())
        logger.info(
            "Витрина закупщиков записана в Google Sheets: лист=%s строк=%s.",
            self.sheet_title,
            len(dataframe.index),
        )

    def update_fbs2_by_wild(self, wilds: list[str]) -> None:
        """Выгружает список `wild` в поле `fbs2` на листе `WMS остатки`.

        Бизнес-логика:
        запрос `card_data` возвращает готовую последовательность wild-кодов.
        Она записывается вертикально начиная со второй строки колонки `fbs2`.
        Это точечная запись только одной колонки и не затрагивает остальные
        показатели WMS.
        """

        google_tabs = GoogleTabs(
            table_title=self.table_title,
            sheet_title=self.sheet_title,
            creds_file=self.credentials_path,
            spreadsheet_id=self.spreadsheet_id,
        )
        worksheet = google_tabs.sheet_title
        headers = worksheet.row_values(1)
        if WMS_REMAINS_FBS2_COLUMN not in headers:
            raise ValueError(
                f"На листе '{self.sheet_title}' отсутствует колонка '{WMS_REMAINS_FBS2_COLUMN}'."
            )

        if not wilds:
            logger.warning(
                "Список wild для поля fbs2 пуст, запись в Google Sheets пропущена: лист=%s.",
                self.sheet_title,
            )
            return

        google_tabs.update_column_by_name(WMS_REMAINS_FBS2_COLUMN, wilds)
        logger.info(
            "Поле fbs2 обновлено в Google Sheets: лист=%s, строк=%s.",
            self.sheet_title,
            len(wilds),
        )
