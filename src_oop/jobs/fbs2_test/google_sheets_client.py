"""Клиент публикации витрины отгрузок в Google Sheets."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src_oop.core.my_gspread import GoogleTabs

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
