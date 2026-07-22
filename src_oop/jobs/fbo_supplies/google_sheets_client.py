from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import gspread
import pandas as pd

from src_oop.core.my_gspread import GoogleTabs

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FboSuppliesGoogleSheetsClient:
    """Клиент записи подготовленных данных по ФБО-отгрузкам в Google Sheets."""

    table_title: str
    sheet_title: str
    default_credentials_path: Path = (
        Path(__file__).resolve().parents[3] / "creds" / "creds.json"
    )
    fallback_credentials_path: Path = (
        Path(__file__).resolve().parents[3] / "creds" / "creds_2.json"
    )

    def upload_dataframe(self, dataframe: pd.DataFrame) -> None:
        logger.info(
            "Готовим выгрузку ФБО-отгрузок в Google Sheets: table=%s sheet=%s rows=%s",
            self.table_title,
            self.sheet_title,
            len(dataframe.index),
        )
        try:
            self._upload_with_credentials(
                dataframe=dataframe,
                credentials_path=self.default_credentials_path,
            )
        except gspread.exceptions.APIError as error:
            if not self._is_service_unavailable_error(error):
                logger.exception(
                    "Ошибка Google Sheets API при выгрузке ФБО-отгрузок без перехода на резервные credentials: table=%s sheet=%s",
                    self.table_title,
                    self.sheet_title,
                )
                raise

            logger.warning(
                "Google Sheets API временно недоступен (503). Повторяем выгрузку ФБО-отгрузок через резервные credentials: %s",
                self.fallback_credentials_path,
            )
            try:
                self._upload_with_credentials(
                    dataframe=dataframe,
                    credentials_path=self.fallback_credentials_path,
                )
                logger.info(
                    "Резервная выгрузка ФБО-отгрузок завершилась успешно: table=%s sheet=%s rows=%s",
                    self.table_title,
                    self.sheet_title,
                    len(dataframe.index),
                )
            except Exception:
                logger.exception(
                    "Обе попытки выгрузки ФБО-отгрузок в Google Sheets завершились ошибкой: table=%s sheet=%s",
                    self.table_title,
                    self.sheet_title,
                )
                raise

    def _upload_with_credentials(
        self,
        dataframe: pd.DataFrame,
        credentials_path: Path,
    ) -> None:
        logger.info("Используем credentials для выгрузки ФБО-отгрузок: %s", credentials_path)
        google_tabs = GoogleTabs(
            table_title=self.table_title,
            sheet_title=self.sheet_title,
            creds_file=credentials_path,
        )
        # Пишем тем же способом, что и в calculation_of_purchases_russia:
        # без предварительной ручной очистки листа через batch_clear.
        google_tabs.set_df_to_google(dataframe.copy())
        logger.info(
            "Данные по ФБО-отгрузкам записаны в Google Sheets: table=%s sheet=%s rows=%s",
            self.table_title,
            self.sheet_title,
            len(dataframe.index),
        )

    @staticmethod
    def _is_service_unavailable_error(error: gspread.exceptions.APIError) -> bool:
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            return status_code == 503
        return "[503]" in str(error)
