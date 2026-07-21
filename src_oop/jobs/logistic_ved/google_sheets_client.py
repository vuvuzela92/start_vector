from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import gspread
import pandas as pd

from src_oop.core.my_gspread import GoogleTabs

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LogisticVedGoogleSheetsClient:
    """Слой записи подготовленного DataFrame в Google Sheets."""

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
            "Подготовка записи logistic_ved в Google Sheets: table=%s sheet=%s rows=%s",
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
                    "Ошибка Google Sheets API без fallback в logistic_ved: table=%s sheet=%s",
                    self.table_title,
                    self.sheet_title,
                )
                raise

            logger.warning(
                "Google Sheets API вернул 503. Повторяем выгрузку через альтернативные credentials: %s",
                self.fallback_credentials_path,
            )
            try:
                self._upload_with_credentials(
                    dataframe=dataframe,
                    credentials_path=self.fallback_credentials_path,
                )
                logger.info(
                    "Альтернативная выгрузка logistic_ved завершилась успешно: table=%s sheet=%s rows=%s",
                    self.table_title,
                    self.sheet_title,
                    len(dataframe.index),
                )
            except Exception:
                logger.exception(
                    "Обе попытки выгрузки logistic_ved в Google Sheets завершились ошибкой: table=%s sheet=%s",
                    self.table_title,
                    self.sheet_title,
                )
                raise

    def _upload_with_credentials(
        self,
        dataframe: pd.DataFrame,
        credentials_path: Path,
    ) -> None:
        logger.info(
            "Используется credential-попытка logistic_ved: %s",
            credentials_path,
        )
        google_tabs = GoogleTabs(
            table_title=self.table_title,
            sheet_title=self.sheet_title,
            creds_file=credentials_path,
        )
        logger.info(
            "Запись logistic_ved выполняется через GoogleTabs.set_df_to_google без предварительного batch_clear."
        )
        google_tabs.set_df_to_google(dataframe.copy())
        logger.info(
            "Данные logistic_ved записаны в Google Sheets: table=%s sheet=%s rows=%s",
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
