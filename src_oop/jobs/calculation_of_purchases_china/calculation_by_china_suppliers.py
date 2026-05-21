import logging

import pandas as pd

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.annual_procurement_plan.config import annual_procurement_plan
from src_oop.jobs.calculation_of_purchases_china.config import (
    ANNUAL_PLAN_COLUMNS,
    delivery_calculation_china,
)

logger = logging.getLogger(__name__)


class CalculationByChinaSuppliers:
    """Переносит поквартальный годовой план в сводную таблицу по поставщикам."""

    def __init__(self) -> None:
        self._source_table_name = annual_procurement_plan.get("title")
        self._source_sheet_name = annual_procurement_plan.get("quarter_sheet")
        self._target_table_name = delivery_calculation_china.get("title")
        self._target_sheet_name = delivery_calculation_china.get("db_sheet_quarterly")

        self._source_conn = None
        self._target_conn = None

    @property
    def source_connect(self) -> GoogleTabs:
        if self._source_conn is None:
            self._source_conn = GoogleTabs(
                self._source_table_name,
                self._source_sheet_name,
            )
        return self._source_conn

    @property
    def target_connect(self) -> GoogleTabs:
        if self._target_conn is None:
            self._target_conn = GoogleTabs(
                self._target_table_name,
                self._target_sheet_name,
            )
        return self._target_conn

    def get_quarterly_plan_data(self) -> pd.DataFrame:
        """Читает поквартальный план: заголовки в 4-й строке, данные с 5-й."""
        data = self.source_connect.sheet_title.get_all_values()

        if len(data) < 4:
            logger.warning("В исходном листе не найдена строка с заголовками.")
            return pd.DataFrame(columns=ANNUAL_PLAN_COLUMNS)

        # Структура листа задана бизнес-таблицей: 4-я строка - заголовки.
        headers = data[3]
        rows = data[4:]

        if not rows:
            logger.warning("В исходном листе не найдены строки с данными.")
            return pd.DataFrame(columns=ANNUAL_PLAN_COLUMNS)

        df = pd.DataFrame(rows, columns=headers)
        return self._apply_annual_plan_columns(df)

    @staticmethod
    def _apply_annual_plan_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Приводит данные к фиксированному набору и порядку колонок."""
        for column in ANNUAL_PLAN_COLUMNS:
            if column not in df.columns:
                df[column] = ""

        return df.loc[:, ANNUAL_PLAN_COLUMNS]

    @staticmethod
    def set_data(connector: GoogleTabs, df: pd.DataFrame) -> None:
        # Используем общий метод проекта: он добавляет колонку updated_at.
        connector.set_df_to_google(df)
