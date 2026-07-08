from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.unit.config import GoogleSheetConfig, unit_gs, unit_gs_test
from src_oop.jobs.unit.queries import query_adv_spend

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class UnitSheets:
    main: GoogleSheetConfig
    test: GoogleSheetConfig


class UnitEconomics:
    def __init__(self) -> None:
        self.database = Database()
        self.sheets = UnitSheets(main=unit_gs, test=unit_gs_test)
        self.google_table = self.sheets.main.title
        self.sheet = self.sheets.main.sheet
        self.google_connect = GoogleTabs(self.google_table, self.sheet)
        self.test_google_table = self.sheets.test.title
        self.test_sheet = self.sheets.test.sheet
        self.google_connect_test = GoogleTabs(self.test_google_table, self.test_sheet)

    def get_adv_spend(self) -> list[dict[str, object]]:
        """Получаем данные о рекламных расходах по статьям за вчерашний день."""
        return self.database.read_sql_to_dict(query_adv_spend)

    @staticmethod
    def _build_dataframe_from_sheet(values: list[list[str]]) -> pd.DataFrame:
        if not values:
            return pd.DataFrame()

        headers = values[0]
        rows = values[1:]
        return pd.DataFrame(rows, columns=headers)

    def get_unit_dataframe(
        self,
        *,
        use_test_sheet: bool = False,
        required_columns: list[str] | tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        connector = self.google_connect_test if use_test_sheet else self.google_connect
        values = connector.sheet_title.get_all_values()
        dataframe = self._build_dataframe_from_sheet(values)

        if required_columns is not None:
            missing_columns = [
                column for column in required_columns if column not in dataframe.columns
            ]
            if missing_columns:
                raise ValueError(
                    "В UNIT-таблице отсутствуют обязательные колонки: "
                    f"{', '.join(missing_columns)}"
                )

        logger.info(
            "UNIT sheet loaded: table=%s, sheet=%s, rows=%s",
            connector.table_title,
            connector.sheet_title.title,
            len(dataframe),
        )
        return dataframe
