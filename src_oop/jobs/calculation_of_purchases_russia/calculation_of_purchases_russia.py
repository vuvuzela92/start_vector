from __future__ import annotations

import logging

import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.calculation_of_purchases_russia.config import (
    google_table,
    penalties_column_name,
    query_orders_and_supply,
    query_penalties_and_virtual_stock,
    unit_table,
    virtual_stock_column_name,
)

logger = logging.getLogger(__name__)


class Calculation_of_purchases_russia:
    def __init__(self) -> None:
        self.db = Database()
        self.engine = Database.get_engine()

        self.google_table = google_table["title"]
        self.sheet = google_table["orders_sheet"]
        self.google_connect = GoogleTabs(self.google_table, self.sheet)

        self.statuses_sheet = google_table["statuses_sheet"]
        self.google_connect_statuses = GoogleTabs(self.google_table, self.statuses_sheet)

        self._purchase_russia_table = google_table["title"]
        self._orders_buyers_sheet = google_table["orders_buyers_sheet"]
        self._calculate_sheet = google_table["calculate_sheet"]
        self._unit_table_title = unit_table["title"]
        self._unit_sheet_title = unit_table["sheet_unit"]

        self._conn_purchase_russia: GoogleTabs | None = None
        self._conn_calculate_sheet: GoogleTabs | None = None
        self._conn_unit_table: GoogleTabs | None = None

    @property
    def google_connect_to_purchsase_russia_table(self) -> GoogleTabs:
        if self._conn_purchase_russia is None:
            self._conn_purchase_russia = GoogleTabs(
                self._purchase_russia_table,
                self._orders_buyers_sheet,
            )
        return self._conn_purchase_russia

    @property
    def google_connect_to_calculate_sheet(self) -> GoogleTabs:
        if self._conn_calculate_sheet is None:
            self._conn_calculate_sheet = GoogleTabs(
                self._purchase_russia_table,
                self._calculate_sheet,
            )
        return self._conn_calculate_sheet

    @property
    def google_connect_to_unit_table(self) -> GoogleTabs:
        if self._conn_unit_table is None:
            self._conn_unit_table = GoogleTabs(
                self._unit_table_title,
                self._unit_sheet_title,
            )
        return self._conn_unit_table

    @staticmethod
    def set_data(coonector: GoogleTabs, df: pd.DataFrame) -> None:
        coonector.set_df_to_google(df)

    @staticmethod
    def _build_dataframe_from_sheet(
        values: list[list[str]],
        header_row_index: int,
        data_row_index: int,
    ) -> pd.DataFrame:
        if len(values) <= header_row_index:
            return pd.DataFrame()

        headers = values[header_row_index]
        rows = values[data_row_index:] if len(values) > data_row_index else []
        return pd.DataFrame(rows, columns=headers)

    def get_orders_and_supplies_data(self) -> pd.DataFrame:
        return Database.read_sql_to_dataframe(query_orders_and_supply)

    def get_penalties_data(self) -> pd.DataFrame:
        return Database.read_sql_to_dataframe(query_penalties_and_virtual_stock)

    def get_purchase_calculation_data(self) -> pd.DataFrame:
        sheet_values = self.google_connect_to_calculate_sheet.sheet_title.get_all_values()
        purchase_df = self._build_dataframe_from_sheet(
            values=sheet_values,
            header_row_index=1,
            data_row_index=2,
        )

        if purchase_df.shape[1] < 10:
            raise ValueError(
                "Лист расчета закупки содержит меньше 10 колонок, "
                "поэтому legacy-логику нельзя перенести без изменения формата."
            )

        return purchase_df.iloc[:, :10].copy()

    def get_unit_virtual_stock_data(self) -> pd.DataFrame:
        sheet_values = self.google_connect_to_unit_table.sheet_title.get_all_values()
        unit_df = self._build_dataframe_from_sheet(
            values=sheet_values,
            header_row_index=0,
            data_row_index=1,
        )

        required_columns = ["wild", "ФБС"]
        missing_columns = [column for column in required_columns if column not in unit_df.columns]
        if missing_columns:
            raise ValueError(
                "В листе UNIT отсутствуют обязательные колонки: "
                f"{', '.join(missing_columns)}"
            )

        unit_df = unit_df[required_columns].copy()
        unit_df["ФБС"] = (
            pd.to_numeric(unit_df["ФБС"].replace("", 0), errors="coerce")
            .fillna(0)
            .astype(int)
        )
        unit_df = unit_df.groupby("wild", as_index=False).agg({"ФБС": "sum"})
        return unit_df.drop_duplicates(subset=["wild"], keep="first")

    def build_penalties_and_virtual_stock_update(self) -> pd.DataFrame:
        purchase_df = self.get_purchase_calculation_data()
        penalties_df = self.get_penalties_data()
        unit_df = self.get_unit_virtual_stock_data()

        merged_df = (
            purchase_df
            .merge(
                penalties_df,
                how="left",
                left_on="wild",
                right_on="local_vendor_code",
            )
            .merge(unit_df, how="left", on="wild")
        )
        merged_df = merged_df.fillna("")
        merged_df[penalties_column_name] = merged_df["sum"]
        merged_df[virtual_stock_column_name] = merged_df["ФБС"]

        return merged_df[[penalties_column_name, virtual_stock_column_name]]

    def update_penalties_and_virtual_stock(self) -> pd.DataFrame:
        update_df = self.build_penalties_and_virtual_stock_update()
        worksheet = self.google_connect_to_calculate_sheet.sheet_title
        start_update = worksheet.find(penalties_column_name)
        start_cell = rowcol_to_a1(start_update.row + 1, start_update.col)
        values = update_df.values.tolist()

        if not values:
            logger.info("No rows to update for penalties and virtual stock.")
            return update_df

        worksheet.update(
            start_cell,
            values,
            value_input_option="USER_ENTERED",
        )
        logger.info(
            "Updated penalties and virtual stock in Google Sheets starting from %s",
            start_cell,
        )
        return update_df
