import logging
import re
from datetime import datetime

import pandas as pd
from pandas.api.types import is_numeric_dtype

from src_oop.core.my_gspread import GoogleTabs
from src_oop.core.utils_general import clean_currency_value
from src_oop.jobs.calculation_of_purchases_china.config import (
    ORDERS_WHITE_CANCELED_STATUSES,
    ORDERS_WHITE_DIGIT_COLS,
    ORDERS_WHITE_PAYMENT_CONFIGS,
    ORDERS_WHITE_REQUIRED_COLUMNS,
    ORDERS_WHITE_UPDATED_AT_COLUMN,
    delivery_calculation_china,
)

logger = logging.getLogger(__name__)

PAYMENT_UNIFIED_COLUMNS = [
    "Статус_по_этапу",
    "Дата_аванса_по_годовому_плану",
    "Дата_план",
    "Дата_платеж_календарь",
    "Дата_факт",
    "%_оплаты",
    "Сумма_оплаты",
]


class OrdersWhiteBalanceAnalyticsService:
    """Формирует и выгружает аналитику платежей по листу заказов белых."""

    def __init__(self) -> None:
        self._table_name = delivery_calculation_china.get("title")
        self._source_sheet_name = delivery_calculation_china.get("white_orders_sheet")
        self._target_sheet_name = delivery_calculation_china.get("payments_analyze_sheet")

        self._source_conn: GoogleTabs | None = None
        self._target_conn: GoogleTabs | None = None

    @property
    def source_connect(self) -> GoogleTabs:
        if self._source_conn is None:
            self._source_conn = GoogleTabs(
                table_title=self._table_name,
                sheet_title=self._source_sheet_name,
            )
        return self._source_conn

    @property
    def target_connect(self) -> GoogleTabs:
        if self._target_conn is None:
            self._target_conn = GoogleTabs(
                table_title=self._table_name,
                sheet_title=self._target_sheet_name,
            )
        return self._target_conn

    @staticmethod
    def normalize_column_name(column_name: object) -> str:
        normalized = str(column_name).replace("\t", " ").replace("\n", " ").replace("\r", " ")
        return re.sub(r"\s+", " ", normalized).strip()

    @staticmethod
    def make_unique_column_names(column_names: list[str]) -> list[str]:
        seen_columns: dict[str, int] = {}
        unique_columns: list[str] = []

        for column_name in column_names:
            seen_columns[column_name] = seen_columns.get(column_name, 0) + 1
            if seen_columns[column_name] == 1:
                unique_columns.append(column_name)
                continue

            unique_columns.append(f"{column_name} {seen_columns[column_name]}")

        return unique_columns

    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            available_columns = df.columns.tolist()
            raise ValueError(
                "Не найдены обязательные колонки: "
                f"{missing_columns}. Доступные колонки после нормализации: {available_columns}"
            )

    def load_source_data(self) -> pd.DataFrame:
        values = self.source_connect.sheet_title.get_all_values()
        if len(values) < 4:
            raise ValueError("В листе меньше 4 строк, строка заголовков не найдена.")

        headers = self.make_unique_column_names(
            [self.normalize_column_name(header) for header in values[3]]
        )
        df = pd.DataFrame(values[4:], columns=headers)

        for column in ORDERS_WHITE_DIGIT_COLS:
            df[column] = df[column].apply(clean_currency_value)

        return df

    def prepare_orders_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        self.validate_required_columns(df, ORDERS_WHITE_REQUIRED_COLUMNS)

        df_orders = df.loc[:, ORDERS_WHITE_REQUIRED_COLUMNS].copy()
        return df_orders[~df_orders["Статус"].isin(ORDERS_WHITE_CANCELED_STATUSES)]

    @staticmethod
    def get_payment_source_columns() -> set[str]:
        return {
            source_column
            for payment_config in ORDERS_WHITE_PAYMENT_CONFIGS
            for source_column in payment_config["columns"].values()
        }

    @classmethod
    def get_order_id_columns(cls) -> list[str]:
        payment_columns = cls.get_payment_source_columns()
        return [
            column
            for column in ORDERS_WHITE_REQUIRED_COLUMNS
            if column not in payment_columns
        ]

    @staticmethod
    def _is_not_empty_payment_value(series: pd.Series) -> pd.Series:
        if is_numeric_dtype(series):
            return series.notna()

        normalized = series.fillna("").astype(str).str.strip()
        return normalized.ne("") & normalized.ne("nan")

    @classmethod
    def get_non_empty_payment_mask(cls, df_balance: pd.DataFrame) -> pd.Series:
        amount_not_empty = cls._is_not_empty_payment_value(df_balance["Сумма_оплаты"])
        calendar_not_empty = cls._is_not_empty_payment_value(df_balance["Дата_платеж_календарь"])
        return amount_not_empty | calendar_not_empty

    def build_payment_dataframe(
        self,
        df: pd.DataFrame,
        base_columns: list[str],
        payment_config: dict[str, object],
    ) -> pd.DataFrame:
        payment_columns = payment_config["columns"]
        self.validate_required_columns(df, base_columns + list(payment_columns.values()))

        df_payment = df.loc[:, base_columns].copy()
        df_payment["_Порядок исходной строки"] = range(len(df))
        df_payment["Этап платежа"] = payment_config["Этап платежа"]
        df_payment["Номер этапа платежа"] = payment_config["Номер этапа платежа"]

        for unified_column in PAYMENT_UNIFIED_COLUMNS:
            source_column = payment_columns.get(unified_column)
            if source_column is None:
                df_payment[unified_column] = pd.NA
                continue

            df_payment[unified_column] = df[source_column].to_numpy()

        return df_payment

    def build_balance_dataframe(self, df_orders: pd.DataFrame) -> pd.DataFrame:
        base_columns = self.get_order_id_columns()
        payment_frames = [
            self.build_payment_dataframe(
                df=df_orders,
                base_columns=base_columns,
                payment_config=payment_config,
            )
            for payment_config in ORDERS_WHITE_PAYMENT_CONFIGS
        ]

        df_balance = pd.concat(payment_frames, ignore_index=True)
        df_balance = df_balance[self.get_non_empty_payment_mask(df_balance)].copy()

        return df_balance.sort_values(
            by=["Номер заказа 1С", "_Порядок исходной строки", "Номер этапа платежа"],
            kind="stable",
        ).drop(columns="_Порядок исходной строки").reset_index(drop=True)

    @staticmethod
    def add_payment_status_amounts(df_balance: pd.DataFrame) -> pd.DataFrame:
        df_result = df_balance.copy()
        paid_mask = (
            df_result["Статус_по_этапу"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("оплачено")
        )
        payment_amount = pd.to_numeric(df_result["Сумма_оплаты"], errors="coerce").fillna(0)

        df_result["Оплачено"] = payment_amount.where(paid_mask, 0)
        df_result["Не_оплачено"] = payment_amount.where(~paid_mask, 0)
        return df_result

    @staticmethod
    def prepare_dataframe_for_upload(df_balance: pd.DataFrame) -> pd.DataFrame:
        df_upload = df_balance.copy()
        df_upload[ORDERS_WHITE_UPDATED_AT_COLUMN] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df_upload

    def upload_to_google_sheet(self, df_upload: pd.DataFrame) -> None:
        if df_upload.empty:
            logger.warning("df_orders_white_balance пустой. Выгрузка в Google Sheets пропущена.")
            return

        self.target_connect.set_df_to_google(df_upload)
        logger.info("df_orders_white_balance выгружен на лист %s.", self._target_sheet_name)

    def run(self, upload: bool = True) -> pd.DataFrame:
        df_source = self.load_source_data()
        df_orders = self.prepare_orders_dataframe(df_source)
        df_balance = self.build_balance_dataframe(df_orders)
        df_balance = self.add_payment_status_amounts(df_balance)
        df_upload = self.prepare_dataframe_for_upload(df_balance)
        if upload:
            self.upload_to_google_sheet(df_upload)
        return df_balance
