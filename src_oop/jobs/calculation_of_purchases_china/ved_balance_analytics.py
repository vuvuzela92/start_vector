import logging
import re
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from src_oop.core.my_gspread import GoogleTabs
from src_oop.core.utils_general import clean_currency_value
from src_oop.jobs.calculation_of_purchases_china.config import (
    LOGISTICS_VED_REQUIRED_COLUMNS,
    ORDERS_WHITE_UPDATED_AT_COLUMN,
    VED_DIGIT_COLS,
    VED_PAYMENT_CONFIGS,
    delivery_calculation_china,
    logistics_ved,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class VedAlignmentResult:
    """
    Результат выравнивания VED-данных под структуру платежной аналитики.

    Атрибуты:
        df_aligned:
            DataFrame после приведения к целевому набору и порядку колонок.
        missing_columns:
            Колонки, которых не хватало в VED-данных и которые были добавлены.
        extra_columns:
            Колонки, которые есть в VED, но отсутствуют в целевой структуре.
    """

    df_aligned: pd.DataFrame
    missing_columns: list[str]
    extra_columns: list[str]


class VedBalanceAnalyticsService:
    """
    Формирует платежную аналитику по листу ВЭД и подготавливает ее
    к объединению с аналитикой по белым заказам.

    Сервис:
    - читает лист `Логистика ВЭД 2026 / ОТЧЁТ`;
    - нормализует заголовки и приводит денежные колонки к числам;
    - разворачивает каждую строку источника в набор платежных этапов
      по `VED_PAYMENT_CONFIGS`;
    - исключает VED-этапы без реальной суммы оплаты;
    - рассчитывает `Оплачено` / `Не_оплачено`;
    - готовит объединенный результат к тестовой или production-выгрузке.
    """

    BASE_COLUMNS_MAP = {
        "Логист": "Закупщик",
        "№ ПРОФОРМЫ": "№ проформы в документах и 1С",
        "Статус": "Статус",
        "АРТИКУЛЫ": "wild",
        "НАИМЕНОВАНИЕ ТОВАРА": "Модель",
        "Юр лицо": "КОМПАНИЯ",
        "КОЛИЧЕСТВО ШТУК": "Кол-во к заказу",
        "ТРАНСПОРТНАЯ КОМПАНИЯ": "Поставщик",
    }

    DEFAULT_COLUMNS = {
        "Номер заказа 1С": pd.NA,
        "ФИН Статус": pd.NA,
        "Дата_аванса_по_годовому_плану": pd.NA,
        "Дата_факт": pd.NA,
        "%_оплаты": pd.NA,
    }

    @staticmethod
    def get_duplicate_risk_stage_numbers() -> list[int]:
        """Возвращает номера этапов VED с совпадающими mappings исходных колонок."""
        seen_column_maps: dict[tuple[tuple[str, str], ...], int] = {}
        duplicate_stage_numbers: list[int] = []

        for payment_config in VED_PAYMENT_CONFIGS:
            stage_number = int(payment_config["Номер этапа платежа"])
            column_map = tuple(sorted(payment_config["columns"].items()))
            if column_map in seen_column_maps:
                duplicate_stage_numbers.append(stage_number)
                duplicate_stage_numbers.append(seen_column_maps[column_map])
                continue

            seen_column_maps[column_map] = stage_number

        return sorted(set(duplicate_stage_numbers))

    def __init__(self) -> None:
        """Сохраняет параметры исходного и целевого листов без немедленного подключения."""
        self._table_name = logistics_ved.get("title")
        self._source_sheet_name = logistics_ved.get("report_sheet")
        self._target_table_name = delivery_calculation_china.get("title")
        self._target_sheet_name = delivery_calculation_china.get("test_sheet")

        self._source_conn: GoogleTabs | None = None
        self._target_connections: dict[tuple[str, str], GoogleTabs] = {}

    @property
    def source_connect(self) -> GoogleTabs:
        """Лениво создает и возвращает подключение к исходному листу ВЭД."""
        if self._source_conn is None:
            self._source_conn = GoogleTabs(
                table_title=self._table_name,
                sheet_title=self._source_sheet_name,
            )
        return self._source_conn

    @property
    def target_connect(self) -> GoogleTabs:
        """Возвращает подключение к тестовому листу для выгрузки результата."""
        return self.get_target_connect(
            target_table_name=self._target_table_name,
            target_sheet_name=self._target_sheet_name,
        )

    def get_target_connect(self, target_table_name: str, target_sheet_name: str) -> GoogleTabs:
        """Возвращает подключение к указанной таблице и листу Google Sheets."""
        connection_key = (target_table_name, target_sheet_name)
        if connection_key not in self._target_connections:
            self._target_connections[connection_key] = GoogleTabs(
                table_title=target_table_name,
                sheet_title=target_sheet_name,
            )
        return self._target_connections[connection_key]

    @staticmethod
    def normalize_column_name(column_name: object) -> str:
        """Нормализует имя колонки: убирает переносы строк, табы и лишние пробелы."""
        normalized = str(column_name).replace("\t", " ").replace("\n", " ").replace("\r", " ")
        return re.sub(r"\s+", " ", normalized).strip()

    @staticmethod
    def make_unique_column_names(column_names: list[str]) -> list[str]:
        """Делает названия колонок уникальными, добавляя числовой суффикс к дублям."""
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
        """Проверяет, что в DataFrame присутствуют все обязательные колонки."""
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            available_columns = df.columns.tolist()
            raise ValueError(
                "Не найдены обязательные колонки: "
                f"{missing_columns}. Доступные колонки после нормализации: {available_columns}"
            )

    def load_source_data(self) -> pd.DataFrame:
        """
        Загружает исходный лист ВЭД и приводит его к рабочему виду.

        В листе заголовки находятся на третьей строке, поэтому при чтении
        используется отдельная логика выбора строки заголовков. Числовые
        колонки очищаются сразу после загрузки, чтобы дальше VED-пайплайн
        работал уже с числами, а не со строками из Google Sheets.
        """
        # В листе ВЭД заголовки находятся на третьей строке, а данные идут ниже.
        values = self.source_connect.sheet_title.get_all_values()
        if len(values) < 3:
            raise ValueError("В листе меньше 3 строк, строка заголовков не найдена.")

        headers = self.make_unique_column_names(
            [self.normalize_column_name(header) for header in values[2]]
        )
        df = pd.DataFrame(values[3:], columns=headers)

        # Числовые поля из Google Sheets приходят строками, поэтому приводим их
        # к рабочему виду заранее.
        for column in VED_DIGIT_COLS:
            if column in df.columns:
                df[column] = df[column].apply(clean_currency_value)

        return df

    def prepare_source_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Готовит базовый VED-DataFrame и приводит общие колонки к общему формату.

        На этом этапе сохраняются только нужные для VED-аналитики исходные
        колонки. Общие идентификационные поля переименовываются так, чтобы
        их можно было затем без дополнительных преобразований объединить
        с white-аналитикой.
        """
        self.validate_required_columns(df, LOGISTICS_VED_REQUIRED_COLUMNS)
        self.validate_required_columns(df, list(self.BASE_COLUMNS_MAP.keys()))

        df_orders = df.loc[:, LOGISTICS_VED_REQUIRED_COLUMNS].copy()
        df_orders = df_orders.rename(columns=self.BASE_COLUMNS_MAP)

        # Добавляем недостающие колонки, которые есть в white-аналитике,
        # но отсутствуют в источнике ВЭД по своей природе.
        for column_name, default_value in self.DEFAULT_COLUMNS.items():
            if column_name not in df_orders.columns:
                df_orders[column_name] = default_value

        return df_orders

    def build_payment_dataframe(
        self,
        df_source: pd.DataFrame,
        df_base: pd.DataFrame,
        payment_config: dict[str, object],
    ) -> pd.DataFrame:
        """
        Разворачивает один VED-этап в строки платежного DataFrame.

        Для каждой исходной строки создается отдельная строка этапа оплаты.
        Заполняются только те унифицированные поля, которые явно описаны
        в конфигурации текущего этапа. Остальные платежные поля остаются
        пустыми, чтобы итоговая структура оставалась совместимой с white-
        аналитикой, но не создавала фиктивных значений.
        """
        payment_columns = payment_config["columns"]
        self.validate_required_columns(df_source, list(payment_columns.values()))

        df_payment = df_base.copy()
        df_payment["_Порядок исходной строки"] = range(len(df_payment))
        stage_name = str(payment_config["Этап платежа"]).strip()
        df_payment["Этап платежа"] = stage_name
        df_payment["Номер этапа платежа"] = payment_config["Номер этапа платежа"]

        for target_column, source_column in payment_columns.items():
            df_payment[target_column] = df_source[source_column].to_numpy()

        # Для этапа брокерского оформления в аналитике важен не общий
        # поставщик перевозки, а конкретный таможенный брокер из источника.
        if stage_name == "Брокерское оформление":
            df_payment["Поставщик"] = df_source["ТАМОЖЕННЫЙ БРОКЕР"].to_numpy()

        if "Сумма_оплаты" not in df_payment.columns:
            df_payment["Сумма_оплаты"] = pd.NA
        if "Дата_план" not in df_payment.columns:
            df_payment["Дата_план"] = pd.NA
        if "Дата_платеж_календарь" not in df_payment.columns:
            df_payment["Дата_платеж_календарь"] = pd.NA
        if "Статус_по_этапу" not in df_payment.columns:
            df_payment["Статус_по_этапу"] = pd.NA

        df_payment["Дата_аванса_по_годовому_плану"] = pd.NA
        df_payment["Дата_факт"] = pd.NA
        df_payment["%_оплаты"] = pd.NA
        return df_payment

    def build_balance_dataframe(self, df_source: pd.DataFrame) -> pd.DataFrame:
        """
        Собирает итоговый `ved_balance_df` по всем этапам платежей VED.

        После разворота этапов сервис оставляет только строки с реальной
        `Сумма_оплаты > 0`. Для VED это важное бизнес-правило: этапы с пустой
        или нулевой суммой не должны попадать в итоговую платежную аналитику,
        даже если у них заполнены дата или статус.
        """
        df_base = self.prepare_source_dataframe(df_source)

        payment_frames = [
            self.build_payment_dataframe(
                df_source=df_source,
                df_base=df_base,
                payment_config=payment_config,
            )
            for payment_config in VED_PAYMENT_CONFIGS
        ]

        df_balance = pd.concat(payment_frames, ignore_index=True)
        payment_amount = pd.to_numeric(df_balance["Сумма_оплаты"], errors="coerce")
        rows_before_amount_filter = len(df_balance)
        positive_amount_mask = payment_amount.notna() & payment_amount.gt(0)
        df_balance = df_balance.loc[positive_amount_mask].copy()
        df_balance["Сумма_оплаты"] = payment_amount.loc[positive_amount_mask].to_numpy()

        logger.info(
            "VED rows before amount filter: %s; after amount filter: %s; removed: %s",
            rows_before_amount_filter,
            len(df_balance),
            rows_before_amount_filter - len(df_balance),
        )

        return df_balance.sort_values(
            by=[
                "№ проформы в документах и 1С",
                "_Порядок исходной строки",
                "Номер этапа платежа",
            ],
            kind="stable",
        ).drop(columns="_Порядок исходной строки").reset_index(drop=True)

    @staticmethod
    def add_payment_status_amounts(df_balance: pd.DataFrame) -> pd.DataFrame:
        """
        Разделяет сумму этапа на колонки `Оплачено` и `Не_оплачено`.

        Этап считается оплаченным, если нормализованный статус равен
        `оплачено` или `оплачен`. Проверка не чувствительна к регистру,
        лишним пробелам и безопасна для пустых значений.
        """
        df_result = df_balance.copy()
        # В исходных таблицах встречаются обе формы статуса: "оплачено"
        # и "оплачен". Для аналитики они считаются эквивалентными.
        paid_mask = (
            df_result["Статус_по_этапу"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"оплачено", "оплачен"})
        )
        payment_amount = pd.to_numeric(df_result["Сумма_оплаты"], errors="coerce").fillna(0)

        df_result["Оплачено"] = payment_amount.where(paid_mask, 0)
        df_result["Не_оплачено"] = payment_amount.where(~paid_mask, 0)
        return df_result

    @staticmethod
    def align_to_balance_columns(
        ved_balance_df: pd.DataFrame,
        balance_columns: list[str],
    ) -> VedAlignmentResult:
        """Приводит VED-DataFrame к набору и порядку колонок `balance_df`."""
        missing_columns = [column for column in balance_columns if column not in ved_balance_df.columns]
        extra_columns = [column for column in ved_balance_df.columns if column not in balance_columns]

        df_aligned = ved_balance_df.copy()
        for column in missing_columns:
            df_aligned[column] = pd.NA

        return VedAlignmentResult(
            df_aligned=df_aligned.loc[:, balance_columns],
            missing_columns=missing_columns,
            extra_columns=extra_columns,
        )

    @staticmethod
    def build_duplicate_risk_report(ved_balance_df: pd.DataFrame) -> pd.DataFrame:
        """Строит диагностический отчет по потенциальным дублям в VED-этапах."""
        risk_columns = [
            "№ проформы в документах и 1С",
            "wild",
            "Модель",
            "Статус_по_этапу",
            "Дата_план",
            "Дата_платеж_календарь",
            "Сумма_оплаты",
        ]
        available_columns = [column for column in risk_columns if column in ved_balance_df.columns]
        if not available_columns:
            return pd.DataFrame()

        duplicate_stage_numbers = VedBalanceAnalyticsService.get_duplicate_risk_stage_numbers()
        if not duplicate_stage_numbers:
            return pd.DataFrame(columns=available_columns + ["Количество дублей"])

        df_risk = ved_balance_df[
            ved_balance_df["Номер этапа платежа"].isin(duplicate_stage_numbers)
        ].copy()
        if df_risk.empty:
            return pd.DataFrame(columns=available_columns + ["Количество дублей"])

        return (
            df_risk.groupby(available_columns, dropna=False)
            .size()
            .reset_index(name="Количество дублей")
            .query("`Количество дублей` > 1")
            .sort_values("Количество дублей", ascending=False)
            .reset_index(drop=True)
        )

    @staticmethod
    def _parse_date_series(date_series: pd.Series) -> pd.Series:
        """Парсит серию дат в формате day.month.year."""
        return pd.to_datetime(
            date_series,
            errors="coerce",
            dayfirst=True,
            format="mixed",
        )

    @staticmethod
    def _month_from_date_series(date_series: pd.Series) -> pd.Series:
        """Формирует месяц в формате MM-YYYY или пустую строку для пустых дат."""
        return date_series.dt.strftime("%m-%Y").fillna("")

    @staticmethod
    def _build_overdue_bucket_series(overdue_days: pd.Series) -> pd.Series:
        """Строит бакеты по количеству дней просрочки."""
        bucket_series = pd.Series("", index=overdue_days.index, dtype="string")
        bucket_series = bucket_series.mask(
            overdue_days.between(1, 10, inclusive="both").fillna(False),
            "до 10 дней",
        )
        bucket_series = bucket_series.mask(
            overdue_days.between(11, 30, inclusive="both").fillna(False),
            "до 30 дней",
        )
        bucket_series = bucket_series.mask(
            overdue_days.between(31, 40, inclusive="both").fillna(False),
            "30-40 дней",
        )
        bucket_series = bucket_series.mask(
            overdue_days.between(41, 50, inclusive="both").fillna(False),
            "40-50 дней",
        )
        bucket_series = bucket_series.mask(
            overdue_days.between(51, 60, inclusive="both").fillna(False),
            "50-60 дней",
        )
        bucket_series = bucket_series.mask(
            overdue_days.ge(61).fillna(False),
            "более 60 дней",
        )
        return bucket_series

    @staticmethod
    def prepare_dataframe_for_upload(df_balance: pd.DataFrame) -> pd.DataFrame:
        """
        Добавляет служебные колонки перед выгрузкой в Google Sheets.

        На этом этапе:
        - рассчитываются месяцы по календарной и плановой датам в формате
          `MM-YYYY`;
        - считается количество дней просрочки и бакет просрочки;
        - для прогнозных этапов корректируется поле `Поставщик`, чтобы в
          сводных таблицах они не выглядели как товарные поставщики;
        - добавляется метка времени обновления.
        """
        df_upload = df_balance.copy()
        payment_calendar_dates = VedBalanceAnalyticsService._parse_date_series(
            df_upload["Дата_платеж_календарь"]
        )
        plan_dates = VedBalanceAnalyticsService._parse_date_series(
            df_upload["Дата_план"]
        )
        overdue_days = (payment_calendar_dates - plan_dates).dt.days.astype("Int64")

        df_upload["Месяц_плат_календарь"] = (
            VedBalanceAnalyticsService._month_from_date_series(payment_calendar_dates)
        )
        df_upload["Месяц_дата_план"] = (
            VedBalanceAnalyticsService._month_from_date_series(plan_dates)
        )
        df_upload["Дней_просрочки"] = overdue_days
        df_upload["Признак_просрочки"] = (
            VedBalanceAnalyticsService._build_overdue_bucket_series(overdue_days)
        )

        # Прогнозные этапы не относятся к реальному поставщику товара, поэтому
        # для аналитики им задаются отдельные технические значения поставщика.
        custom_supplier_by_stage = {
            "Таможня прогноз": "Таможня",
            "Логистика прогноз": "Логистика",
        }
        stage_normalized = df_upload["Этап платежа"].fillna("").astype(str).str.strip()
        custom_supplier_mask = stage_normalized.isin(custom_supplier_by_stage)
        df_upload.loc[custom_supplier_mask, "Поставщик"] = (
            stage_normalized[custom_supplier_mask].map(custom_supplier_by_stage)
        )

        df_upload[ORDERS_WHITE_UPDATED_AT_COLUMN] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        service_columns = [
            "Месяц_плат_календарь",
            "Месяц_дата_план",
            "Дней_просрочки",
            "Признак_просрочки",
            ORDERS_WHITE_UPDATED_AT_COLUMN,
        ]
        base_columns = [
            column
            for column in df_upload.columns
            if column not in {"Месяц", *service_columns}
        ]
        df_upload = df_upload.loc[:, base_columns + service_columns]
        return df_upload

    def upload_to_test_sheet(self, df_upload: pd.DataFrame) -> None:
        """Выгружает подготовленный DataFrame в тестовый лист `test_sheet`."""
        self.upload_to_sheet(
            df_upload=df_upload,
            target_table_name=self._target_table_name,
            target_sheet_name=self._target_sheet_name,
        )

    def upload_to_sheet(
        self,
        df_upload: pd.DataFrame,
        target_table_name: str,
        target_sheet_name: str,
    ) -> None:
        """Выгружает подготовленный DataFrame в указанную таблицу и лист Google Sheets."""
        if df_upload.empty:
            logger.warning(
                "combined_balance_df пустой. Выгрузка в лист %s пропущена.",
                target_sheet_name,
            )
            return

        self.get_target_connect(
            target_table_name=target_table_name,
            target_sheet_name=target_sheet_name,
        ).set_df_to_google(df_upload)
        logger.info(
            "combined_balance_df выгружен в таблицу %s на лист %s.",
            target_table_name,
            target_sheet_name,
        )

    def run(self) -> pd.DataFrame:
        """
        Выполняет полный VED-пайплайн и возвращает итоговый DataFrame.

        Метод не выполняет выгрузку сам по себе: это позволяет безопасно
        использовать результат и в test-, и в production-сценариях объединения.
        """
        df_source = self.load_source_data()
        df_balance = self.build_balance_dataframe(df_source)
        return self.add_payment_status_amounts(df_balance)
