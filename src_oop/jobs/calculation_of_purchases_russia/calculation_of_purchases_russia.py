from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.calculation_of_purchases_russia.config import (
    google_table,
    penalties_column_name,
    query_orders_and_supply,
    query_penalties_and_virtual_stock,
    query_supplies_1c,
    supply_1c_output_columns,
    supply_1c_columns_rename,
    unit_table,
    virtual_stock_column_name,
)

logger = logging.getLogger(__name__)


class Calculation_of_purchases_russia:
    """Собирает и публикует связанные витрины таблицы «Расчет закупки Россия».

    Бизнес-сценарий:
    класс объединяет несколько регламентных выгрузок для закупочного контура
    России: загрузку заказов, витрину заказов и поступлений, точечное
    обновление штрафов и виртуальных остатков, а также перенос приходов 1С в
    отдельный лист Google Sheets без изменения пользовательской структуры
    таблицы.
    """

    def __init__(self) -> None:
        """Инициализирует подключения к БД и конфигурацию листов модуля.

        Бизнес-сценарий:
        все джобы этого контура работают с одной и той же Google-таблицей и
        связанными листами, поэтому базовая конфигурация собирается в одном
        месте, чтобы новые сценарии не дублировали названия вкладок и способ
        подключения.
        """

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
        self._supplies_1c_sheet = google_table["supplies_1c_sheet"]
        self._unit_table_title = unit_table["title"]
        self._unit_sheet_title = unit_table["sheet_unit"]

        self._conn_purchase_russia: GoogleTabs | None = None
        self._conn_calculate_sheet: GoogleTabs | None = None
        self._conn_unit_table: GoogleTabs | None = None
        self._conn_supplies_1c_sheet: GoogleTabs | None = None

    @property
    def google_connect_to_purchsase_russia_table(self) -> GoogleTabs:
        """Возвращает подключение к листу заказов и поступлений.

        Бизнес-сценарий:
        лист `Заказы_и_поступления` используется как витрина для сводной
        выгрузки заказов и фактических поступлений, поэтому подключение
        создаётся лениво и только в момент фактической публикации данных.
        """

        if self._conn_purchase_russia is None:
            self._conn_purchase_russia = GoogleTabs(
                self._purchase_russia_table,
                self._orders_buyers_sheet,
            )
        return self._conn_purchase_russia

    @property
    def google_connect_to_calculate_sheet(self) -> GoogleTabs:
        """Возвращает подключение к листу расчёта закупки.

        Бизнес-сценарий:
        лист `Расчет закупки` нужен для чтения текущей пользовательской
        раскладки и для точечного обновления производных колонок без полной
        перезаписи остальных блоков витрины.
        """

        if self._conn_calculate_sheet is None:
            self._conn_calculate_sheet = GoogleTabs(
                self._purchase_russia_table,
                self._calculate_sheet,
            )
        return self._conn_calculate_sheet

    @property
    def google_connect_to_unit_table(self) -> GoogleTabs:
        """Возвращает подключение к UNIT для чтения виртуальных остатков.

        Бизнес-сценарий:
        блок штрафов и виртуального склада использует остатки из UNIT, поэтому
        отдельное подключение к этой таблице создаётся только для сценариев,
        которым действительно нужны данные `wild` и `ФБС`.
        """

        if self._conn_unit_table is None:
            self._conn_unit_table = GoogleTabs(
                self._unit_table_title,
                self._unit_sheet_title,
            )
        return self._conn_unit_table

    @property
    def google_connect_to_supplies_1c_sheet(self) -> GoogleTabs:
        """Возвращает подключение к листу `Приходы_1С`.

        Бизнес-сценарий:
        перенос приходов 1С должен писать данные в отдельный лист закупочной
        таблицы, сохраняя legacy-структуру: служебная отметка времени в `A1`,
        а основной набор колонок начиная с `A2`.
        """

        if self._conn_supplies_1c_sheet is None:
            self._conn_supplies_1c_sheet = GoogleTabs(
                self._purchase_russia_table,
                self._supplies_1c_sheet,
            )
        return self._conn_supplies_1c_sheet

    @staticmethod
    def set_data(coonector: GoogleTabs, df: pd.DataFrame) -> None:
        """Публикует DataFrame в целевой лист стандартным способом проекта.

        Бизнес-сценарий:
        метод используется транспортными job-ами, где вся рабочая область
        листа может быть безопасно перезаписана через общий клиент Google
        Sheets с принятыми в проекте правилами нормализации дат и пропусков.
        """

        coonector.set_df_to_google(df)

    @staticmethod
    def _build_dataframe_from_sheet(
        values: list[list[str]],
        header_row_index: int,
        data_row_index: int,
    ) -> pd.DataFrame:
        """Собирает DataFrame из массива значений Google Sheets.

        Бизнес-сценарий:
        часть закупочных листов хранит служебные строки перед заголовками, и
        при переносе legacy-логики важно читать ровно ту строку шапки и тот
        диапазон данных, которые ожидались историческим сценарием.
        """

        if len(values) <= header_row_index:
            return pd.DataFrame()

        headers = values[header_row_index]
        rows = values[data_row_index:] if len(values) > data_row_index else []
        return pd.DataFrame(rows, columns=headers)

    @staticmethod
    def _format_date_columns_for_google(
        df: pd.DataFrame,
        date_columns: list[str],
    ) -> pd.DataFrame:
        """Приводит выбранные колонки дат к legacy-формату `DD.MM.YYYY`.

        Бизнес-сценарий:
        лист `Приходы_1С` исторически ожидает даты в человекочитаемом русском
        формате. Метод также защищает сценарий от `0`, пустых строк и
        некорректных дат, чтобы они не превращались в мусорные значения после
        записи в Google Sheets.
        """

        formatted_df = df.copy()
        for column in date_columns:
            if column not in formatted_df.columns:
                continue
            formatted_df[column] = formatted_df[column].replace(
                ["0", "00.00.0000", 0, ""],
                pd.NA,
            )
            formatted_df[column] = pd.to_datetime(
                formatted_df[column],
                errors="coerce",
            )
            formatted_df[column] = formatted_df[column].dt.strftime("%d.%m.%Y")
            formatted_df[column] = formatted_df[column].fillna("")
        return formatted_df

    @staticmethod
    def _build_sheet_output(df: pd.DataFrame) -> list[list[object]]:
        """Готовит матрицу значений для листа с отдельной строкой заголовков.

        Бизнес-сценарий:
        для листов вида `Приходы_1С` и подобных витрин проект исторически
        публикует заголовки и данные единым update, начиная со строки `A2`,
        поэтому метод формирует итоговый массив в том же формате.
        """

        if df.empty and len(df.columns) == 0:
            return []
        return [df.columns.tolist(), *df.values.tolist()]

    @staticmethod
    def _update_sheet_range(
        connector: GoogleTabs,
        range_name: str,
        values: list[list[object]],
    ) -> None:
        """Записывает произвольный прямоугольный диапазон с retry клиента.

        Бизнес-сценарий:
        некоторые листы нельзя перезаписывать через `set_df_to_google`,
        потому что у них есть служебная строка над заголовками. В таких случаях
        нужна точечная запись в конкретный диапазон с сохранением retry на
        `429` и временные ошибки Google Sheets.
        """

        connector._execute_google_write_with_retry(
            operation_name=f"update_range {connector.sheet_title.title} {range_name}",
            func=connector.sheet_title.update,
            range_name=range_name,
            values=values,
            value_input_option="USER_ENTERED",
        )

    @staticmethod
    def _build_updated_label() -> str:
        """Возвращает служебную отметку времени для первой строки листа.

        Бизнес-сценарий:
        лист `Приходы_1С` исторически показывает пользователю момент последнего
        обновления в ячейке `A1`, и этот индикатор нужен для ручной проверки
        свежести данных после выполнения регламентной job.
        """

        return f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"

    def get_orders_and_supplies_data(self) -> pd.DataFrame:
        """Читает свод заказов и поступлений для листа `Заказы_и_поступления`.

        Бизнес-сценарий:
        этот метод обслуживает витрину, где бизнес сопоставляет заказы
        поставщикам с фактическими поступлениями и возвратами без ручного
        объединения нескольких выгрузок.
        """

        return Database.read_sql_to_dataframe(query_orders_and_supply)

    def get_penalties_data(self) -> pd.DataFrame:
        """Читает штрафы по браку, невыполнению и подмене за 7 дней.

        Бизнес-сценарий:
        данные нужны для расчётного листа закупки, где пользователи видят
        суммарные штрафы рядом с рабочими колонками по каждому `wild`.
        """

        return Database.read_sql_to_dataframe(query_penalties_and_virtual_stock)

    def get_supplies_1c_data(self) -> pd.DataFrame:
        """Читает из БД валидные приходы 1С для публикации в Google Sheets.

        Бизнес-сценарий:
        метод переносит в OOP-контур legacy-выгрузку листа `Приходы_1С`,
        сохраняя тот же источник `supply_to_sellers_warehouse`, тот же фильтр
        `is_valid = TRUE` и тот же исторический порог по дате обновления.
        """

        return Database.read_sql_to_dataframe(query_supplies_1c).fillna("")

    def get_purchase_calculation_data(self) -> pd.DataFrame:
        """Читает базовый блок расчётного листа для merge по `wild`.

        Бизнес-сценарий:
        legacy-логика штрафов опирается только на первые 10 колонок листа
        `Расчет закупки`, поэтому метод сохраняет именно эту историческую
        выборку и не вмешивается в остальные пользовательские колонки.
        """

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
        """Читает и агрегирует виртуальные остатки UNIT по `wild`.

        Бизнес-сценарий:
        для колонки виртуального склада в закупочном листе нужны итоговые
        остатки `ФБС` по каждому `wild`, поэтому метод суммирует дубли и
        нормализует пропуски в ноль до группировки.
        """

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
        """Собирает итоговые колонки штрафов и виртуальных остатков.

        Бизнес-сценарий:
        метод переносит существующий расчётный блок legacy-сценария без
        изменения бизнес-логики: merge по `wild`, подстановка штрафов из БД и
        подстановка агрегированного остатка `ФБС` из UNIT.
        """

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

    def build_supplies_1c_sheet_data(self) -> pd.DataFrame:
        """Подготавливает DataFrame для листа `Приходы_1С`.

        Бизнес-сценарий:
        метод переносит legacy-правила листа `Приходы_1С`: форматирует даты в
        `DD.MM.YYYY`, сохраняет исторические русские заголовки колонок и не
        добавляет служебные поля вроде `updated_at`, которые изменили бы
        структуру пользовательской витрины. Порядок колонок фиксируется явно,
        чтобы новые поля можно было безопасно добавлять в конец без сдвига
        legacy-раскладки.
        """

        supplies_df = self.get_supplies_1c_data()
        supplies_df = self._format_date_columns_for_google(
            df=supplies_df,
            date_columns=[
                "document_created_at",
                "supply_date",
                "update_document_datetime",
            ],
        )
        supplies_df = supplies_df.rename(columns=supply_1c_columns_rename)
        return supplies_df[supply_1c_output_columns].copy()

    def update_penalties_and_virtual_stock(self) -> pd.DataFrame:
        """Точечно обновляет колонки штрафов и виртуального склада.

        Бизнес-сценарий:
        расчётный лист закупки содержит ручные и производные блоки, поэтому
        этот сценарий перезаписывает только две нужные колонки, начиная с
        найденного заголовка штрафов, и не трогает остальные части витрины.
        """

        update_df = self.build_penalties_and_virtual_stock_update()
        worksheet = self.google_connect_to_calculate_sheet.sheet_title
        start_update = worksheet.find(penalties_column_name)
        start_cell = rowcol_to_a1(start_update.row + 1, start_update.col)
        values = update_df.values.tolist()

        if not values:
            logger.info("Нет строк для обновления штрафов и виртуального склада.")
            return update_df

        worksheet.update(
            start_cell,
            values,
            value_input_option="USER_ENTERED",
        )
        logger.info(
            "Штрафы и виртуальный склад обновлены в Google Sheets, начиная с ячейки %s",
            start_cell,
        )
        return update_df

    def update_supplies_1c_sheet(self) -> pd.DataFrame:
        """Обновляет лист `Приходы_1С` в legacy-совместимом формате.

        Бизнес-сценарий:
        метод переносит в OOP-контур регламентную выгрузку приходов 1С. Он
        публикует заголовки и данные начиная с `A2`, а в `A1` ставит отметку
        последнего обновления, чтобы сохранить привычное поведение рабочей
        таблицы и не ломать сценарии ручной проверки.
        """

        supplies_sheet_df = self.build_supplies_1c_sheet_data()
        output_values = self._build_sheet_output(supplies_sheet_df)
        connector = self.google_connect_to_supplies_1c_sheet

        if output_values:
            self._update_sheet_range(
                connector=connector,
                range_name="A2",
                values=output_values,
            )
        else:
            logger.info(
                "Выгрузка листа Приходы_1С не записала строк данных, но сценарий продолжает обновление отметки времени."
            )

        self._update_sheet_range(
            connector=connector,
            range_name="A1",
            values=[[self._build_updated_label()]],
        )
        logger.info("Лист Приходы_1С успешно обновлён.")
        return supplies_sheet_df
