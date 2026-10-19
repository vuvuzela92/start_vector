"""Регламентные витрины закупочного контура России.

Бизнес-логика процесса:
1. Заказы поставщикам из 1С читаются из `ordered_goods_from_buyers` вместе с
   реквизитами чеков. В витрину попадают только корректные документы,
   обновлённые с мая 2025 года. Результат публикуется в лист
   `Заказы_поставщиков_1С` таблицы «Расчет закупки Россия».
2. Приходы 1С читаются из `supply_to_sellers_warehouse` по тем же правилам
   корректности и исторической глубины. Даты приводятся к формату
   `DD.MM.YYYY`, а технические имена полей заменяются пользовательскими
   заголовками. Один набор данных публикуется в лист `Приходы_1С` таблиц
   «Расчет закупки Россия» и «Расчет поставки Китай_по обороту».
3. Состояние поставок WB за последние два месяца собирается из
   `wb_supplies_goods`, `wb_supplies` и `article`. Для каждой пары поставка-
   артикул выбирается последнее обновление, а числовые статусы переводятся в
   понятные названия. Витрина `БД_поставки` очищает строки, не попавшие в
   новую выборку, чтобы закрытые поставки не отображались как актуальные.
4. Отдельные расчётные сценарии объединяют заказы, приходы и возвраты для
   листа `Заказы_и_поступления`, а также точечно обновляют штрафы и
   виртуальные остатки в листе `Расчет закупки`.

Для legacy-листов заголовки и данные записываются с `A2`, а в `A1` ставится
отметка времени обновления. Это сохраняет совместимость с существующими
формулами и ручной работой пользователей в Google Sheets.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime

import gspread
import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.calculation_of_purchases_russia.config import (
    google_table,
    orders_1c_columns_rename,
    penalties_column_name,
    query_orders_1c,
    query_orders_and_supply,
    query_penalties_and_virtual_stock,
    query_supplies_1c,
    query_wb_supplies,
    supply_1c_output_columns,
    supply_1c_columns_rename,
    supplies_1c_target_sheets,
    unit_table,
    virtual_stock_column_name,
    wb_supply_statuses,
)

logger = logging.getLogger(__name__)
# Ограничиваем размер payload и частоту запросов, чтобы не перегружать Sheets API.
SUPPLIES_1C_MAX_PAYLOAD_BYTES = 2_000_000
SUPPLIES_1C_WRITE_CHUNK_SIZE = 1_000
SUPPLIES_1C_WRITE_PAUSE_SECONDS = 1.0


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
        self._orders_1c_sheet = google_table["orders_1c_sheet"]
        self._calculate_sheet = google_table["calculate_sheet"]
        self._supplies_1c_sheet = google_table["supplies_1c_sheet"]
        self._wb_supplies_sheet = google_table["wb_supplies_sheet"]
        self._unit_table_title = unit_table["title"]
        self._unit_sheet_title = unit_table["sheet_unit"]

        self._conn_purchase_russia: GoogleTabs | None = None
        self._conn_orders_1c_sheet: GoogleTabs | None = None
        self._conn_calculate_sheet: GoogleTabs | None = None
        self._conn_unit_table: GoogleTabs | None = None
        self._conn_supplies_1c_sheet: GoogleTabs | None = None
        self._conn_wb_supplies_sheet: GoogleTabs | None = None

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

    @property
    def google_connect_to_orders_1c_sheet(self) -> GoogleTabs:
        """Возвращает подключение к листу `Заказы_поставщиков_1С`.

        Бизнес-сценарий:
        закупочная команда сверяет заказы 1С в отдельной витрине, поэтому
        подключение создаётся лениво и не влияет на остальные job-сценарии
        этого модуля.
        """

        if self._conn_orders_1c_sheet is None:
            self._conn_orders_1c_sheet = GoogleTabs(
                self._purchase_russia_table,
                self._orders_1c_sheet,
            )
        return self._conn_orders_1c_sheet

    @property
    def google_connect_to_wb_supplies_sheet(self) -> GoogleTabs:
        """Возвращает подключение к листу `БД_поставки`.

        Бизнес-сценарий:
        лист показывает актуальный состав поставок WB за последние два месяца,
        поэтому его подключение изолировано от витрин заказов и приходов 1С.
        """

        if self._conn_wb_supplies_sheet is None:
            self._conn_wb_supplies_sheet = GoogleTabs(
                self._purchase_russia_table,
                self._wb_supplies_sheet,
            )
        return self._conn_wb_supplies_sheet

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

    def _update_values_in_chunks(
        self,
        values: list[list[object]],
        update_range: Callable[[str, list[list[object]]], None],
        operation_name: str,
    ) -> None:
        """Записывает большую выгрузку Google Sheets пакетами.

        Бизнес-сценарий:
        выгрузка `Приходы_1С` может содержать тысячи строк, и один большой
        HTTP-запрос иногда превышает тайм-аут Google Sheets. Разбиение на
        пакеты снижает размер каждого запроса; заголовок записывается только
        в первом пакете, а вызывающий сценарий обновляет `A1` после завершения
        всех пакетов.
        """

        if not values:
            return

        total_rows = len(values)
        total_columns = len(values[0])
        payload_size = len(
            json.dumps(values, ensure_ascii=False, default=str).encode("utf-8")
        )
        chunk_size = (
            total_rows
            if payload_size <= SUPPLIES_1C_MAX_PAYLOAD_BYTES
            else SUPPLIES_1C_WRITE_CHUNK_SIZE
        )
        total_chunks = (total_rows + chunk_size - 1) // chunk_size

        for chunk_number, start_index in enumerate(
            range(0, total_rows, chunk_size),
            start=1,
        ):
            end_index = min(
                start_index + chunk_size,
                total_rows,
            )
            if chunk_number > 1:
                time.sleep(SUPPLIES_1C_WRITE_PAUSE_SECONDS)
            start_row = start_index + 2
            end_row = end_index + 1
            range_name = f"A{start_row}:{rowcol_to_a1(end_row, total_columns)}"
            update_range(range_name, values[start_index:end_index])
            logger.info(
                "Пакет выгрузки записан в Google Sheets | operation=%s | range=%s | packet=%s/%s | rows=%s/%s | payload_bytes=%s",
                operation_name,
                range_name,
                chunk_number,
                total_chunks,
                end_index,
                total_rows,
                payload_size,
            )

    def _replace_sheet_data_below_status(
        self,
        connector: GoogleTabs,
        output_values: list[list[object]],
    ) -> None:
        """Перезаписывает данные с `A2` и очищает только устаревший хвост листа.

        Бизнес-сценарий:
        витрина `БД_поставки` содержит меняющееся число строк. После сокращения
        выборки старые позиции не должны оставаться в листе и ошибочно
        восприниматься закупочной командой как актуальные, при этом служебная
        отметка в `A1` сохраняется до отдельного обновления.
        """

        worksheet = connector.sheet_title
        old_values = connector.get_all_values_with_retry()
        old_data_rows = max(len(old_values) - 1, 0)
        old_columns = max((len(row) for row in old_values), default=0)
        new_columns = max((len(row) for row in output_values), default=0)
        target_rows = max(old_data_rows, len(output_values))
        target_columns = max(old_columns, new_columns)

        if target_rows == 0 or target_columns == 0:
            return

        values = [
            ["" for _ in range(target_columns)]
            for _ in range(target_rows)
        ]
        for row_index, row in enumerate(output_values):
            values[row_index][:len(row)] = row

        target_range = f"A2:{rowcol_to_a1(target_rows + 1, target_columns)}"
        connector._execute_google_write_with_retry(
            operation_name=f"replace_range {worksheet.title} {target_range}",
            func=worksheet.update,
            range_name=target_range,
            values=values,
            value_input_option="USER_ENTERED",
        )

    def _open_worksheet_by_spreadsheet_id(
        self,
        spreadsheet_id: str,
        sheet_title: str,
    ) -> gspread.Worksheet:
        """Открывает лист Google Sheets по `spreadsheet_id`.

        Бизнес-сценарий:
        часть витрин проекта нужно писать строго в конкретный документ, даже
        если в рабочем пространстве встречаются таблицы с похожими названиями.
        Для таких случаев безопаснее открывать таблицу по `spreadsheet_id`,
        а не по заголовку.
        """

        client = gspread.service_account(filename=str(self.google_connect.creds_file))
        spreadsheet = client.open_by_key(spreadsheet_id)
        return spreadsheet.worksheet(sheet_title)

    def _update_worksheet_range_with_retry(
        self,
        worksheet: gspread.Worksheet,
        range_name: str,
        values: list[list[object]],
    ) -> None:
        """Записывает диапазон в произвольный лист Google Sheets с retry.

        Бизнес-сценарий:
        сценарий выгрузки `Приходы_1С` теперь обновляет несколько таблиц, и для
        дополнительных документов по `spreadsheet_id` нужен тот же retry на
        временные ошибки Google Sheets, что и для базового клиента проекта.
        """

        self.google_connect._execute_google_write_with_retry(
            operation_name=f"update_range {worksheet.title} {range_name}",
            func=worksheet.update,
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

    def get_orders_1c_data(self) -> pd.DataFrame:
        """Читает валидные заказы 1С вместе с данными чеков.

        Бизнес-сценарий:
        лист `Заказы_поставщиков_1С` объединяет заказ и сведения о его чеке,
        чтобы закупочная команда контролировала расчёты с поставщиками в одной
        витрине. Отбираются только корректные документы, обновлённые с мая
        2025 года, как в legacy-сценарии.
        """

        return Database.read_sql_to_dataframe(query_orders_1c).fillna("")

    def get_wb_supplies_data(self) -> pd.DataFrame:
        """Читает актуальные товары поставок WB за последние два месяца.

        Бизнес-сценарий:
        витрина `БД_поставки` должна содержать только последнее состояние
        каждой пары поставка-артикул, поэтому SQL фиксирует запись через
        `DISTINCT ON` и сортировку по времени обновления.
        """

        return Database.read_sql_to_dataframe(query_wb_supplies)

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

    def build_orders_1c_sheet_data(self) -> pd.DataFrame:
        """Подготавливает данные для legacy-листа `Заказы_поставщиков_1С`.

        Бизнес-сценарий:
        метод сохраняет русские заголовки, исторический формат дат
        `DD.MM.YYYY` и набор полей заказов 1С, включая реквизиты чеков,
        чтобы существующие формулы и ручная работа в Google Sheets не
        изменились после переноса в OOP-модуль.
        """

        orders_df = self.get_orders_1c_data()
        orders_df = self._format_date_columns_for_google(
            df=orders_df,
            date_columns=[
                "document_created_at",
                "supply_date",
                "update_document_datetime",
                "created_at",
                "expected_receipt_date",
                "shipment_date",
                "receipt_date",
            ],
        )
        return orders_df.rename(columns=orders_1c_columns_rename)

    def build_wb_supplies_sheet_data(self) -> pd.DataFrame:
        """Подготавливает витрину WB-поставок для листа `БД_поставки`.

        Бизнес-сценарий:
        числовые статусы WB переводятся в понятные закупочной команде названия,
        а плановая и фактическая даты приводятся к единому формату. Неизвестный
        статус сохраняется исходным значением, чтобы не скрывать новые статусы
        API до их явного добавления в справочник.
        """

        wb_supplies_df = self.get_wb_supplies_data()
        if "Статус" in wb_supplies_df.columns:
            wb_supplies_df["Статус"] = wb_supplies_df["Статус"].replace(
                wb_supply_statuses,
            )
        wb_supplies_df = self._add_in_acceptance_quantity(wb_supplies_df)
        wb_supplies_df = self._format_date_columns_for_google(
            df=wb_supplies_df,
            date_columns=["Плановая дата поставки", "Фактическая дата поставки"],
        )
        return wb_supplies_df.replace([float("inf"), float("-inf")], "").fillna("")

    @staticmethod
    def _add_in_acceptance_quantity(df: pd.DataFrame) -> pd.DataFrame:
        """Рассчитывает количество товара, которое ещё находится в приёмке.

        Бизнес-правило листа `БД_поставки` полностью повторяет формулу Google
        Sheets: для статусов «Отгружено на воротах» и «Идёт приёмка» берётся
        разница между добавленным количеством и поступившим в продажу; для
        статуса «Принято» — разница между принятым и поступившим в продажу.
        Для остальных статусов значение равно нулю.
        """
        required_columns = (
            "Статус",
            "Добавлено в поставку",
            "Принято, шт",
            "Поступило в продажу",
        )
        if any(column not in df.columns for column in required_columns):
            logger.warning(
                "Количество приёмки не рассчитано: в витрине отсутствует обязательная колонка | missing=%s",
                [column for column in required_columns if column not in df.columns],
            )
            df["Идет приемка"] = 0
            return df

        added_quantity = pd.to_numeric(
            df["Добавлено в поставку"],
            errors="coerce",
        ).fillna(0)
        accepted_quantity = pd.to_numeric(
            df["Принято, шт"],
            errors="coerce",
        ).fillna(0)
        ready_for_sale_quantity = pd.to_numeric(
            df["Поступило в продажу"],
            errors="coerce",
        ).fillna(0)

        in_acceptance = pd.Series(0, index=df.index, dtype="int64")
        active_statuses = df["Статус"].isin(
            ["Отгружено на воротах", "Идёт приёмка"],
        )
        accepted_status = df["Статус"].eq("Принято")
        in_acceptance = in_acceptance.where(
            ~active_statuses,
            added_quantity - ready_for_sale_quantity,
        )
        in_acceptance = in_acceptance.where(
            ~accepted_status,
            accepted_quantity - ready_for_sale_quantity,
        )

        if "Идет приемка" in df.columns:
            df = df.drop(columns=["Идет приемка"])
        df["Идет приемка"] = in_acceptance
        return df

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
        последнего обновления. Один и тот же набор данных выгружается сразу в
        несколько таблиц, чтобы российский и китайский закупочные контуры
        видели одинаковую витрину `Приходы_1С`.
        """

        supplies_sheet_df = self.build_supplies_1c_sheet_data()
        output_values = self._build_sheet_output(supplies_sheet_df)
        updated_label = self._build_updated_label()

        for target_sheet in supplies_1c_target_sheets:
            if target_sheet.spreadsheet_id:
                worksheet = self._open_worksheet_by_spreadsheet_id(
                    spreadsheet_id=target_sheet.spreadsheet_id,
                    sheet_title=target_sheet.sheet_title,
                )
                if output_values:
                    self._update_values_in_chunks(
                        values=output_values,
                        update_range=lambda range_name, values: self._update_worksheet_range_with_retry(
                            worksheet=worksheet,
                            range_name=range_name,
                            values=values,
                        ),
                        operation_name=target_sheet.table_title,
                    )
                else:
                    logger.info(
                        "Выгрузка листа Приходы_1С не записала строк данных, но сценарий продолжает обновление отметки времени | table=%s",
                        target_sheet.table_title,
                    )

                self._update_worksheet_range_with_retry(
                    worksheet=worksheet,
                    range_name="A1",
                    values=[[updated_label]],
                )
            else:
                connector = self.google_connect_to_supplies_1c_sheet
                if output_values:
                    self._update_values_in_chunks(
                        values=output_values,
                        update_range=lambda range_name, values: self._update_sheet_range(
                            connector=connector,
                            range_name=range_name,
                            values=values,
                        ),
                        operation_name=target_sheet.table_title,
                    )
                else:
                    logger.info(
                        "Выгрузка листа Приходы_1С не записала строк данных, но сценарий продолжает обновление отметки времени | table=%s",
                        target_sheet.table_title,
                    )

                self._update_sheet_range(
                    connector=connector,
                    range_name="A1",
                    values=[[updated_label]],
                )

            logger.info(
                "Лист Приходы_1С успешно обновлён | table=%s | sheet=%s",
                target_sheet.table_title,
                target_sheet.sheet_title,
            )

        return supplies_sheet_df

    def update_orders_1c_sheet(self) -> pd.DataFrame:
        """Обновляет лист `Заказы_поставщиков_1С` в legacy-совместимом виде.

        Бизнес-сценарий:
        job публикует заказы поставщикам и реквизиты чеков с заголовками в
        строке `A2`, а в `A1` ставит время обновления. Такой формат сохраняет
        совместимость с действующей структурой закупочной таблицы.
        """

        orders_sheet_df = self.build_orders_1c_sheet_data()
        output_values = self._build_sheet_output(orders_sheet_df)
        connector = self.google_connect_to_orders_1c_sheet

        self._update_sheet_range(
            connector=connector,
            range_name="A2",
            values=output_values,
        )
        self._update_sheet_range(
            connector=connector,
            range_name="A1",
            values=[[self._build_updated_label()]],
        )
        logger.info("Лист Заказы_поставщиков_1С успешно обновлён.")
        return orders_sheet_df

    def update_wb_supplies_sheet(self) -> pd.DataFrame:
        """Обновляет лист `БД_поставки` актуальным состоянием поставок WB.

        Бизнес-сценарий:
        job заменяет данные под служебной строкой статуса и очищает устаревшие
        строки, если поставок стало меньше. Это предотвращает отображение
        закрытых или больше неактуальных поставок в закупочной витрине.
        """

        wb_supplies_sheet_df = self.build_wb_supplies_sheet_data()
        output_values = self._build_sheet_output(wb_supplies_sheet_df)
        connector = self.google_connect_to_wb_supplies_sheet

        self._replace_sheet_data_below_status(
            connector=connector,
            output_values=output_values,
        )
        self._update_sheet_range(
            connector=connector,
            range_name="A1",
            values=[[self._build_updated_label()]],
        )
        logger.info("Лист БД_поставки успешно обновлён.")
        return wb_supplies_sheet_df
