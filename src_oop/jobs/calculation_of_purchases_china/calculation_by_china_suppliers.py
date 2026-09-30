"""Перенос квартального плана в рабочую таблицу закупок Китая.

Сценарий публикует значения исходного плана в фиксированной структуре колонок.
Группировка по поставщикам и расчет сумм закупки здесь не выполняются.
"""

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
    """Готовит строки годового плана для листа «БД_Поквартально».

    Сохраняет строки источника и отбирает колонки ANNUAL_PLAN_COLUMNS.
    Отсутствующие поля добавляет пустыми, без расчета или переименования.
    """

    def __init__(self) -> None:
        """Задает источник плана и приемник из конфигураций двух модулей.

        Подключения создаются при первом чтении или записи, чтобы пустой
        исходный план не требовал открытия целевого листа.
        """
        self._source_table_name = annual_procurement_plan.get("title")
        self._source_sheet_name = annual_procurement_plan.get("quarter_sheet")
        self._target_table_name = delivery_calculation_china.get("title")
        self._target_sheet_name = delivery_calculation_china.get("db_sheet_quarterly")

        self._source_conn = None
        self._target_conn = None

    @property
    def source_connect(self) -> GoogleTabs:
        """Открывает и кеширует лист квартального плана для чтения по имени."""
        if self._source_conn is None:
            self._source_conn = GoogleTabs(
                self._source_table_name,
                self._source_sheet_name,
            )
        return self._source_conn

    @property
    def target_connect(self) -> GoogleTabs:
        """Открывает и кеширует целевой лист для публикации квартального плана."""
        if self._target_conn is None:
            self._target_conn = GoogleTabs(
                self._target_table_name,
                self._target_sheet_name,
            )
        return self._target_conn

    def get_quarterly_plan_data(self) -> pd.DataFrame:
        """Читает значения квартального плана и выбирает колонки для переноса.

        Заголовки ожидаются в строке 4, данные начинаются со строки 5.
        При отсутствии заголовков или строк возвращает пустой DataFrame:
        функция запуска использует это как условие пропуска записи.
        Строки не фильтруются, имена колонок сопоставляются точно.
        """
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
        """Сохраняет структуру выгрузки плана при пропусках колонок источника.

        Добавляет отсутствующие поля пустыми в переданный DataFrame и
        возвращает выборку ANNUAL_PLAN_COLUMNS в заданном порядке.
        Остальные колонки не переносятся; строки и суммы не пересчитываются.
        """
        for column in ANNUAL_PLAN_COLUMNS:
            if column not in df.columns:
                df[column] = ""

        return df.loc[:, ANNUAL_PLAN_COLUMNS]

    @staticmethod
    def set_data(connector: GoogleTabs, df: pd.DataFrame) -> None:
        """Публикует подготовленный план через общий клиент Google Sheets.

        Клиент добавляет updated_at и полностью заменяет значения рабочей
        области, включая очистку старых лишних строк и колонок. Проверка
        пустого результата выполняется функцией запуска до вызова метода.
        """
        connector.set_df_to_google(df)
