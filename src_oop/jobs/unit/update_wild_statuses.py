import logging

import pandas as pd

from src_oop.jobs.calculation_of_purchases_russia.calculation_of_purchases_russia import (
    Calculation_of_purchases_russia,
)
from src_oop.jobs.unit.unit import UnitEconomics

logger = logging.getLogger(__name__)

WILD_COLUMN = "wild"
STATUS_SOURCE_COLUMN = "статус вилд"
STATUS_TARGET_COLUMN = "Статус товара"


def _normalize_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _prepare_statuses_lookup(df_statuses: pd.DataFrame) -> tuple[pd.DataFrame, int, list[str]]:
    prepared_statuses = df_statuses[[WILD_COLUMN, STATUS_SOURCE_COLUMN]].copy()
    prepared_statuses[WILD_COLUMN] = _normalize_series(prepared_statuses[WILD_COLUMN])
    prepared_statuses[STATUS_SOURCE_COLUMN] = _normalize_series(
        prepared_statuses[STATUS_SOURCE_COLUMN]
    )

    prepared_statuses = prepared_statuses[prepared_statuses[WILD_COLUMN] != ""].copy()

    status_variants = (
        prepared_statuses.groupby(WILD_COLUMN)[STATUS_SOURCE_COLUMN]
        .nunique()
        .reset_index(name="status_count")
    )
    conflicting_wilds = status_variants[status_variants["status_count"] > 1][WILD_COLUMN].tolist()

    # Для каждого wild берём последнее непустое значение статуса и не допускаем
    # размножение строк при merge из-за нескольких вариантов статуса в источнике.
    prepared_statuses = prepared_statuses[prepared_statuses[STATUS_SOURCE_COLUMN] != ""].copy()
    prepared_statuses = prepared_statuses.drop_duplicates(subset=[WILD_COLUMN], keep="last")

    return prepared_statuses, len(conflicting_wilds), conflicting_wilds[:5]


def update_wild_statuses() -> None:
    calc = Calculation_of_purchases_russia()
    statuses_table = calc.google_connect_statuses.sheet_title.get_all_values()

    status_headers = statuses_table[0]
    status_rows = statuses_table[1:]
    df_statuses = pd.DataFrame(status_rows, columns=status_headers)
    logger.info(
        "Источник статусов загружен: %s строк данных.",
        len(df_statuses),
    )

    unit_economics = UnitEconomics()
    unit_table = unit_economics.google_connect.sheet_title.get_all_values()
    unit_headers = unit_table[0]
    unit_rows = unit_table[1:]
    df_unit = pd.DataFrame(unit_rows, columns=unit_headers)
    df_unit_short = df_unit[[WILD_COLUMN, STATUS_TARGET_COLUMN]].copy()
    df_unit_short[WILD_COLUMN] = _normalize_series(df_unit_short[WILD_COLUMN])
    logger.info(
        "Основной лист UNIT загружен: %s строк данных.",
        len(df_unit_short),
    )

    wild_with_statuses, conflicting_wild_count, conflicting_wild_examples = (
        _prepare_statuses_lookup(df_statuses)
    )
    logger.info(
        "Подготовлен справочник статусов: %s уникальных wild, %s wild с несколькими статусами. "
        "Примеры конфликтных wild: %s",
        len(wild_with_statuses),
        conflicting_wild_count,
        conflicting_wild_examples,
    )

    source_row_count = len(df_unit_short)
    result_df = df_unit_short.merge(
        wild_with_statuses,
        on=WILD_COLUMN,
        how="left",
    )
    result_row_count = len(result_df)

    if result_row_count != source_row_count:
        raise ValueError(
            "Нарушен инвариант при объединении статусов с UNIT: "
            f"исходных строк={source_row_count}, после merge={result_row_count}, "
            f"wild с несколькими статусами={conflicting_wild_count}, "
            f"примеры проблемных wild={conflicting_wild_examples}"
        )

    result_df[STATUS_TARGET_COLUMN] = _normalize_series(result_df[STATUS_SOURCE_COLUMN])
    result_df = result_df.drop(columns=[STATUS_SOURCE_COLUMN]).fillna("")

    if STATUS_TARGET_COLUMN not in result_df.columns:
        raise ValueError(
            f"В обработанных данных отсутствует колонка '{STATUS_TARGET_COLUMN}'."
        )

    results_list = result_df[STATUS_TARGET_COLUMN].astype(str).tolist()
    logger.info("Подготовлено %s значений для записи в колонку '%s'.", len(results_list), STATUS_TARGET_COLUMN)

    if len(results_list) != source_row_count:
        raise ValueError(
            "Длина данных перед записью не совпадает с длиной основного листа: "
            f"строк в UNIT={source_row_count}, значений к записи={len(results_list)}, "
            f"wild с несколькими статусами={conflicting_wild_count}, "
            f"примеры проблемных wild={conflicting_wild_examples}"
        )

    logger.info(
        "Проверка длины перед записью пройдена: %s значений на %s строк основного листа.",
        len(results_list),
        source_row_count,
    )
    unit_economics.google_connect.update_column_by_name(STATUS_TARGET_COLUMN, results_list)
    logger.info("Колонка '%s' успешно обновлена в Google Sheets.", STATUS_TARGET_COLUMN)
