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
    """
    Нормализует значения колонки перед сопоставлением по wild.

    Бизнес-логика:
    убирает `NaN` и лишние пробелы, чтобы одинаковые wild и статусы не расходились из-за формата
    ячеек Google Sheets.
    """
    return series.fillna("").astype(str).str.strip()


def _prepare_statuses_lookup(df_statuses: pd.DataFrame) -> tuple[pd.DataFrame, int, list[str]]:
    """
    Готовит справочник статусов wild для безопасного обновления UNIT.

    Бизнес-логика:
    убирает пустые wild, фиксирует конфликтующие статусы и оставляет один последний непустой статус,
    чтобы при merge не размножить строки основного листа UNIT.
    """
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

    # Оставляем один последний непустой статус на wild, чтобы merge не размножал строки.
    prepared_statuses = prepared_statuses[prepared_statuses[STATUS_SOURCE_COLUMN] != ""].copy()
    prepared_statuses = prepared_statuses.drop_duplicates(subset=[WILD_COLUMN], keep="last")

    return prepared_statuses, len(conflicting_wilds), conflicting_wilds[:5]


def update_wild_statuses() -> None:
    """
    Обновляет статус товара в UNIT по справочнику wild-статусов.

    Бизнес-логика:
    читает источник статусов и основной лист UNIT, сопоставляет строки по `wild`, проверяет, что число
    строк не изменилось после merge, и затем записывает подготовленные статусы в целевую колонку.
    """
    calc = Calculation_of_purchases_russia()
    statuses_table = calc.google_connect_statuses.sheet_title.get_all_values()

    status_headers = statuses_table[0]
    status_rows = statuses_table[1:]
    df_statuses = pd.DataFrame(status_rows, columns=status_headers)
    logger.info(
        "Источник статусов wild загружен: rows=%s.",
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
        "Основной лист UNIT загружен для обновления статусов: rows=%s.",
        len(df_unit_short),
    )

    wild_with_statuses, conflicting_wild_count, conflicting_wild_examples = (
        _prepare_statuses_lookup(df_statuses)
    )
    logger.info(
        "Справочник статусов подготовлен: unique_wild=%s conflicting_wild=%s examples=%s",
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
            "Проверка merge не пройдена: "
            f"source_rows={source_row_count}, merged_rows={result_row_count}, "
            f"conflicting_wild={conflicting_wild_count}, "
            f"examples={conflicting_wild_examples}"
        )

    result_df[STATUS_TARGET_COLUMN] = _normalize_series(result_df[STATUS_SOURCE_COLUMN])
    result_df = result_df.drop(columns=[STATUS_SOURCE_COLUMN]).fillna("")

    if STATUS_TARGET_COLUMN not in result_df.columns:
        raise ValueError(
            f"Целевая колонка отсутствует после подготовки данных: '{STATUS_TARGET_COLUMN}'."
        )

    results_list = result_df[STATUS_TARGET_COLUMN].astype(str).tolist()
    logger.info(
        "Подготовлены значения для обновления колонки UNIT: values=%s column='%s'.",
        len(results_list),
        STATUS_TARGET_COLUMN,
    )

    if len(results_list) != source_row_count:
        raise ValueError(
            "Проверка длины перед записью не пройдена: "
            f"unit_rows={source_row_count}, values_to_write={len(results_list)}, "
            f"conflicting_wild={conflicting_wild_count}, "
            f"examples={conflicting_wild_examples}"
        )

    logger.info(
        "Проверка длины перед записью пройдена: values=%s unit_rows=%s.",
        len(results_list),
        source_row_count,
    )
    unit_economics.google_connect.update_column_by_name(STATUS_TARGET_COLUMN, results_list)
    logger.info("Колонка UNIT обновлена в Google Sheets: column='%s'.", STATUS_TARGET_COLUMN)
