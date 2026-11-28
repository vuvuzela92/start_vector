import logging

import pandas as pd
from gspread.utils import ValueRenderOption

from src_oop.jobs.calculation_of_purchases_russia.calculation_of_purchases_russia import (
    Calculation_of_purchases_russia,
)
from src_oop.jobs.unit.unit import UnitEconomics

logger = logging.getLogger(__name__)

WILD_COLUMN = "wild"
STATUS_SOURCE_COLUMN = "статус вилд"
STATUS_TARGET_COLUMN = "Статус товара"


def _read_columns_with_formula_support(
    connector,
    *,
    key_column: str,
    value_column: str,
    operation_name: str,
) -> tuple[list[str], list[list[object]], list[list[object]]]:
    """Читает заголовки и две бизнес-колонки листа с повторами временных сбоев.

    Бизнес-правило: синхронизация статусов должна сохранять соответствие строк
    по wild; формульные ключи и статусы читаются как вычисленные значения,
    а обычные значения получают облегчённым запросом FORMULA.
    """
    worksheet = connector.sheet_title
    headers = connector.execute_with_retry(
        f"Чтение заголовков для {operation_name}",
        worksheet.row_values,
        1,
        value_render_option=ValueRenderOption.formula,
    )
    missing = [column for column in (key_column, value_column) if column not in headers]
    if missing and any(isinstance(value, str) and value.startswith("=") for value in headers):
        headers = connector.execute_with_retry(
            f"Чтение вычисляемых заголовков для {operation_name}",
            worksheet.row_values,
            1,
            value_render_option=ValueRenderOption.formatted,
        )
    indexes = []
    for column in (key_column, value_column):
        if column not in headers:
            raise ValueError(
                f"В листе '{worksheet.title}' отсутствует обязательная колонка '{column}'."
            )
        indexes.append(headers.index(column))

    ranges = [f"{_column_letter(index + 1)}2:{_column_letter(index + 1)}" for index in indexes]
    values = connector.execute_with_retry(
        f"Чтение колонок {key_column} и {value_column} для {operation_name}",
        worksheet.batch_get,
        ranges,
        value_render_option=ValueRenderOption.formula,
    )
    result = []
    for index, column_values in zip(indexes, values, strict=True):
        if any(
            row and isinstance(row[0], str) and row[0].startswith("=")
            for row in column_values
        ):
            column_values = connector.execute_with_retry(
                f"Чтение вычисляемой колонки {key_column if not result else value_column} "
                f"для {operation_name}",
                worksheet.batch_get,
                [ranges[len(result)]],
                value_render_option=ValueRenderOption.unformatted,
            )[0]
        result.append(column_values)
    return headers, result[0], result[1]


def _column_letter(column_number: int) -> str:
    """Преобразует индекс колонки в A1-обозначение для узкого чтения листа."""
    letters = ""
    while column_number:
        column_number, remainder = divmod(column_number - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


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
    читает только ключевые колонки источника статусов и UNIT, сопоставляет
    строки по `wild`, проверяет сохранение числа строк и записывает статусы
    только после успешных проверок. Временные ошибки Google Sheets повторяются.
    """
    calc = Calculation_of_purchases_russia()
    _, status_keys, status_values = _read_columns_with_formula_support(
        calc.google_connect_statuses,
        key_column=WILD_COLUMN,
        value_column=STATUS_SOURCE_COLUMN,
        operation_name="чтение справочника статусов",
    )
    status_row_count = max(len(status_keys), len(status_values))
    df_statuses = pd.DataFrame(
        {
            WILD_COLUMN: [row[0] if row else "" for row in status_keys]
            + [""] * (status_row_count - len(status_keys)),
            STATUS_SOURCE_COLUMN: [row[0] if row else "" for row in status_values]
            + [""] * (status_row_count - len(status_values)),
        }
    )
    logger.info(
        "Источник статусов wild загружен: rows=%s.",
        len(df_statuses),
    )

    unit_economics = UnitEconomics()
    unit_headers, unit_keys, unit_statuses = _read_columns_with_formula_support(
        unit_economics.google_connect,
        key_column=WILD_COLUMN,
        value_column=STATUS_TARGET_COLUMN,
        operation_name="чтение основного листа UNIT",
    )
    unit_row_count = max(len(unit_keys), len(unit_statuses))
    df_unit_short = pd.DataFrame(
        {
            WILD_COLUMN: [row[0] if row else "" for row in unit_keys]
            + [""] * (unit_row_count - len(unit_keys)),
            STATUS_TARGET_COLUMN: [row[0] if row else "" for row in unit_statuses]
            + [""] * (unit_row_count - len(unit_statuses)),
        }
    )
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
    status_column = _column_letter(unit_headers.index(STATUS_TARGET_COLUMN) + 1)
    end_cell = f"{status_column}{source_row_count + 1}"
    unit_economics.google_connect.execute_with_retry(
        "Запись статусов товаров UNIT",
        unit_economics.google_connect.sheet_title.update,
        range_name=f"{status_column}2:{end_cell}",
        values=[[value] for value in results_list],
        value_input_option="RAW",
    )
    logger.info("Колонка UNIT обновлена в Google Sheets: column='%s'.", STATUS_TARGET_COLUMN)
