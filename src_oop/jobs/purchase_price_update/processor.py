from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd

from src_oop.jobs.purchase_price_update.config import (
    ARTICLE_COLUMN,
    NEVER_CHANGE_PRICE_COLUMN,
    PURCHASE_PRICE_COLUMN,
    REPORT_COLUMNS,
    REQUIRED_DB_COLUMNS,
    REQUIRED_UNIT_COLUMNS,
    SUSPICIOUS_DIFF_THRESHOLD,
)

logger = logging.getLogger(__name__)


class PurchasePriceUpdateError(Exception):
    """Базовая ошибка задачи обновления закупочных цен."""


class MissingRequiredColumnsError(PurchasePriceUpdateError):
    """Во входных данных отсутствуют обязательные колонки."""


class DuplicateBusinessKeyError(PurchasePriceUpdateError):
    """Бизнес-ключ не уникален, поэтому продолжать задачу небезопасно."""


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    """
    Результат подготовки данных к обновлению.

    Назначение:
    собирает в одном объекте как итоговые строки на обновление, так и
    служебную диагностику, которая важна для логов и расследования инцидентов.

    Поля:
    `prepared_rows` — все строки после нормализации и сопоставления с UNIT.
    `changed_rows` — только строки, где новая цена отличается от текущей.
    `suspicious_rows` — строки с подозрительно большим изменением цены.
    `excluded_locked_codes` — SKU, исключенные из-за флага неизменяемой цены.
    `absent_in_unit_codes` — SKU, найденные в БД, но отсутствующие в UNIT.
    `missing_price_codes` — SKU, для которых не удалось получить корректную цену.
    """

    prepared_rows: pd.DataFrame
    changed_rows: pd.DataFrame
    suspicious_rows: pd.DataFrame
    excluded_locked_codes: tuple[str, ...]
    absent_in_unit_codes: tuple[str, ...]
    missing_price_codes: tuple[str, ...]


def build_unit_sheet_dataframe(
    values: list[list[str]],
    header_row_index: int,
    data_row_index: int,
) -> pd.DataFrame:
    """
    Преобразует сырые значения листа Google Sheets в DataFrame.

    Назначение:
    превращает список списков, полученный из Google Sheets API, в структуру,
    с которой удобно дальше работать через pandas.

    Параметры:
    `values` — все значения листа в формате Google Sheets API.
    `header_row_index` — индекс строки, где лежат заголовки.
    `data_row_index` — индекс первой строки с данными.

    Возвращаемый результат:
    `pandas.DataFrame` со строками листа и дополнительной колонкой `sheet_row_number`.

    Возможные исключения:
    `MissingRequiredColumnsError`, если лист пустой или не содержит заголовков.
    `DuplicateBusinessKeyError`, если заголовки продублированы.

    Особенности поведения:
    колонка `sheet_row_number` сохраняет исходный номер строки в листе, чтобы
    запись обратно в Google Sheets была привязана к фактической строке.

    Дополнительно добавляет служебную колонку `sheet_row_number`.
    Она нужна, чтобы потом обновлять ровно те строки, которые были прочитаны,
    а не рассчитывать позицию товара заново уже после преобразований.
    """

    if len(values) <= header_row_index:
        raise MissingRequiredColumnsError(
            "Лист Сопост пуст или не содержит строку заголовков."
        )

    headers = values[header_row_index]
    if not headers:
        raise MissingRequiredColumnsError(
            "В листе Сопост не удалось прочитать заголовки."
        )

    duplicate_headers = pd.Index(headers)[pd.Index(headers).duplicated()].unique().tolist()
    if duplicate_headers:
        raise DuplicateBusinessKeyError(
            "В листе Сопост продублированы заголовки: "
            f"{', '.join(map(str, duplicate_headers))}"
        )

    rows = values[data_row_index:] if len(values) > data_row_index else []
    dataframe = pd.DataFrame(rows, columns=headers)
    # Сохраняем фактический номер строки Google Sheets в отдельной колонке.
    # Это позволяет обновлять цену адресно и не зависеть от того, как строки
    # могли быть переставлены или отфильтрованы на промежуточных шагах обработки.
    dataframe["sheet_row_number"] = list(
        range(data_row_index + 1, data_row_index + 1 + len(rows))
    )
    return dataframe


def prepare_unit_state(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Подготавливает состояние листа `Сопост` для сопоставления с БД.

    Назначение:
    превращает сырые данные из листа `Сопост` в компактное состояние,
    пригодное для надежного merge по business key `wild`.

    Параметры:
    `dataframe` — исходный DataFrame, собранный из листа `Сопост`.

    Возвращаемый результат:
    `pandas.DataFrame` только с нужными для сопоставления колонками:
    номер строки, артикул, текущая цена и флаг запрета на изменение цены.

    Возможные исключения:
    `MissingRequiredColumnsError`, если отсутствуют обязательные колонки.
    `DuplicateBusinessKeyError`, если `wild` не уникален.

    Особенности поведения:
    пустые `wild` отбрасываются, а цена из UNIT приводится к числу и округляется
    до целого так же, как это делал старый сценарий сопоставления.

    На этом шаге:
    1. проверяем наличие обязательных колонок;
    2. очищаем `wild` от пустых значений;
    3. приводим цену из UNIT к числу;
    4. вычисляем флаг "цену менять нельзя";
    5. проверяем уникальность business key `wild`.
    """

    _ensure_required_columns(dataframe, REQUIRED_UNIT_COLUMNS, source_name="лист Сопост")

    prepared = dataframe.copy()
    prepared[ARTICLE_COLUMN] = prepared[ARTICLE_COLUMN].map(_normalize_text)
    prepared = prepared[prepared[ARTICLE_COLUMN] != ""].copy()
    prepared[PURCHASE_PRICE_COLUMN] = _normalize_numeric_series(
        prepared[PURCHASE_PRICE_COLUMN]
    ).round(0)
    prepared["is_locked_price"] = prepared[NEVER_CHANGE_PRICE_COLUMN].map(
        _is_locked_price_flag
    )

    duplicates = prepared[
        prepared[ARTICLE_COLUMN].duplicated(keep=False)
    ][ARTICLE_COLUMN].unique().tolist()
    if duplicates:
        raise DuplicateBusinessKeyError(
            "В листе Сопост найдены дубли по business key 'wild': "
            f"{', '.join(duplicates)}"
        )

    return prepared[
        [
            "sheet_row_number",
            ARTICLE_COLUMN,
            PURCHASE_PRICE_COLUMN,
            "is_locked_price",
        ]
    ].rename(columns={PURCHASE_PRICE_COLUMN: "unit_price"})


def prepare_purchase_price_updates(
    db_dataframe: pd.DataFrame,
    unit_state: pd.DataFrame,
    round_price: bool,
) -> ProcessingResult:
    """
    Готовит итоговый набор строк для обновления закупочных цен.

    Логика:
    1. нормализуем ключи и цену из БД;
    2. при необходимости применяем округление;
    3. исключаем SKU с флагом "неизменяемая цена";
    4. оставляем только SKU, присутствующие в UNIT;
    5. считаем разницу между текущей ценой в UNIT и новой ценой из БД;
    6. выделяем только реально изменившиеся строки.

    Параметры:
    `db_dataframe` — сырые данные из PostgreSQL.
    `unit_state` — подготовленное состояние листа `Сопост`.
    `round_price` — нужно ли применять округление новой цены.

    Возвращаемый результат:
    `ProcessingResult` с итоговыми строками на обновление и диагностикой.

    Возможные исключения:
    `MissingRequiredColumnsError`, если во входных данных нет обязательных колонок.
    `DuplicateBusinessKeyError`, если бизнес-ключ не уникален в БД или UNIT.

    Особенности поведения:
    функция не пишет данные наружу и не зависит от Google API, поэтому она
    является основной точкой для unit-тестов бизнес-логики.
    """

    _ensure_required_columns(db_dataframe, REQUIRED_DB_COLUMNS, source_name="данные БД")

    prepared_db = db_dataframe.copy()
    prepared_db["local_vendor_code"] = prepared_db["local_vendor_code"].map(_normalize_text)
    prepared_db = prepared_db[prepared_db["local_vendor_code"] != ""].copy()
    prepared_db["price_per_item"] = _normalize_numeric_series(prepared_db["price_per_item"])

    if round_price:
        # Округление оставлено совместимым со старым скриптом,
        # чтобы не поменять поведение задачи на боевом запуске.
        prepared_db["price_per_item"] = prepared_db["price_per_item"].map(
            _round_price_like_legacy
        )

    duplicate_db_keys = (
        prepared_db[prepared_db["local_vendor_code"].duplicated(keep=False)][
            "local_vendor_code"
        ]
        .unique()
        .tolist()
    )
    if duplicate_db_keys:
        raise DuplicateBusinessKeyError(
            "В данных БД найдены дубли по business key 'local_vendor_code': "
            f"{', '.join(duplicate_db_keys)}"
        )

    locked_codes = tuple(
        sorted(unit_state.loc[unit_state["is_locked_price"], ARTICLE_COLUMN].astype(str).tolist())
    )
    filtered_db = prepared_db[~prepared_db["local_vendor_code"].isin(locked_codes)].copy()

    unit_codes = set(unit_state[ARTICLE_COLUMN].astype(str))
    absent_in_unit_codes = tuple(
        sorted(set(filtered_db["local_vendor_code"].astype(str)) - unit_codes)
    )
    filtered_db = filtered_db[filtered_db["local_vendor_code"].isin(unit_codes)].copy()

    # Ключевой момент: сопоставляем по business key, а не по номеру строки.
    # Это защищает от тихих ошибок при ручных перестановках строк в таблице.
    merged = filtered_db.merge(
        unit_state,
        how="left",
        left_on="local_vendor_code",
        right_on=ARTICLE_COLUMN,
        sort=False,
    )

    missing_price_mask = merged["price_per_item"].isna()
    missing_price_codes = tuple(
        sorted(merged.loc[missing_price_mask, "local_vendor_code"].astype(str).tolist())
    )
    merged = merged.loc[~missing_price_mask].copy()

    merged["unit_price"] = _normalize_numeric_series(merged["unit_price"]).round(0)
    merged["price_diff_rub"] = merged["unit_price"] - merged["price_per_item"]

    unit_price_denominator = merged["unit_price"].replace(0, np.nan)
    merged["price_diff_percent"] = (
        ((merged["price_per_item"] - merged["unit_price"]) / unit_price_denominator)
        .round(2)
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
    )

    suspicious_rows = merged.loc[
        merged["price_diff_percent"].abs() >= SUSPICIOUS_DIFF_THRESHOLD
    ].copy()
    changed_rows = merged.loc[merged["price_per_item"] != merged["unit_price"]].copy()

    logger.info(
        "Подготовка данных завершена: db_rows=%s rows_after_unit_match=%s changed_rows=%s suspicious_rows=%s",
        len(db_dataframe.index),
        len(merged.index),
        len(changed_rows.index),
        len(suspicious_rows.index),
    )

    return ProcessingResult(
        prepared_rows=merged,
        changed_rows=changed_rows,
        suspicious_rows=suspicious_rows,
        excluded_locked_codes=locked_codes,
        absent_in_unit_codes=absent_in_unit_codes,
        missing_price_codes=missing_price_codes,
    )


def build_report_dataframe(changed_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Подготавливает DataFrame отчета для листа истории изменений.

    Назначение:
    привести набор изменившихся строк к формату, который удобно сохранять
    в локальный CSV и добавлять в отдельный лист истории.

    Параметры:
    `changed_rows` — строки, в которых новая цена отличается от текущей.

    Возвращаемый результат:
    `pandas.DataFrame` в фиксированном порядке колонок, пригодный для отчета.

    Особенности поведения:
    дата поставки переводится в строку, а в колонку `insert_date` записывается
    дата формирования отчета.
    Если прежняя цена отсутствовала, в отчет подставляется `0`, чтобы поле
    разницы не оставалось пустым и корректно показывало отклонение.
    """

    report_dataframe = changed_rows.copy()
    report_dataframe["supply_date"] = pd.to_datetime(
        report_dataframe["supply_date"],
        errors="coerce",
    ).dt.date.astype(str)
    report_dataframe["insert_date"] = datetime.now().strftime("%Y-%m-%d")
    # Для листа истории пустую прежнюю цену трактуем как отсутствие базовой цены.
    # Это позволяет явно показать ноль и корректно посчитать разницу, вместо того
    # чтобы оставлять в отчете пустые ячейки, которые сложно интерпретировать.
    report_dataframe["unit_price"] = _normalize_numeric_series(
        report_dataframe["unit_price"]
    ).fillna(0)
    report_dataframe["price_per_item"] = _normalize_numeric_series(
        report_dataframe["price_per_item"]
    )
    report_dataframe["price_diff_rub"] = (
        report_dataframe["unit_price"] - report_dataframe["price_per_item"]
    )
    report_dataframe = report_dataframe.reindex(columns=list(REPORT_COLUMNS))
    return report_dataframe.fillna("")


def _ensure_required_columns(
    dataframe: pd.DataFrame,
    required_columns: tuple[str, ...],
    source_name: str,
) -> None:
    """Проверяет, что во входном DataFrame присутствуют все обязательные колонки."""

    missing_columns = [column for column in required_columns if column not in dataframe.columns]
    if missing_columns:
        raise MissingRequiredColumnsError(
            f"В источнике '{source_name}' отсутствуют обязательные колонки: "
            f"{', '.join(missing_columns)}"
        )


def _normalize_text(value: object) -> str:
    """Безопасно приводит значение к очищенной строке."""

    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_numeric_series(series: pd.Series) -> pd.Series:
    """
    Нормализует колонку с числовыми значениями.

    Обрабатываем типичные проблемы данных из Google Sheets:
    пробелы, неразрывные пробелы, запятую как разделитель дробной части,
    пустые строки и строковые `None`/`nan`.
    """

    normalized = (
        series.astype(str)
        .str.replace("\u00a0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip()
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
    )
    return pd.to_numeric(normalized, errors="coerce")


def _is_locked_price_flag(value: object) -> bool:
    """Возвращает True, если для SKU установлен признак неизменяемой цены."""

    return _normalize_text(value) == "1"


def _round_price_like_legacy(value: object) -> object:
    """Применяет округление, совместимое со старым скриптом."""

    if value is None or pd.isna(value):
        return np.nan
    return round(float(value) + 0.01)
