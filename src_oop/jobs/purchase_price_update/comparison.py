from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from src_oop.jobs.purchase_price_update.processor import (
    ProcessingResult,
    prepare_purchase_price_updates,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ComparisonSummary:
    """
    Сводка сравнения новой реализации с legacy-совместимым результатом.

    Назначение:
    помогает быстро понять, совпадают ли две версии логики на одном и том же входе,
    не переходя к детальному разбору всех промежуточных DataFrame.
    """

    legacy_rows: int
    new_rows: int
    same_row_count: bool
    same_order: bool
    same_prices: bool
    only_in_legacy: tuple[str, ...]
    only_in_new: tuple[str, ...]
    price_mismatches: pd.DataFrame


def build_legacy_changed_rows(
    db_dataframe: pd.DataFrame,
    unit_state: pd.DataFrame,
    round_price: bool,
) -> pd.DataFrame:
    """
    Строит набор изменившихся строк в legacy-совместимом формате.

    Назначение:
    привести результат к стабильному набору колонок, который удобно сравнивать
    с новой реализацией без записи в Google Sheets.

    Параметры:
    `db_dataframe` — данные из PostgreSQL.
    `unit_state` — подготовленное состояние листа `Сопост`.
    `round_price` — применять ли legacy-округление.

    Возвращаемый результат:
    `pandas.DataFrame` только с теми колонками, которые важны для сравнения.
    """

    comparison_result = prepare_purchase_price_updates(
        db_dataframe=db_dataframe,
        unit_state=unit_state,
        round_price=round_price,
    )

    legacy_changed = comparison_result.changed_rows.copy()
    return legacy_changed[
        [
            "local_vendor_code",
            "product_name",
            "price_per_item",
            "unit_price",
            "price_diff_rub",
            "price_diff_percent",
            "__row_number",
        ]
    ].reset_index(drop=True)


def compare_with_legacy(
    db_dataframe: pd.DataFrame,
    unit_state: pd.DataFrame,
    round_price: bool,
) -> tuple[ComparisonSummary, ProcessingResult]:
    """
    Сравнивает новую реализацию подготовки изменений с legacy-совместимым результатом.

    Назначение:
    убедиться, что перенос логики в новый OOP-модуль не изменил итоговый результат.

    Параметры:
    `db_dataframe` — данные из PostgreSQL для сравнения.
    `unit_state` — подготовленное состояние листа `Сопост`.
    `round_price` — применять ли legacy-округление.

    Возвращаемый результат:
    кортеж из двух объектов:
    1. `ComparisonSummary` с итогом сравнения;
    2. `ProcessingResult` новой реализации, если нужно продолжить анализ глубже.

    Особенности поведения:
    функция сравнивает не только количество строк, но и порядок SKU, а также
    фактические значения цен и привязку к строкам листа.
    """

    new_result = prepare_purchase_price_updates(
        db_dataframe=db_dataframe,
        unit_state=unit_state,
        round_price=round_price,
    )
    legacy_changed = build_legacy_changed_rows(
        db_dataframe=db_dataframe,
        unit_state=unit_state,
        round_price=round_price,
    )
    new_changed = _normalize_for_comparison(new_result.changed_rows)

    legacy_codes = tuple(legacy_changed["local_vendor_code"].astype(str).tolist())
    new_codes = tuple(new_changed["local_vendor_code"].astype(str).tolist())
    only_in_legacy = tuple(sorted(set(legacy_codes) - set(new_codes)))
    only_in_new = tuple(sorted(set(new_codes) - set(legacy_codes)))

    price_mismatches = _build_price_mismatches(
        legacy_changed=legacy_changed,
        new_changed=new_changed,
    )
    summary = ComparisonSummary(
        legacy_rows=len(legacy_changed.index),
        new_rows=len(new_changed.index),
        same_row_count=len(legacy_changed.index) == len(new_changed.index),
        same_order=legacy_codes == new_codes,
        same_prices=price_mismatches.empty,
        only_in_legacy=only_in_legacy,
        only_in_new=only_in_new,
        price_mismatches=price_mismatches,
    )

    logger.info(
        "Сравнение legacy/new purchase_price_update: legacy_rows=%s new_rows=%s same_order=%s same_prices=%s only_in_legacy=%s only_in_new=%s mismatches=%s",
        summary.legacy_rows,
        summary.new_rows,
        summary.same_order,
        summary.same_prices,
        len(summary.only_in_legacy),
        len(summary.only_in_new),
        len(summary.price_mismatches.index),
    )
    return summary, new_result


def _normalize_for_comparison(changed_rows: pd.DataFrame) -> pd.DataFrame:
    return changed_rows[
        [
            "local_vendor_code",
            "product_name",
            "price_per_item",
            "unit_price",
            "price_diff_rub",
            "price_diff_percent",
            "__row_number",
        ]
    ].reset_index(drop=True)


def _build_price_mismatches(
    legacy_changed: pd.DataFrame,
    new_changed: pd.DataFrame,
) -> pd.DataFrame:
    merged = legacy_changed.merge(
        new_changed,
        how="inner",
        on="local_vendor_code",
        suffixes=("_legacy", "_new"),
    )

    mismatch_mask = (
        merged["price_per_item_legacy"] != merged["price_per_item_new"]
    ) | (
        merged["unit_price_legacy"] != merged["unit_price_new"]
    ) | (
        merged["__row_number_legacy"] != merged["__row_number_new"]
    )

    mismatch_columns = [
        "local_vendor_code",
        "price_per_item_legacy",
        "price_per_item_new",
        "unit_price_legacy",
        "unit_price_new",
        "__row_number_legacy",
        "__row_number_new",
    ]
    return merged.loc[mismatch_mask, mismatch_columns].reset_index(drop=True)
