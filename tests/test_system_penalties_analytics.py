from datetime import date

import pandas as pd

from src_oop.jobs.fin_reports_analyze.system_penalties_analytics import (
    SystemPenaltiesAnalyzer,
)


def test_normalize_month_start_uses_first_day() -> None:
    """Период аналитики всегда начинается с первого дня календарного месяца."""

    assert SystemPenaltiesAnalyzer._normalize_month_start("2026-08-19") == date(2026, 8, 1)
    assert SystemPenaltiesAnalyzer._next_month(date(2026, 12, 1)) == date(2027, 1, 1)


def test_calculate_intervals_marks_inconsistent_sequence() -> None:
    """Отрицательная последовательность событий должна быть видна оператору."""

    source = pd.DataFrame(
        {
            "service_task_found": [True],
            "wb_created_at": ["2026-08-01T10:00:00+03:00"],
            "shipped_at": ["2026-08-01T09:00:00+03:00"],
            "wb_status_sorted": ["2026-08-01T11:00:00+03:00"],
            "has_shipped": [True],
            "has_wb_sorted": [True],
            "status_row_count": [2],
        }
    )

    result = SystemPenaltiesAnalyzer._calculate_intervals(source)

    assert result.loc[0, "event_data_quality"] == "аномальная последовательность дат"
    assert result.loc[0, "hours_created_to_shipped"] == -1


def test_classify_zero_stock_unshipped_order_as_inventory() -> None:
    """Невыполненный заказ без отгрузки и с нулевым остатком относится к остаткам."""

    source = pd.DataFrame(
        {
            "bonus_type_name": ["Штраф МП. Невыполненный заказ (отмена продавцом)"],
            "stock_on_order_date": [0],
            "service_task_found": [True],
            "has_shipped": [False],
            "has_wb_sorted": [False],
        }
    )

    result = SystemPenaltiesAnalyzer._classify(source)

    assert result.loc[0, "probable_responsible_department"] == "планирование / закупки / остатки"
    assert result.loc[0, "classification_confidence"] == "high"


def test_classify_wrong_item_as_warehouse() -> None:
    """Основание отправки другого товара имеет приоритет над статусами заказа."""

    source = pd.DataFrame(
        {
            "bonus_type_name": [
                "Штраф МП. Невыполненный заказ (отправка товара отличного от заявленного)"
            ],
            "stock_on_order_date": [10],
            "service_task_found": [True],
            "has_shipped": [True],
            "has_wb_sorted": [True],
        }
    )

    result = SystemPenaltiesAnalyzer._classify(source)

    assert result.loc[0, "probable_responsible_department"] == "склад / комплектация"
    assert result.loc[0, "classification_confidence"] == "high"


def test_acceptance_act_marks_wb_stage() -> None:
    """Сформированный акт приемки переносит обычный кейс в спорную зону WB."""

    source = pd.DataFrame(
        {
            "bonus_type_name": ["Штраф МП. Невыполненный заказ"],
            "stock_on_order_date": [5],
            "service_task_found": [True],
            "has_shipped": [True],
            "has_wb_sorted": [False],
            "acceptance_act_found": [True],
        }
    )

    result = SystemPenaltiesAnalyzer._classify(source)

    assert result.loc[0, "responsibility_stage"] == "после формирования акта приемки WB"
    assert result.loc[0, "probable_responsible_department"] == "WB / спорный кейс"
