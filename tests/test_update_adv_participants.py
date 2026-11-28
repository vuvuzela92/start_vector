"""Проверяет восстановление обновления рекламы UNIT после сбоев Google Sheets."""

import json
import logging
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests
from gspread.exceptions import APIError
from gspread.utils import ValueRenderOption

from src_oop.core import my_gspread
from src_oop.jobs.unit import update_adv_participants as job


def _api_error(status: int) -> APIError:
    """Создаёт ответ Google без сети для проверки восстановления выгрузки рекламы."""
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(
        {"error": {"code": status, "message": "Тестовая ошибка Google Sheets"}}
    ).encode("utf-8")
    return APIError(response)


@pytest.fixture
def google_tabs(monkeypatch: pytest.MonkeyPatch) -> my_gspread.GoogleTabs:
    """Изолирует БД и HTTP, сохраняя настоящий механизм повторов для рекламы UNIT."""
    with patch.object(my_gspread.GoogleTabs, "_safe_connect"):
        client = my_gspread.GoogleTabs("UNIT", "MAIN")
    worksheet = Mock()
    worksheet.row_values.return_value = ["ЛК", "Реклама", "Артикул"]
    # Пустая строка внутри списка и старая отметка в хвосте не должны сдвигать запись.
    worksheet.batch_get.return_value = [
        [["101"], [], ["202"]],
        [["старое"], ["старое"], ["старое"], ["старое"]],
    ]
    client.sheet_title = worksheet
    database = Mock()
    database.read_sql_to_dataframe.return_value = pd.DataFrame({"article_id": [101]})
    monkeypatch.setattr(job, "Database", Mock(return_value=database))
    monkeypatch.setattr(job, "GoogleTabs", Mock(return_value=client))
    monkeypatch.setattr(my_gspread.time, "sleep", Mock())
    return client


@pytest.mark.parametrize("operation_name", ["row_values", "batch_get", "update"])
@pytest.mark.parametrize("failure", [500, 429, "timeout"])
def test_transient_failure_recovers_without_shifting_rows(
    google_tabs: my_gspread.GoogleTabs, operation_name: str, failure: int | str
) -> None:
    """Временный сбой чтения или записи не мешает корректно обновить отметки товаров."""
    operation = getattr(google_tabs.sheet_title, operation_name)
    error = (
        requests.exceptions.ReadTimeout()
        if failure == "timeout"
        else _api_error(int(failure))
    )
    operation.side_effect = [error, operation.return_value]

    job.update_adv_participants_to_gs()

    assert operation.call_count == 2
    my_gspread.time.sleep.assert_called_once()
    google_tabs.sheet_title.update.assert_called_with(
        range_name="B2:B5",
        values=[["реклама"], [""], [""], [""]],
        value_input_option="RAW",
    )
    google_tabs.sheet_title.batch_get.assert_called_with(
        ["C2:C", "B2:B"], value_render_option=ValueRenderOption.formula
    )
    google_tabs.sheet_title.row_values.assert_called_with(
        1, value_render_option=ValueRenderOption.formula
    )
    assert google_tabs.sheet_title.row_values.call_count == (
        2 if operation_name == "row_values" else 1
    )


@pytest.mark.parametrize("status", [502, 503, 504])
def test_other_temporary_api_errors_are_retried(
    google_tabs: my_gspread.GoogleTabs, status: int
) -> None:
    """Временная недоступность Google не обрывает обновление рекламы при первом ответе."""
    read = google_tabs.sheet_title.row_values
    read.side_effect = [_api_error(status), read.return_value]

    job.update_adv_participants_to_gs()

    assert read.call_count == 2
    google_tabs.sheet_title.update.assert_called_once()


@pytest.mark.parametrize("operation_name", ["row_values", "batch_get", "update"])
def test_persistent_failure_stops_job_without_success_log(
    google_tabs: my_gspread.GoogleTabs,
    operation_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Неустранимый сбой прерывает задачу без ложного сообщения об обновлении рекламы."""
    operation = getattr(google_tabs.sheet_title, operation_name)
    error = _api_error(500)
    operation.side_effect = error

    with caplog.at_level(logging.INFO), pytest.raises(APIError) as caught:
        job.update_adv_participants_to_gs()

    assert caught.value is error
    assert operation.call_count == my_gspread.GOOGLE_READ_RETRY_ATTEMPTS
    assert my_gspread.time.sleep.call_count == my_gspread.GOOGLE_READ_RETRY_ATTEMPTS - 1
    assert "Колонка признака рекламы обновлена" not in caplog.text
    if operation_name != "update":
        google_tabs.sheet_title.update.assert_not_called()


@pytest.mark.parametrize("status", [400, 403, 404])
def test_permanent_api_error_is_not_retried(
    google_tabs: my_gspread.GoogleTabs, status: int
) -> None:
    """Ошибки доступа и диапазонов сразу останавливают запись рекламных отметок."""
    google_tabs.sheet_title.row_values.side_effect = _api_error(status)

    with pytest.raises(APIError):
        job.update_adv_participants_to_gs()

    google_tabs.sheet_title.row_values.assert_called_once()
    google_tabs.sheet_title.update.assert_not_called()
    my_gspread.time.sleep.assert_not_called()


def test_quota_retry_respects_retry_after(google_tabs: my_gspread.GoogleTabs) -> None:
    """Пауза Google при квоте имеет приоритет, чтобы задача не усиливала перегрузку."""
    error = _api_error(429)
    error.response.headers["Retry-After"] = "90"
    read = google_tabs.sheet_title.row_values
    read.side_effect = [error, read.return_value]

    job.update_adv_participants_to_gs()

    my_gspread.time.sleep.assert_called_once_with(90)


def test_empty_columns_do_not_send_invalid_write(google_tabs: my_gspread.GoogleTabs) -> None:
    """Пустой список товаров без прежних отметок завершается без записи диапазона B2:B1."""
    google_tabs.sheet_title.batch_get.return_value = [[], []]

    job.update_adv_participants_to_gs()

    google_tabs.sheet_title.update.assert_not_called()


def test_missing_advert_header_stops_before_write(google_tabs: my_gspread.GoogleTabs) -> None:
    """Отсутствие колонки рекламы запрещает запись в другую колонку UNIT."""
    google_tabs.sheet_title.row_values.return_value = ["ЛК", "Артикул"]

    with pytest.raises(ValueError, match="Реклама"):
        job.update_adv_participants_to_gs()

    google_tabs.sheet_title.batch_get.assert_not_called()
    google_tabs.sheet_title.update.assert_not_called()


def test_formula_header_uses_evaluated_name(google_tabs: my_gspread.GoogleTabs) -> None:
    """Заголовок из формулы остаётся доступен для поиска нужной колонки рекламы."""
    google_tabs.sheet_title.row_values.side_effect = [
        ["ЛК", '="Реклама"', "Артикул"],
        ["ЛК", "Реклама", "Артикул"],
    ]

    job.update_adv_participants_to_gs()

    assert google_tabs.sheet_title.row_values.call_count == 2
    google_tabs.sheet_title.row_values.assert_called_with(
        1, value_render_option=ValueRenderOption.formatted
    )
    google_tabs.sheet_title.update.assert_called_once()


def test_formula_article_is_evaluated_before_matching(google_tabs: my_gspread.GoogleTabs) -> None:
    """Формулы артикулов вычисляются отдельно, сохраняя привязку рекламы к товару."""
    google_tabs.sheet_title.batch_get.side_effect = [
        [[["=100+1"], [], ["202"]], [["старое"], ["старое"], ["старое"], ["старое"]]],
        [[[101], [], [202]]],
    ]

    job.update_adv_participants_to_gs()

    assert google_tabs.sheet_title.batch_get.call_count == 2
    google_tabs.sheet_title.batch_get.assert_called_with(
        ["C2:C"], value_render_option=ValueRenderOption.unformatted
    )
    google_tabs.sheet_title.update.assert_called_once_with(
        range_name="B2:B5",
        values=[["реклама"], [""], [""], [""]],
        value_input_option="RAW",
    )


def test_unrelated_formulas_do_not_trigger_evaluation(
    google_tabs: my_gspread.GoogleTabs,
) -> None:
    """Формулы других заголовков и старых отметок не заставляют вычислять лист UNIT."""
    google_tabs.sheet_title.row_values.return_value.append('="Расчётная колонка"')
    google_tabs.sheet_title.batch_get.return_value[1][3] = ['="реклама"']

    job.update_adv_participants_to_gs()

    google_tabs.sheet_title.row_values.assert_called_once()
    google_tabs.sheet_title.batch_get.assert_called_once()
    assert google_tabs.sheet_title.update.call_args.kwargs["values"][-1] == [""]
