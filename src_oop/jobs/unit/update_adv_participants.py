from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from gspread.utils import ValueRenderOption, rowcol_to_a1

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.unit.config import (
    GOOGLE_DATA_START_ROW,
    GOOGLE_HEADER_ROW_INDEX,
    UNIT_ARTICLE_COLUMN,
    unit_gs,
)
from src_oop.jobs.unit.queries import query_adv_spend

logger = logging.getLogger(__name__)

ADVERT_MARK_COLUMN = "Реклама"
ADVERT_MARK_VALUE = "реклама"


@dataclass(frozen=True, slots=True)
class ArticleParsingResult:
    """Результат подготовки колонки `Артикул` для сценария обновления рекламы.

    Бизнес-логика:
    джобе нужны сразу два результата:
    1. очищенная серия артикулов для дальнейшего сопоставления;
    2. список пустых ячеек, которые допустимы для пропуска, но требуют
       отдельного контроля человеком после завершения задачи.
    """

    article_series: pd.Series
    empty_article_cells: list[str]


def _ensure_required_column(headers: list[str], column_name: str, *, sheet_name: str) -> int:
    """Проверяет наличие обязательной колонки в листе UNIT.

    Бизнес-логика:
    задача обновления признака рекламы не может работать по приблизительному
    совпадению названий колонок. Если нужной колонки нет, сценарий должен
    остановиться сразу и явно сообщить, какой лист требует ручной правки.
    """

    if column_name not in headers:
        raise ValueError(
            f"В листе '{sheet_name}' отсутствует обязательная колонка '{column_name}'."
        )
    return headers.index(column_name)


def _parse_article_series_with_cell_context(
    dataframe: pd.DataFrame,
    *,
    headers: list[str],
    sheet_name: str,
    table_title: str,
) -> ArticleParsingResult:
    """Готовит колонку `Артикул` к сопоставлению и собирает служебную диагностику.

    Бизнес-логика:
    признак участия в рекламе рассчитывается строго по артикулу. Если в колонке
    `Артикул` есть только пробелы по краям, их нужно автоматически убрать и
    продолжить работу. Если ячейка после очистки полностью пустая, строка не
    участвует в сопоставлении и остаётся без отметки `Реклама`. Ошибкой считаем
    только те случаи, когда после очистки значение осталось непустым, но так и
    не смогло превратиться в число. В таком случае функция показывает точную
    ячейку Google Sheets, которую нужно исправить вручную. Для пустых ячеек
    функция отдельно возвращает список адресов, чтобы их можно было показать
    в финале работы сценария.
    """

    article_column_index = _ensure_required_column(
        headers,
        UNIT_ARTICLE_COLUMN,
        sheet_name=sheet_name,
    )
    raw_series = dataframe[UNIT_ARTICLE_COLUMN].fillna("").astype(str)
    normalized_series = raw_series.str.strip()
    numeric_series = pd.to_numeric(normalized_series, errors="coerce")
    empty_mask = normalized_series.eq("")

    invalid_mask = normalized_series.ne("") & numeric_series.isna()

    if invalid_mask.any():
        first_problem_index = int(invalid_mask[invalid_mask].index[0])
        google_row = GOOGLE_DATA_START_ROW + first_problem_index
        google_column = article_column_index + 1
        cell_a1 = rowcol_to_a1(google_row, google_column)
        raw_value = raw_series.iloc[first_problem_index]

        raise ValueError(
            "В колонке с артикулами найдено некорректное значение. "
            f"Таблица='{table_title}', лист='{sheet_name}', "
            f"колонка='{UNIT_ARTICLE_COLUMN}', строка={google_row}, "
            f"ячейка='{cell_a1}', значение={raw_value!r}."
        )

    empty_article_cells = [
        rowcol_to_a1(GOOGLE_DATA_START_ROW + int(index), article_column_index + 1)
        for index in empty_mask[empty_mask].index.tolist()
    ]

    return ArticleParsingResult(
        article_series=numeric_series,
        empty_article_cells=empty_article_cells,
    )


def update_adv_participants_to_gs() -> None:
    """Обновляет в UNIT признак участия артикула в рекламе за прошлый день.

    Бизнес-логика:
    сценарий получает список артикулов, по которым за вчера были реальные
    рекламные расходы, затем проходит по строкам листа UNIT и заполняет
    колонку `Реклама` только для таких товаров. Пустые ячейки в колонке
    `Артикул` считаются допустимыми и просто пропускаются. Если значение после
    очистки от пробелов остаётся непустым, но не является числом, задача
    завершается с точным адресом проблемной ячейки в Google Sheets.
    Заголовки и две нужные колонки читаются отдельно. Общий клиент делает
    до четырёх попыток при тайм-аутах, сетевых сбоях и HTTP 429/500/502/503/504.
    Сначала читаются исходные значения в режиме FORMULA: обычные заголовки
    и артикулы не требуют получения вычисленного представления листа. Если
    нужный заголовок или артикул задан формулой, вычисленные значения читаются
    отдельно. Формулы в колонке рекламы не вычисляются: нужен только её размер,
    чтобы очистить прежние отметки в хвосте.
    Запись использует найденный индекс колонки без повторного чтения заголовков.
    Ошибка после исчерпания попыток прерывает задачу; успешное обновление
    сообщается только после подтверждения записи. Если обе колонки пусты,
    запись не выполняется. Старые отметки ниже списка артикулов очищаются.
    """

    database = Database()
    adv_spend = database.read_sql_to_dataframe(query_adv_spend)

    table_title = unit_gs.title
    sheet_title = unit_gs.sheet
    google_tabs = GoogleTabs(table_title, sheet_title)

    headers = google_tabs.execute_with_retry(
        "Чтение заголовков UNIT для обновления рекламы",
        google_tabs.sheet_title.row_values,
        GOOGLE_HEADER_ROW_INDEX,
        value_render_option=ValueRenderOption.formula,
    )
    required_headers = (UNIT_ARTICLE_COLUMN, ADVERT_MARK_COLUMN)
    if any(name not in headers for name in required_headers) and any(
        isinstance(value, str) and value.startswith("=") for value in headers
    ):
        headers = google_tabs.execute_with_retry(
            "Чтение вычисляемых заголовков UNIT для обновления рекламы",
            google_tabs.sheet_title.row_values,
            GOOGLE_HEADER_ROW_INDEX,
            value_render_option=ValueRenderOption.formatted,
        )
    if not headers:
        raise ValueError(
            f"Лист '{sheet_title}' таблицы '{table_title}' пуст и не содержит заголовков."
        )

    article_index = _ensure_required_column(
        headers, UNIT_ARTICLE_COLUMN, sheet_name=sheet_title
    )
    advert_index = _ensure_required_column(headers, ADVERT_MARK_COLUMN, sheet_name=sheet_title)
    article_column = rowcol_to_a1(1, article_index + 1)[:-1]
    advert_column = rowcol_to_a1(1, advert_index + 1)[:-1]
    ranges = [
        f"{article_column}{GOOGLE_DATA_START_ROW}:{article_column}",
        f"{advert_column}{GOOGLE_DATA_START_ROW}:{advert_column}",
    ]
    article_values, advert_values = google_tabs.execute_with_retry(
        "Чтение артикулов и отметок рекламы UNIT",
        google_tabs.sheet_title.batch_get,
        ranges,
        value_render_option=ValueRenderOption.formula,
    )
    if any(
        row and isinstance(row[0], str) and row[0].startswith("=")
        for row in article_values
    ):
        article_values = google_tabs.execute_with_retry(
            "Чтение вычисляемых артикулов UNIT для обновления рекламы",
            google_tabs.sheet_title.batch_get,
            [ranges[0]],
            value_render_option=ValueRenderOption.unformatted,
        )[0]
    row_count = max(len(article_values), len(advert_values))
    if row_count == 0:
        logger.info(
            "Обновление рекламы UNIT завершено без записи: колонки артикулов "
            "и рекламы пусты | table=%s | sheet=%s",
            table_title,
            sheet_title,
        )
        return
    dataframe = pd.DataFrame(
        {
            UNIT_ARTICLE_COLUMN: [row[0] if row else "" for row in article_values]
            + [""] * (row_count - len(article_values)),
        }
    )
    article_parsing_result = _parse_article_series_with_cell_context(
        dataframe,
        headers=headers,
        sheet_name=sheet_title,
        table_title=table_title,
    )
    dataframe[UNIT_ARTICLE_COLUMN] = article_parsing_result.article_series

    articles_with_spend = set(
        pd.to_numeric(adv_spend["article_id"], errors="coerce").dropna().astype(int)
    )
    logger.info(
        "Подготовлен список артикулов с рекламными расходами: count=%s.",
        len(articles_with_spend),
    )

    result_values = [
        ADVERT_MARK_VALUE if pd.notna(article) and int(article) in articles_with_spend else ""
        for article in dataframe[UNIT_ARTICLE_COLUMN].tolist()
    ]

    logger.info(
        "Подготовлены значения для обновления колонки рекламы: table='%s', sheet='%s', rows=%s.",
        table_title,
        sheet_title,
        len(result_values),
    )
    start_cell = rowcol_to_a1(GOOGLE_DATA_START_ROW, advert_index + 1)
    end_cell = rowcol_to_a1(GOOGLE_DATA_START_ROW + row_count - 1, advert_index + 1)
    google_tabs.execute_with_retry(
        "Запись отметок рекламы UNIT",
        google_tabs.sheet_title.update,
        range_name=f"{start_cell}:{end_cell}",
        values=[[value] for value in result_values],
        value_input_option="RAW",
    )
    logger.info(
        "Колонка признака рекламы обновлена в Google Sheets: table='%s', sheet='%s', column='%s', rows=%s.",
        table_title,
        sheet_title,
        ADVERT_MARK_COLUMN,
        len(result_values),
    )

    if article_parsing_result.empty_article_cells:
        empty_cells_text = ", ".join(article_parsing_result.empty_article_cells)
        logger.warning(
            "При обновлении рекламы пропущены пустые ячейки в колонке артикулов: count=%s, cells=%s.",
            len(article_parsing_result.empty_article_cells),
            empty_cells_text,
        )
        print(
            "\nПропущены пустые ячейки в колонке 'Артикул':\n"
            f"{empty_cells_text}"
        )
