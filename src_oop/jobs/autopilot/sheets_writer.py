from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime

import gspread
import pandas as pd
from gspread.utils import a1_to_rowcol, rowcol_to_a1

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.autopilot.config import (
    AUTOPILOT_DATE_COLUMN_OFFSET,
    AUTOPILOT_STATUS_CELL,
    AUTOPILOT_VALUES_FIRST_ROW,
    METRIC_TO_BASE_COLUMN,
)
from src_oop.jobs.autopilot.models import MetricValues

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MetricWriteResult:
    metric_name: str
    written: bool
    rows: int = 0
    error_message: str | None = None


class AutopilotSheetsWriter:
    def __init__(
        self,
        connector: GoogleTabs,
        values_first_row: int = AUTOPILOT_VALUES_FIRST_ROW,
        date_column_offset: int = AUTOPILOT_DATE_COLUMN_OFFSET,
    ) -> None:
        """
        Создает writer для целевого листа ПУ.

        Бизнес-логика:
        хранит настройки первой строки данных и смещения дневной колонки,
        чтобы все метрики писались в тот же почасовой блок, что и в legacy.
        """
        self.connector = connector
        self.worksheet = connector.sheet_title
        self.values_first_row = values_first_row
        self.date_column_offset = date_column_offset

    def read_articles(self) -> list[int]:
        """
        Читает список артикулов из первой колонки ПУ.

        Бизнес-логика:
        именно порядок этих артикулов определяет порядок записи всех метрик.
        Нечисловые строки пропускаются как служебные или пустые.
        """
        raw_articles = self.worksheet.col_values(1)[self.values_first_row - 1 :]
        articles: list[int] = []
        for value in raw_articles:
            value_text = str(value).strip()
            if value_text.isdigit():
                articles.append(int(value_text))
        logger.info("Артикулы загружены из листа ПУ: rows=%s", len(articles))
        return articles

    def write_metric(
        self,
        metric_name: str,
        values_by_article: MetricValues,
        articles: list[int],
    ) -> MetricWriteResult:
        """
        Записывает одну метрику в фиксированную колонку ПУ.

        Бизнес-логика:
        каждая метрика пишется отдельной порцией. Если запись одной метрики упала,
        job продолжит следующие метрики. Для отсутствующих артикулов пишется пустая ячейка.
        """
        if metric_name not in METRIC_TO_BASE_COLUMN:
            return MetricWriteResult(
                metric_name=metric_name,
                written=False,
                error_message=f"Метрика {metric_name} не настроена для записи в ПУ.",
            )
        if not values_by_article:
            logger.info("Метрика ПУ пропущена, потому что нет значений для записи: metric=%s", metric_name)
            return MetricWriteResult(metric_name=metric_name, written=True, rows=0)

        rows: list[list[object]] = []
        for article_id in articles:
            if article_id not in values_by_article:
                rows.append([""])
                continue
            rows.append([self._sheet_value(values_by_article[article_id])])

        target_column = self._offset_column(
            METRIC_TO_BASE_COLUMN[metric_name],
            self.date_column_offset,
        )
        last_row = self.values_first_row + len(rows) - 1
        target_range = f"{target_column}{self.values_first_row}:{target_column}{last_row}"

        try:
            self.worksheet.update(
                target_range,
                rows,
                value_input_option="USER_ENTERED",
            )
            logger.info(
                "Метрика ПУ записана в Google Sheets: metric=%s range=%s rows=%s",
                metric_name,
                target_range,
                len(rows),
            )
            return MetricWriteResult(metric_name=metric_name, written=True, rows=len(rows))
        except gspread.exceptions.APIError as error:
            logger.warning(
                "Не удалось записать метрику ПУ в Google Sheets, сценарий продолжает остальные метрики: "
                "metric=%s range=%s error=%s",
                metric_name,
                target_range,
                error,
            )
            return MetricWriteResult(
                metric_name=metric_name,
                written=False,
                error_message=str(error),
            )
        except Exception as error:
            logger.exception(
                "Неожиданная ошибка записи метрики ПУ, сценарий продолжает остальные метрики: "
                "metric=%s range=%s",
                metric_name,
                target_range,
            )
            return MetricWriteResult(
                metric_name=metric_name,
                written=False,
                error_message=str(error),
            )

    def update_status(self, updated_at: datetime | None = None) -> None:
        """
        Обновляет служебную ячейку статуса в ПУ.

        Бизнес-логика:
        показывает пользователям таблицы, когда hourly job записал свежий блок Воронки продаж,
        потому что именно этот блок актуализируется в ПУ первым.
        """
        updated_at = updated_at or datetime.now()
        value = f"Актуализировано на {updated_at.strftime('%d.%m.%Y %H:%M:%S')}"
        try:
            self.worksheet.update(
                range_name=AUTOPILOT_STATUS_CELL,
                values=[[value]],
                value_input_option="USER_ENTERED",
            )
        except Exception:
            logger.exception("Не удалось обновить служебную ячейку статуса ПУ.")

    @staticmethod
    def _offset_column(column: str, offset: int) -> str:
        """
        Смещает базовую колонку метрики на номер дневного блока.

        Бизнес-логика:
        legacy пишет показатели не в базовую колонку, а в колонку текущего блока ПУ;
        смещение `AUTOPILOT_DATE_COLUMN_OFFSET` сохраняет эту схему.
        """
        _, column_index = a1_to_rowcol(f"{column}1")
        return rowcol_to_a1(1, column_index + offset - 1).rstrip("1")

    @staticmethod
    def _sheet_value(value: object) -> object:
        """
        Приводит Python-значение к безопасному значению для Google Sheets.

        Бизнес-логика:
        отсутствующие или бесконечные значения не должны попадать в ПУ как текст `nan`
        или `inf`, поэтому записываются пустой ячейкой.
        """
        if value is None:
            return ""
        if isinstance(value, float) and (pd.isna(value) or value in {float("inf"), float("-inf")}):
            return ""
        return value


def execute_google_write_pause(seconds: float = 0.2) -> None:
    """
    Делает короткую паузу между порционными записями в Google Sheets.

    Бизнес-логика:
    снижает риск 429 при последовательной записи большого набора метрик в ПУ.
    """
    time.sleep(seconds)
