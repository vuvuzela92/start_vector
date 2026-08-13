from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

import gspread
import pandas as pd
import requests
from gspread.utils import a1_to_rowcol, rowcol_to_a1

from src_oop.jobs.autopilot_daily.config import (
    CURRENT_METRIC_TO_BASE_COLUMN,
    DAILY_SHEET,
    DISABLED_PU_METRICS,
    HISTORY_METRIC_TO_COLUMN,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DailyMetricWriteResult:
    """Результат записи одной дневной метрики в ПУ.

    Бизнес-логика:
    каждая метрика пишется отдельной порцией; результат нужен, чтобы дневной
    сценарий продолжал остальные метрики при частичной ошибке Google Sheets.
    """

    metric_name: str
    written: bool
    rows: int = 0
    error_message: str | None = None


class AutopilotDailySheetsWriter:
    """Writer фиксированных диапазонов дневного ПУ.

    Бизнес-логика:
    переносит legacy-поведение записи по статическим колонкам, но делает
    запись контролируемой: неизвестные и временно отключенные метрики
    пропускаются, а ошибки одной метрики не прерывают весь дневной сценарий.
    """

    def __init__(self, worksheet: gspread.Worksheet, today: date | None = None) -> None:
        """Создает writer для конкретного листа ПУ.

        Бизнес-логика:
        хранит ссылку на worksheet и параметры строк, чтобы все записи дневного
        сценария попадали в один и тот же пользовательский лист ПУ. Дата
        запуска нужна, чтобы явно выстроить последние завершенные дни и не
        сдвигать показатели, если в БД отсутствует один из дней.
        """
        self.worksheet = worksheet
        self.values_first_row = DAILY_SHEET.values_first_row
        self.today = today or date.today()

    def read_articles(self) -> list[int]:
        """Читает артикула ПУ в пользовательском порядке.

        Бизнес-логика:
        порядок первой колонки является главным порядком записи всех метрик.
        Нечисловые и пустые строки пропускаются, чтобы служебные строки не
        ломали дневную актуализацию.
        """
        raw_values = self.worksheet.col_values(DAILY_SHEET.articles_column_index)
        articles: list[int] = []
        for value in raw_values[self.values_first_row - 1 :]:
            text_value = str(value).strip()
            if text_value.isdigit():
                articles.append(int(text_value))
        logger.info("Артикулы для дневного ПУ прочитаны из Google Sheets: rows=%s", len(articles))
        return articles

    def write_current_metric(
        self,
        dataframe: pd.DataFrame,
        metric_name: str,
        articles: list[int],
    ) -> DailyMetricWriteResult:
        """Пишет одну текущую метрику за блок завершенных дней в ПУ.

        Бизнес-логика:
        текущие дневные метрики раскладываются pivot-таблицей по датам и
        артикулам. Отсутствующие артикулы и даты пишутся пустыми ячейками, а не
        нулями, чтобы не искажать управленческие показатели.
        """
        if metric_name in DISABLED_PU_METRICS:
            logger.info(
                "Метрика дневного ПУ временно пропущена, чтобы не затронуть спорную колонку: metric=%s",
                metric_name,
            )
            return DailyMetricWriteResult(metric_name=metric_name, written=True, rows=0)
        if metric_name not in CURRENT_METRIC_TO_BASE_COLUMN:
            return DailyMetricWriteResult(
                metric_name=metric_name,
                written=False,
                error_message=f"Для текущей метрики не задан диапазон: {metric_name}",
            )
        if dataframe.empty or metric_name not in dataframe.columns:
            logger.info("Текущая метрика дневного ПУ пропущена: metric=%s", metric_name)
            return DailyMetricWriteResult(metric_name=metric_name, written=True, rows=0)

        metric_frame = dataframe.copy()
        metric_frame["date"] = pd.to_datetime(metric_frame["date"]).dt.date
        metric_frame = metric_frame.pivot(
            columns="date",
            index="article_id",
            values=metric_name,
        ).reindex(articles)
        metric_frame = self._align_to_completed_dates(metric_frame, DAILY_SHEET.current_days_width)
        rows = self._dataframe_to_rows(metric_frame)
        start_column = CURRENT_METRIC_TO_BASE_COLUMN[metric_name]
        end_column = self._calculate_range_end(start_column, DAILY_SHEET.current_days_width)
        target_range = self._build_range(start_column, end_column, len(rows))
        return self._update_range(metric_name, target_range, rows)

    def write_history_metric(
        self,
        dataframe: pd.DataFrame,
        metric_name: str,
        articles: list[int],
    ) -> DailyMetricWriteResult:
        """Пишет одну историческую метрику в одну колонку ПУ.

        Бизнес-логика:
        средние показатели предыдущей недели и 30-дневные цены служат
        ориентирами для сравнения с текущей неделей. Отсутствующие данные
        остаются пустыми ячейками.
        """
        if metric_name not in HISTORY_METRIC_TO_COLUMN:
            return DailyMetricWriteResult(
                metric_name=metric_name,
                written=False,
                error_message=f"Для исторической метрики не задан диапазон: {metric_name}",
            )
        if dataframe.empty or metric_name not in dataframe.columns:
            logger.info("Историческая метрика дневного ПУ пропущена: metric=%s", metric_name)
            return DailyMetricWriteResult(metric_name=metric_name, written=True, rows=0)

        metric_frame = dataframe[["article_id", metric_name]].set_index("article_id")
        metric_frame = metric_frame.reindex(articles)
        rows = self._dataframe_to_rows(metric_frame)
        column = HISTORY_METRIC_TO_COLUMN[metric_name]
        target_range = self._build_range(column, column, len(rows))
        return self._update_range(metric_name, target_range, rows)

    def write_avg_positions(self, current_positions: pd.DataFrame, history_positions: dict[int, object]) -> None:
        """Пишет блоки средних позиций в ПУ.

        Бизнес-логика:
        позиции находятся в отдельных колонках ПУ и обновляются после основных
        метрик. Ошибка этого блока логируется отдельно, чтобы не скрывать
        результат записи продажных показателей.
        """
        try:
            current_positions = self._align_to_completed_dates(
                current_positions,
                DAILY_SHEET.avg_position_current_width,
            )
            current_rows = self._dataframe_to_rows(current_positions)
            if current_rows:
                current_range = self._build_range(
                    DAILY_SHEET.avg_position_current_range_start,
                    DAILY_SHEET.avg_position_current_range_end,
                    len(current_rows),
                )
                self._update_range("avg_position_current", current_range, current_rows)
            else:
                current_range = "не записывался"
            history_rows = [[self._sheet_value(value)] for value in history_positions.values()]
            if history_rows:
                history_range = self._build_range(
                    DAILY_SHEET.avg_position_history_column,
                    DAILY_SHEET.avg_position_history_column,
                    len(history_rows),
                )
                self._update_range("avg_position_history", history_range, history_rows)
            else:
                history_range = "не записывался"
            logger.info(
                "Средние позиции записаны в ПУ: current_range=%s history_range=%s",
                current_range,
                history_range,
            )
        except Exception:
            logger.exception("Не удалось записать средние позиции в ПУ, дневной сценарий продолжает работу.")

    def update_goods_info(self, info_by_article: dict[int, dict[str, object]]) -> int:
        """Обновляет предмет, ЛК и wild в колонках A:D ПУ.

        Бизнес-логика:
        артикула остаются в порядке ПУ, а справочные поля берутся из БД только
        если новое значение найдено. Это сохраняет ручные/старые значения для
        артикулов, которых нет в справочнике.
        """
        values = self.worksheet.get_values(
            f"A{DAILY_SHEET.source_header_row}:D{self.worksheet.row_count}"
        )
        if not values:
            logger.info("Обновление справочных колонок ПУ пропущено: диапазон A:D пуст.")
            return 0

        headers = values[0]
        rows = values[1:]
        dataframe = pd.DataFrame(rows, columns=headers)
        if "Артикул" not in dataframe.columns:
            logger.warning("Обновление справочных колонок ПУ пропущено: не найдена колонка Артикул.")
            return 0

        dataframe["Артикул"] = pd.to_numeric(dataframe["Артикул"], errors="coerce").astype("Int64")
        output_rows: list[list[object]] = []
        for row in dataframe.to_dict(orient="records"):
            article_value = row.get("Артикул")
            if pd.isna(article_value):
                continue
            article = int(article_value)
            info = info_by_article.get(article, {})
            output_rows.append(
                [
                    article,
                    info.get("category") or row.get("Предмет") or "",
                    info.get("account") or row.get("ЛК") or "",
                    info.get("local_vendor_code") or row.get("wild") or "",
                ]
            )

        if not output_rows:
            return 0

        end_row = self.values_first_row + len(output_rows) - 1
        target_range = f"A{self.values_first_row}:D{end_row}"
        self.worksheet.update(
            target_range,
            output_rows,
            value_input_option="USER_ENTERED",
        )
        logger.info("Справочные колонки ПУ обновлены: range=%s rows=%s", target_range, len(output_rows))
        return len(output_rows)

    def update_status(self, updated_at: datetime | None = None) -> None:
        """Обновляет служебную ячейку A2 временем дневной актуализации.

        Бизнес-логика:
        пользователь должен видеть, когда дневной сценарий завершил запись
        основного блока ПУ. Формат отображения сохранен как
        `Актуализировано на dd.mm.YYYY HH:MM:SS`.
        """
        updated_at = updated_at or datetime.now()
        value = f"Актуализировано на {updated_at.strftime('%d.%m.%Y %H:%M:%S')}"
        try:
            self.worksheet.update(
                range_name=DAILY_SHEET.status_cell,
                values=[[value]],
                value_input_option="USER_ENTERED",
            )
            logger.info("Статус дневной актуализации ПУ обновлен: cell=%s", DAILY_SHEET.status_cell)
        except Exception:
            logger.exception("Не удалось обновить служебную ячейку статуса дневного ПУ.")

    def _update_range(
        self,
        metric_name: str,
        target_range: str,
        rows: list[list[object]],
    ) -> DailyMetricWriteResult:
        """Безопасно записывает подготовленный диапазон Google Sheets.

        Бизнес-логика:
        реализует порционную запись с retry на 429. Если Google Sheets не
        принял одну метрику, сценарий возвращает ошибку по этой метрике и
        продолжает остальные показатели.
        """
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                self.worksheet.update(
                    target_range,
                    rows,
                    value_input_option="USER_ENTERED",
                )
                logger.info(
                    "Метрика дневного ПУ записана в Google Sheets: metric=%s range=%s rows=%s",
                    metric_name,
                    target_range,
                    len(rows),
                )
                return DailyMetricWriteResult(metric_name=metric_name, written=True, rows=len(rows))
            except gspread.exceptions.APIError as error:
                status_code = getattr(getattr(error, "response", None), "status_code", None)
                if self._is_retryable_google_error(error) and attempt < max_retries:
                    wait_seconds = self._retry_wait_seconds(attempt)
                    logger.warning(
                        "Google Sheets временно не принял запись дневной метрики, повторяем попытку: metric=%s range=%s status_code=%s attempt=%s/%s wait_seconds=%s",
                        metric_name,
                        target_range,
                        status_code,
                        attempt,
                        max_retries,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue
                logger.warning(
                    "Не удалось записать дневную метрику в Google Sheets, сценарий продолжает остальные метрики: metric=%s range=%s error=%s",
                    metric_name,
                    target_range,
                    error,
                )
                return DailyMetricWriteResult(metric_name=metric_name, written=False, error_message=str(error))
            except requests.exceptions.RequestException as error:
                if attempt < max_retries:
                    wait_seconds = self._retry_wait_seconds(attempt)
                    logger.warning(
                        "Сетевое соединение с Google Sheets прервано при записи дневной метрики, повторяем попытку: metric=%s range=%s attempt=%s/%s wait_seconds=%s error=%s",
                        metric_name,
                        target_range,
                        attempt,
                        max_retries,
                        wait_seconds,
                        error,
                    )
                    time.sleep(wait_seconds)
                    continue
                logger.warning(
                    "Не удалось записать дневную метрику в Google Sheets после сетевой ошибки, сценарий продолжает остальные метрики: metric=%s range=%s error=%s",
                    metric_name,
                    target_range,
                    error,
                )
                return DailyMetricWriteResult(metric_name=metric_name, written=False, error_message=str(error))
            except Exception as error:
                logger.exception(
                    "Неожиданная ошибка записи дневной метрики, сценарий продолжает остальные метрики: metric=%s range=%s",
                    metric_name,
                    target_range,
                )
                return DailyMetricWriteResult(metric_name=metric_name, written=False, error_message=str(error))

        return DailyMetricWriteResult(metric_name=metric_name, written=False, error_message="Повторы исчерпаны")

    def _build_range(self, start_column: str, end_column: str, rows_count: int) -> str:
        """Формирует A1-диапазон для записи блока ПУ.

        Бизнес-логика:
        централизует расчет строк, чтобы все дневные метрики начинались с
        четвертой строки и не затрагивали шапку или служебные строки ПУ.
        """
        last_row = self.values_first_row + max(rows_count, 1) - 1
        return f"{start_column}{self.values_first_row}:{end_column}{last_row}"

    @staticmethod
    def _calculate_range_end(start_column: str, width: int) -> str:
        """Считает последнюю колонку фиксированного блока метрики.

        Бизнес-логика:
        дневной ПУ хранит каждую daily-метрику блоком завершенных дней;
        расчет конца диапазона защищает от ручных ошибок в буквенных колонках.
        """
        _, column_index = a1_to_rowcol(f"{start_column}1")
        return rowcol_to_a1(1, column_index + width - 1).rstrip("1")

    def _align_to_completed_dates(self, dataframe: pd.DataFrame, width: int) -> pd.DataFrame:
        """Выравнивает табличный блок по последним завершенным датам.

        Бизнес-логика:
        дневная выгрузка должна писать каждую дату в свою колонку ПУ. Если в
        PostgreSQL нет данных за один из дней, эта дата остается пустой
        колонкой, а последующие дни не сдвигаются влево.
        """
        if dataframe.empty:
            return dataframe
        normalized = dataframe.copy()
        normalized.columns = [
            pd.to_datetime(column).date()
            if isinstance(column, (date, datetime, pd.Timestamp))
            else column
            for column in normalized.columns
        ]
        return normalized.reindex(self._completed_dates(width), axis=1)

    def _completed_dates(self, width: int) -> list[date]:
        """Возвращает даты последних завершенных дней для daily-блока.

        Бизнес-логика:
        текущий день принадлежит `autopilot_hourly_run`, поэтому daily всегда
        строит сетку от `today - width` до вчера включительно.
        """
        first_day = self.today - timedelta(days=width)
        return [
            first_day + timedelta(days=offset)
            for offset in range(width)
        ]

    @staticmethod
    def _is_retryable_google_error(error: gspread.exceptions.APIError) -> bool:
        """Определяет, стоит ли повторять ошибку Google Sheets API.

        Бизнес-логика:
        дневной сценарий пишет десятки крупных диапазонов, и временные ответы
        Google Sheets вроде 429 или 5xx не должны приводить к пропуску метрики
        без повторной попытки. Неретрайбл-ошибки, например неверный диапазон,
        возвращаются сразу, чтобы не ждать без пользы.
        """
        status_code = getattr(getattr(error, "response", None), "status_code", None)
        return status_code in {429, 500, 502, 503, 504}

    @staticmethod
    def _retry_wait_seconds(attempt: int) -> int:
        """Считает паузу перед повторной записью в Google Sheets.

        Бизнес-логика:
        небольшая возрастающая пауза снижает риск повторного 429 или повторного
        сетевого обрыва, но не задерживает дневной сценарий слишком надолго.
        """
        return attempt * 10

    @classmethod
    def _dataframe_to_rows(cls, dataframe: pd.DataFrame) -> list[list[object]]:
        """Преобразует DataFrame в безопасные значения Google Sheets.

        Бизнес-логика:
        пропуски, NaN и бесконечности превращаются в пустые ячейки, потому что
        для дневного ПУ отсутствие данных должно оставаться пропуском, а не
        нулем или текстом `nan`.
        """
        if dataframe.empty:
            return []
        safe_frame = dataframe.astype(object).where(pd.notnull(dataframe), "")
        return [
            [cls._sheet_value(value) for value in row]
            for row in safe_frame.values.tolist()
        ]

    @staticmethod
    def _pad_columns(dataframe: pd.DataFrame, width: int) -> pd.DataFrame:
        """Дополняет pivot текущей метрики до ширины daily-блока.

        Бизнес-логика:
        daily-диапазон ПУ имеет фиксированную ширину; если в БД пока меньше дат, пустые
        колонки сохраняют структуру листа и не смещают соседние показатели.
        """
        result = dataframe.copy()
        while len(result.columns) < width:
            result[f"empty_{len(result.columns)}"] = ""
        return result.iloc[:, :width]

    @classmethod
    def _normalize_width(cls, dataframe: pd.DataFrame, width: int) -> pd.DataFrame:
        """Приводит табличный блок к фиксированной ширине диапазона ПУ.

        Бизнес-логика:
        отдельные блоки ПУ, например средние позиции, имеют жесткую ширину.
        Лишние колонки из БД не должны выходить за пределы назначенного блока,
        а недостающие колонки должны очищать старые значения пустыми ячейками.
        """
        if dataframe.empty:
            return dataframe
        normalized = dataframe.reindex(sorted(dataframe.columns), axis=1)
        return cls._pad_columns(normalized.iloc[:, -width:], width)

    @staticmethod
    def _sheet_value(value: object) -> object:
        """Готовит одно значение для записи в Google Sheets.

        Бизнес-логика:
        Google Sheets не должен получать служебные pandas/float значения
        отсутствия данных или Decimal из PostgreSQL; пропуски заменяются на
        пустую строку, а Decimal переводится в обычное число, которое gspread
        может сериализовать в JSON без падения дневного сценария.
        """
        if value is None:
            return ""
        if isinstance(value, Decimal):
            if value == value.to_integral_value():
                return int(value)
            return float(value)
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return ""
        return value
