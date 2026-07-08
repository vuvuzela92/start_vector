from __future__ import annotations
"""Обновление конкурентных колонок в UNIT на основе данных из ClickHouse.

Этот модуль покрывает только функциональность бывшего `competitors_prices.py`.
Логика бывшего `competitors_list.py` из проекта уже удалена по бизнес-решению.

Pipeline текущей джобы:
1. Читаем строки UNIT из `MAIN (tested)` в их исходном порядке.
2. Читаем данные о конкурентах и наших ценах из ClickHouse.
3. Строим lookup-словари:
   - `wild -> артикул конкурента` для каждого конкурентного слота;
   - `wild -> цена конкурента` для каждого конкурентного слота;
   - `наш артикул -> наша цена после СПП`.
4. Собираем матрицы значений строго по строкам UNIT.
5. Обновляем только нужные колонки листа, а не весь лист целиком.

Главный инвариант модуля:
- количество строк в каждой матрице должно точно совпадать с количеством строк
  в UNIT. Если это не так, запись в Google Sheets запрещена.
"""

import logging
from dataclasses import dataclass

import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.clickhouse import ClickHouseDatabase
from src_oop.jobs.unit.config import (
    COMPETITOR_ARTICLE_COLUMN,
    COMPETITOR_NAME_COLUMN,
    COMPETITOR_POSITION_COLUMN,
    COMPETITOR_PRICE_COLUMN,
    COMPETITOR_PRICE_TARGET_COLUMNS,
    COMPETITOR_TARGET_COLUMNS,
    FIXED_COMPETITOR_NAMES,
    GOOGLE_DATA_START_ROW,
    GOOGLE_HEADER_ROW_INDEX,
    NO_COMPETITOR_VALUE,
    OUR_ARTICLE_COLUMN,
    OUR_PRICE_TARGET_COLUMN,
    UNIT_ARTICLE_COLUMN,
    UNIT_WILD_COLUMN,
)
from src_oop.jobs.unit.queries import (
    query_competitors_positions,
    query_our_article_prices,
)
from src_oop.jobs.unit.unit import UnitEconomics

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ColumnUpdatePlan:
    """Описание одной операции записи в Google Sheets.

    `column_names`
        Имена колонок, которые должны быть обновлены в листе UNIT.

    `matrix`
        Двумерный массив значений:
        - каждая строка соответствует строке UNIT;
        - каждая колонка соответствует элементу из `column_names`.
    """

    column_names: tuple[str, ...]
    matrix: list[list[object]]


class UnitCompetitorsService:
    """Сервис обновления конкурентных колонок в UNIT.

    Здесь собрана вся бизнес-логика:
    - чтение UNIT;
    - чтение ClickHouse;
    - нормализация и дедупликация;
    - построение матриц записи;
    - проверка инвариантов;
    - запись в Google Sheets.
    """

    def __init__(
        self,
        clickhouse: ClickHouseDatabase | None = None,
        unit_economics: UnitEconomics | None = None,
    ) -> None:
        self.clickhouse = clickhouse or ClickHouseDatabase()
        self.unit_economics = unit_economics or UnitEconomics()

    def build_competitors_price_update_plans(
        self,
    ) -> tuple[ColumnUpdatePlan, ColumnUpdatePlan, ColumnUpdatePlan]:
        """Готовит все планы записи для конкурентных колонок.

        Возвращает три независимых плана:
        1. `Конкурент 1/2/3`
        2. `Цена 1/2/3 конкурента`
        3. `Наша цена после СПП`

        Мы строим именно планы, а не сразу пишем в Google Sheets, потому что
        это позволяет:
        - делать dry-run;
        - логировать промежуточные данные;
        - проверять форму матриц до фактической записи.
        """

        unit_df = self._prepare_unit_dataframe_for_prices()
        competitors_df = self._prepare_competitors_positions_dataframe()
        our_prices_df = self._prepare_our_prices_dataframe()

        # Для каждого фиксированного конкурентного слота строим отдельный
        # словарь. Это повторяет старую бизнес-логику:
        # "Первый конкурент" -> колонка "Конкурент 1", и так далее.
        competitor_article_maps = self._build_lookup_by_competitor(
            dataframe=competitors_df,
            value_column=COMPETITOR_ARTICLE_COLUMN,
        )
        competitor_price_maps = self._build_lookup_by_competitor(
            dataframe=competitors_df,
            value_column=COMPETITOR_PRICE_COLUMN,
        )
        our_price_map = self._build_lookup_by_article(
            dataframe=our_prices_df,
            key_column=OUR_ARTICLE_COLUMN,
            value_column=COMPETITOR_PRICE_COLUMN,
        )

        # Матрицы собираются строго в порядке строк UNIT. Здесь нельзя
        # использовать merge/sort/reindex, если это меняет порядок или длину.
        competitor_matrix = self._build_matrix_for_unit_rows(
            unit_df=unit_df,
            lookup_maps=competitor_article_maps,
            key_column=UNIT_WILD_COLUMN,
            default_value=NO_COMPETITOR_VALUE,
        )
        competitor_price_matrix = self._build_matrix_for_unit_rows(
            unit_df=unit_df,
            lookup_maps=competitor_price_maps,
            key_column=UNIT_WILD_COLUMN,
            default_value=0,
        )
        our_price_matrix = self._build_single_column_matrix_for_unit_rows(
            unit_df=unit_df,
            lookup_map=our_price_map,
            key_column=UNIT_ARTICLE_COLUMN,
            default_value=0,
        )

        # Явно страхуемся от ситуаций, когда матрица "съехала" по длине или
        # ширине. В таком случае запись в таблицу лучше запретить полностью.
        self._validate_matrix_shape(
            matrix=competitor_matrix,
            expected_row_count=len(unit_df),
            expected_column_count=len(COMPETITOR_TARGET_COLUMNS),
            matrix_name="competitor articles",
        )
        self._validate_matrix_shape(
            matrix=competitor_price_matrix,
            expected_row_count=len(unit_df),
            expected_column_count=len(COMPETITOR_PRICE_TARGET_COLUMNS),
            matrix_name="competitor prices",
        )
        self._validate_matrix_shape(
            matrix=our_price_matrix,
            expected_row_count=len(unit_df),
            expected_column_count=1,
            matrix_name="our prices",
        )

        logger.info(
            "Competitors price plans prepared: unit_rows=%s, db_rows=%s, "
            "our_price_rows=%s, unique_wild=%s",
            len(unit_df),
            len(competitors_df),
            len(our_prices_df),
            competitors_df[UNIT_WILD_COLUMN].nunique(dropna=True),
        )

        return (
            ColumnUpdatePlan(
                column_names=COMPETITOR_TARGET_COLUMNS,
                matrix=competitor_matrix,
            ),
            ColumnUpdatePlan(
                column_names=COMPETITOR_PRICE_TARGET_COLUMNS,
                matrix=competitor_price_matrix,
            ),
            ColumnUpdatePlan(
                column_names=(OUR_PRICE_TARGET_COLUMN,),
                matrix=our_price_matrix,
            ),
        )

    def update_competitors_prices(
        self,
        *,
        dry_run: bool = False,
    ) -> tuple[ColumnUpdatePlan, ColumnUpdatePlan, ColumnUpdatePlan]:
        """Выполняет обновление конкурентных колонок.

        В обычном режиме:
        - строит планы записи;
        - записывает их в лист UNIT.

        В `dry_run=True`:
        - строит те же планы;
        - проходит все проверки;
        - вместо записи только логирует, что было бы обновлено.
        """

        plans = self.build_competitors_price_update_plans()
        unit_sheet = self.unit_economics.google_connect.sheet_title

        for plan in plans:
            self._write_columns(
                worksheet=unit_sheet,
                plan=plan,
                dry_run=dry_run,
            )

        return plans

    def _prepare_unit_dataframe_for_prices(self) -> pd.DataFrame:
        """Готовит минимальный DataFrame UNIT для дальнейших lookup-операций.

        Оставляем только:
        - `Артикул`
        - `wild`

        Оба поля приводим к строкам, потому что дальше они используются как
        ключи словарей, и смешение типов легко даёт промахи по lookup.
        """

        unit_df = self.unit_economics.get_unit_dataframe(
            required_columns=[UNIT_ARTICLE_COLUMN, UNIT_WILD_COLUMN]
        )
        result_df = unit_df[[UNIT_ARTICLE_COLUMN, UNIT_WILD_COLUMN]].copy()
        result_df[UNIT_WILD_COLUMN] = self._normalize_series(
            result_df[UNIT_WILD_COLUMN]
        )
        result_df[UNIT_ARTICLE_COLUMN] = self._normalize_series(
            result_df[UNIT_ARTICLE_COLUMN]
        )
        return result_df

    def _prepare_competitors_positions_dataframe(self) -> pd.DataFrame:
        """Готовит ClickHouse-данные для конкурентных слотов.

        После чтения:
        - проверяем обязательные колонки;
        - нормализуем `wild` и имя конкурента;
        - приводим числовые колонки к nullable-int виду;
        - логируем число дублей по (`Конкурент`, `wild`).

        Дубли как входное состояние допустимы: позже они схлопываются
        через `drop_duplicates(..., keep="last")`.
        """

        dataframe = self.clickhouse.read_sql_to_dataframe(
            query_competitors_positions
        )
        self._validate_required_columns(
            dataframe,
            (
                OUR_ARTICLE_COLUMN,
                COMPETITOR_ARTICLE_COLUMN,
                COMPETITOR_PRICE_COLUMN,
                UNIT_WILD_COLUMN,
                COMPETITOR_POSITION_COLUMN,
                COMPETITOR_NAME_COLUMN,
            ),
            source_name="competitors positions query",
        )
        dataframe = dataframe.copy()
        dataframe[UNIT_WILD_COLUMN] = self._normalize_series(
            dataframe[UNIT_WILD_COLUMN]
        )
        dataframe[COMPETITOR_NAME_COLUMN] = self._normalize_series(
            dataframe[COMPETITOR_NAME_COLUMN]
        )
        dataframe[COMPETITOR_ARTICLE_COLUMN] = (
            self._coerce_numeric_series_to_nullable_int(
                dataframe[COMPETITOR_ARTICLE_COLUMN]
            )
        )
        dataframe[COMPETITOR_PRICE_COLUMN] = (
            self._coerce_numeric_series_to_nullable_int(
                dataframe[COMPETITOR_PRICE_COLUMN]
            )
        )
        duplicate_rows = int(
            dataframe.duplicated(
                subset=[COMPETITOR_NAME_COLUMN, UNIT_WILD_COLUMN]
            ).sum()
        )
        if duplicate_rows:
            logger.warning(
                "Competitors positions contain duplicate keys: duplicates=%s",
                duplicate_rows,
            )
        return dataframe

    def _prepare_our_prices_dataframe(self) -> pd.DataFrame:
        """Готовит набор данных для колонки `Наша цена после СПП`.

        Из результата ClickHouse оставляем только строки, относящиеся к нашему
        артикулу. Затем на их основе строится словарь
        `наш артикул -> наша цена`.
        """

        dataframe = self.clickhouse.read_sql_to_dataframe(query_our_article_prices)
        self._validate_required_columns(
            dataframe,
            (
                OUR_ARTICLE_COLUMN,
                COMPETITOR_PRICE_COLUMN,
                COMPETITOR_NAME_COLUMN,
            ),
            source_name="our prices query",
        )
        dataframe = dataframe.copy()
        dataframe[OUR_ARTICLE_COLUMN] = self._normalize_series(
            dataframe[OUR_ARTICLE_COLUMN]
        )
        dataframe[COMPETITOR_PRICE_COLUMN] = (
            self._coerce_numeric_series_to_nullable_int(
                dataframe[COMPETITOR_PRICE_COLUMN]
            )
        )
        dataframe[COMPETITOR_NAME_COLUMN] = self._normalize_series(
            dataframe[COMPETITOR_NAME_COLUMN]
        )
        dataframe = dataframe[
            dataframe[COMPETITOR_NAME_COLUMN] == OUR_ARTICLE_COLUMN
        ].copy()
        return dataframe

    @staticmethod
    def _normalize_series(series: pd.Series) -> pd.Series:
        """Нормализует серию до строковых ключей без `NaN` и лишних пробелов."""

        return series.fillna("").astype(str).str.strip()

    @staticmethod
    def _coerce_numeric_series_to_nullable_int(series: pd.Series) -> pd.Series:
        """Приводит числовую серию к nullable-int, сохраняя пустые значения."""

        numeric_series = pd.to_numeric(series, errors="coerce")
        return numeric_series.where(
            numeric_series.isna(),
            numeric_series.astype("Int64"),
        )

    @staticmethod
    def _validate_required_columns(
        dataframe: pd.DataFrame,
        required_columns: tuple[str, ...] | list[str],
        source_name: str,
    ) -> None:
        """Проверяет, что источник содержит все обязательные колонки."""

        missing_columns = [
            column for column in required_columns if column not in dataframe.columns
        ]
        if missing_columns:
            raise ValueError(
                f"В источнике '{source_name}' отсутствуют обязательные колонки: "
                f"{', '.join(missing_columns)}"
            )

    def _build_lookup_by_competitor(
        self,
        *,
        dataframe: pd.DataFrame,
        value_column: str,
    ) -> list[dict[str, object]]:
        """Строит lookup-словари по фиксированным конкурентным слотам.

        На выходе получается список словарей:
        - для `Первый конкурент`;
        - для `Второй конкурент`;
        - для `Третий конкурент`.

        Каждый словарь имеет вид `wild -> value`.
        Если по одному `wild` есть несколько записей, берём последнюю
        через `keep="last"`, что соответствует старой логике.
        """

        lookup_maps: list[dict[str, object]] = []
        for competitor_name in FIXED_COMPETITOR_NAMES:
            competitor_df = dataframe[
                dataframe[COMPETITOR_NAME_COLUMN] == competitor_name
            ].copy()
            competitor_df = competitor_df[
                competitor_df[UNIT_WILD_COLUMN] != ""
            ].copy()
            competitor_df = competitor_df.drop_duplicates(
                subset=[UNIT_WILD_COLUMN],
                keep="last",
            )
            lookup_maps.append(
                dict(
                    zip(
                        competitor_df[UNIT_WILD_COLUMN],
                        competitor_df[value_column],
                    )
                )
            )
        return lookup_maps

    def _build_lookup_by_article(
        self,
        *,
        dataframe: pd.DataFrame,
        key_column: str,
        value_column: str,
    ) -> dict[str, object]:
        """Строит словарь `ключ -> значение` для наших цен."""

        filtered_df = dataframe[dataframe[key_column] != ""].copy()
        filtered_df = filtered_df.drop_duplicates(
            subset=[key_column],
            keep="first",
        )
        return dict(zip(filtered_df[key_column], filtered_df[value_column]))

    def _build_matrix_for_unit_rows(
        self,
        *,
        unit_df: pd.DataFrame,
        lookup_maps: list[dict[str, object]],
        key_column: str,
        default_value: object,
    ) -> list[list[object]]:
        """Собирает двумерную матрицу значений строго по строкам UNIT.

        Поведение:
        - если ключ в строке пустой, возвращаем пустые значения;
        - если lookup не нашёл значение, подставляем `default_value`;
        - если lookup вернул `NaN`, тоже подставляем `default_value`.
        """

        matrix: list[list[object]] = []
        for key in unit_df[key_column].tolist():
            if key == "":
                matrix.append(["" for _ in lookup_maps])
                continue

            matrix.append(
                [
                    self._coerce_lookup_value(
                        lookup_map.get(key, default_value),
                        default_value=default_value,
                    )
                    for lookup_map in lookup_maps
                ]
            )
        return matrix

    def _build_single_column_matrix_for_unit_rows(
        self,
        *,
        unit_df: pd.DataFrame,
        lookup_map: dict[str, object],
        key_column: str,
        default_value: object,
    ) -> list[list[object]]:
        """Собирает одно-колоночную матрицу по строкам UNIT.

        Используется для записи нашей цены, где на каждую строку приходится
        ровно одно итоговое значение.
        """

        matrix: list[list[object]] = []
        for wild_value, key in zip(
            unit_df[UNIT_WILD_COLUMN].tolist(),
            unit_df[key_column].tolist(),
        ):
            if wild_value == "":
                matrix.append([""])
                continue

            matrix.append(
                [
                    self._coerce_lookup_value(
                        lookup_map.get(key, default_value),
                        default_value=default_value,
                    )
                ]
            )
        return matrix

    @staticmethod
    def _coerce_lookup_value(value: object, *, default_value: object) -> object:
        """Преобразует значение lookup к безопасному виду для Google Sheets.

        Основная задача метода — убрать `NaN`, потому что Google Sheets API
        не принимает такие значения в JSON payload.
        """

        if pd.isna(value):
            return default_value
        return value

    @staticmethod
    def _validate_matrix_shape(
        *,
        matrix: list[list[object]],
        expected_row_count: int,
        expected_column_count: int,
        matrix_name: str,
    ) -> None:
        """Проверяет форму матрицы перед записью в Google Sheets."""

        if len(matrix) != expected_row_count:
            raise ValueError(
                f"Матрица '{matrix_name}' имеет неверное число строк: "
                f"{len(matrix)} вместо {expected_row_count}."
            )

        invalid_rows = [
            index
            for index, row in enumerate(matrix, start=1)
            if len(row) != expected_column_count
        ]
        if invalid_rows:
            raise ValueError(
                f"Матрица '{matrix_name}' имеет неверное число колонок "
                f"в строках: {invalid_rows[:5]}"
            )

    def _write_columns(
        self,
        *,
        worksheet,
        plan: ColumnUpdatePlan,
        dry_run: bool,
    ) -> None:
        """Записывает одну группу колонок в лист UNIT.

        Метод обновляет только нужные диапазоны. Это принципиально важно:
        мы не должны перезаписывать весь лист ради нескольких колонок.
        """

        header_values = worksheet.row_values(GOOGLE_HEADER_ROW_INDEX)
        if not header_values:
            raise ValueError("В целевом листе отсутствует строка заголовков.")

        row_count = len(plan.matrix)
        if row_count == 0:
            logger.info(
                "Column update skipped: sheet=%s, columns=%s, rows=0",
                worksheet.title,
                plan.column_names,
            )
            return

        self._validate_matrix_shape(
            matrix=plan.matrix,
            expected_row_count=row_count,
            expected_column_count=len(plan.column_names),
            matrix_name=", ".join(plan.column_names),
        )

        for column_offset, column_name in enumerate(plan.column_names):
            if column_name not in header_values:
                raise ValueError(
                    f"В листе отсутствует колонка '{column_name}'."
                )

            column_index = header_values.index(column_name) + 1
            start_cell = rowcol_to_a1(GOOGLE_DATA_START_ROW, column_index)
            end_cell = rowcol_to_a1(
                GOOGLE_DATA_START_ROW + row_count - 1,
                column_index,
            )
            range_label = f"{start_cell}:{end_cell}"
            values = [[row[column_offset]] for row in plan.matrix]

            if dry_run:
                logger.info(
                    "Dry-run column update: sheet=%s, column=%s, range=%s, rows=%s",
                    worksheet.title,
                    column_name,
                    range_label,
                    row_count,
                )
                continue

            worksheet.update(
                range_label,
                values,
                value_input_option="USER_ENTERED",
            )
            logger.info(
                "Column updated: sheet=%s, column=%s, range=%s, rows=%s",
                worksheet.title,
                column_name,
                range_label,
                row_count,
            )


def update_competitors_prices(
    *,
    dry_run: bool = False,
) -> tuple[ColumnUpdatePlan, ColumnUpdatePlan, ColumnUpdatePlan]:
    """CLI-entrypoint для задачи `update_competitors_prices`."""

    service = UnitCompetitorsService()
    return service.update_competitors_prices(dry_run=dry_run)
