from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import bindparam, text

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.annual_procurement_plan.annual_procurement_plan import AnnualProcurementPlan
from src_oop.jobs.sales_plan.config import (
    ACTIVE_WILD_STATUSES,
    QUARTER_PLAN_3Q_2026_UNITS_COLUMN,
    QUARTER_PLAN_PRICE_COLUMN,
    QUARTER_PLAN_SUBJECT_COLUMN,
    QUARTER_PLAN_WILD_COLUMN,
    QUARTER_PLAN_WILD_STATUS_COLUMN,
    SALES_PLAN_ACCOUNTING_CATEGORY_KEY_COLUMNS,
    SALES_PLAN_ACCOUNTING_CATEGORY_SCHEMA,
    SALES_PLAN_ACCOUNTING_CATEGORY_TABLE,
    SALES_PLAN_MANAGER_REFERENCE_KEY_COLUMNS,
    SALES_PLAN_MANAGER_REFERENCE_SCHEMA,
    SALES_PLAN_MANAGER_REFERENCE_TABLE,
    SALES_WILD_STATUS_DAILY_KEY_COLUMNS,
    SALES_WILD_STATUS_DAILY_SCHEMA,
    SALES_WILD_STATUS_DAILY_TABLE,
    SOURCE_MANAGER_COLUMN,
    SOURCE_SUBJECT_COLUMN,
    SOURCE_WILD_COLUMN,
    sales_plan_manager_reference_sheet,
)

logger = logging.getLogger(__name__)

MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")


@dataclass(slots=True)
class SalesPlanManagerReferenceSyncResult:
    """Итог чтения и записи snapshot-справочника менеджеров для плана продаж."""

    source_rows: int
    rows_after_cleanup: int
    duplicate_rows: int
    written_rows: int
    snapshot_date: date


@dataclass(slots=True)
class SalesPlanAccountingCategorySyncResult:
    """Итог подготовки и записи справочника учетной категории по `wild`."""

    source_rows: int
    rows_after_cleanup: int
    inserted_rows: int
    updated_rows: int
    deleted_rows: int


@dataclass(slots=True)
class SalesWildStatusDailySyncResult:
    """Итог чтения и записи дневного статуса `wild` для плана продаж."""

    source_rows: int
    rows_after_cleanup: int
    duplicate_rows: int
    written_rows: int
    snapshot_date: date


class SalesPlanManagerReferenceRepository:
    """Подготавливает и сохраняет в БД снимок листа `Справочник Категория-Менеджер`.

    Бизнес-сценарий:
    справочник менеджеров нужен как опорный слой для будущего плана продаж по
    `wild`. Первая загрузка не решает окончательно спорный учет категорий, а
    сохраняет ежедневный снимок источника, чтобы не терять историю назначений и
    состав категорий в том виде, в котором их видит бизнес в ПУ.
    """

    def __init__(self) -> None:
        """Инициализирует подключение к исходному листу справочника менеджеров.

        Бизнес-сценарий:
        job должна читать один конкретный управленческий лист из ПУ, поэтому
        конфигурация источника фиксируется централизованно и переиспользуется
        во всех шагах загрузки.
        """

        self._connector = GoogleTabs(
            table_title=sales_plan_manager_reference_sheet["title"],
            sheet_title=sales_plan_manager_reference_sheet["sheet_title"],
        )

    def sync_snapshot(self, snapshot_date: date | None = None) -> SalesPlanManagerReferenceSyncResult:
        """Читает лист ПУ и сохраняет его как исторический снимок в PostgreSQL.

        Бизнес-сценарий:
        на старте проекта важно накапливать историю справочника менеджеров по
        датам. Даже если позже изменится логика учета категорий, снимки листа
        помогут понять, какой `wild` и в каком предмете был закреплен за
        менеджером в конкретный день без примеси лишних аналитических полей.
        """

        effective_snapshot_date = snapshot_date or datetime.now(MOSCOW_TIMEZONE).date()
        source_dataframe = self._read_source_dataframe()
        prepared_dataframe, duplicate_rows = self._prepare_snapshot_dataframe(
            dataframe=source_dataframe,
            snapshot_date=effective_snapshot_date,
        )

        if prepared_dataframe.empty:
            logger.warning(
                "Синхронизация справочника категорий и менеджеров пропущена: после очистки не осталось строк | snapshot_date=%s",
                effective_snapshot_date,
            )
            return SalesPlanManagerReferenceSyncResult(
                source_rows=len(source_dataframe.index),
                rows_after_cleanup=0,
                duplicate_rows=duplicate_rows,
                written_rows=0,
                snapshot_date=effective_snapshot_date,
            )

        Database.sync_data_to_postgres(
            table_name=SALES_PLAN_MANAGER_REFERENCE_TABLE,
            data=prepared_dataframe,
            schema_definition=SALES_PLAN_MANAGER_REFERENCE_SCHEMA,
            unique_keys=SALES_PLAN_MANAGER_REFERENCE_KEY_COLUMNS,
        )
        logger.info(
            "Снимок справочника категорий и менеджеров сохранен в PostgreSQL | table=%s | source_rows=%s | written_rows=%s | duplicate_rows=%s | snapshot_date=%s",
            SALES_PLAN_MANAGER_REFERENCE_TABLE,
            len(source_dataframe.index),
            len(prepared_dataframe.index),
            duplicate_rows,
            effective_snapshot_date,
        )
        return SalesPlanManagerReferenceSyncResult(
            source_rows=len(source_dataframe.index),
            rows_after_cleanup=len(prepared_dataframe.index),
            duplicate_rows=duplicate_rows,
            written_rows=len(prepared_dataframe.index),
            snapshot_date=effective_snapshot_date,
        )

    def _read_source_dataframe(self) -> pd.DataFrame:
        """Читает текущий лист ПУ в DataFrame с реальными заголовками источника.

        Бизнес-сценарий:
        дальнейшая логика плана продаж опирается на живой справочник из ПУ, и
        важно сохранить фактическую структуру листа без ручного копирования
        заголовков в коде. Так job быстрее выявляет изменение шапки источника.
        """

        values = self._connector.sheet_title.get_all_values()
        if not values:
            raise ValueError("Лист справочника категорий и менеджеров пуст и не может быть загружен.")

        headers = values[0]
        rows = values[1:]
        dataframe = pd.DataFrame(rows, columns=headers)
        logger.info(
            "Лист справочника категорий и менеджеров прочитан из Google Sheets | rows=%s | columns=%s",
            len(dataframe.index),
            headers,
        )
        return dataframe

    def _prepare_snapshot_dataframe(
        self,
        dataframe: pd.DataFrame,
        snapshot_date: date,
    ) -> tuple[pd.DataFrame, int]:
        """Нормализует снимок листа перед сохранением в PostgreSQL.

        Бизнес-сценарий:
        для исторического справочника нужно сохранить `wild`, предмет и
        менеджера в стабильных колонках БД. На этом шаге строки очищаются от
        пустых ключей, а полю `Артикул` присваивается роль `wild`, потому что
        именно так текущий источник идентифицирует товар для плана продаж.
        """

        self._validate_required_columns(dataframe)

        prepared_dataframe = pd.DataFrame(
            {
                "subject_name": dataframe[SOURCE_SUBJECT_COLUMN].map(self._normalize_string),
                "manager_name": dataframe[SOURCE_MANAGER_COLUMN].map(self._normalize_string),
            }
        )

        prepared_dataframe = prepared_dataframe.loc[
            (prepared_dataframe["subject_name"] != "")
            & (prepared_dataframe["manager_name"] != "")
        ].copy()
        duplicate_rows = int(
            prepared_dataframe.duplicated(
                subset=["subject_name", "manager_name"],
                keep="last",
            ).sum()
        )
        if duplicate_rows:
            logger.warning(
                "В источнике найдены повторяющиеся строки по ключу предмет + менеджер, будут сохранены последние значения | duplicate_rows=%s | snapshot_date=%s",
                duplicate_rows,
                snapshot_date,
            )

        prepared_dataframe = prepared_dataframe.drop_duplicates(
            subset=["subject_name", "manager_name"],
            keep="last",
        ).copy()
        prepared_dataframe["snapshot_date"] = snapshot_date
        prepared_dataframe["loaded_at"] = datetime.now(MOSCOW_TIMEZONE).replace(tzinfo=None)

        ordered_columns = list(SALES_PLAN_MANAGER_REFERENCE_SCHEMA.keys())
        database_dataframe = prepared_dataframe.loc[:, ordered_columns].copy()
        return database_dataframe.astype(object).where(pd.notna(database_dataframe), None), duplicate_rows

    @staticmethod
    def _validate_required_columns(dataframe: pd.DataFrame) -> None:
        """Проверяет, что лист ПУ все еще содержит обязательные бизнес-колонки.

        Бизнес-сценарий:
        если в справочнике переименовали или удалили колонку `Предмет`,
        `Менеджер` или `Артикул`, загрузка должна остановиться
        явно. Иначе в БД может попасть искаженный снимок, который потом сложно
        отличить от корректных исторических данных.
        """

        required_columns = {
            SOURCE_SUBJECT_COLUMN,
            SOURCE_MANAGER_COLUMN,
            SOURCE_WILD_COLUMN,
        }
        missing_columns = sorted(required_columns - set(dataframe.columns))
        if missing_columns:
            raise ValueError(
                "В листе справочника категорий и менеджеров отсутствуют обязательные колонки: "
                f"{missing_columns}"
            )

    @staticmethod
    def _normalize_string(value: object) -> str:
        """Приводит текстовые поля справочника к стабильному строковому виду.

        Бизнес-сценарий:
        в Google Sheets поля могут содержать пробелы, `NaN` и технические
        пустоты. Для последующей группировки по предмету и менеджеру важно
        сохранить единый очищенный формат значений.
        """

        if value is None or pd.isna(value):
            return ""
        return str(value).strip()

    @staticmethod
    def _normalize_wild(value: object) -> str:
        """Нормализует `wild` из листа ПУ без потери числового идентификатора.

        Бизнес-сценарий:
        в листе ПУ колонка `Артикул` визуально хранится как число, но в БД
        `wild` удобнее держать строкой. Нормализация убирает артефакты вида
        `.0`, чтобы ключ товара не менялся из-за формата ячейки.
        """

        normalized_value = SalesPlanManagerReferenceRepository._normalize_string(value)
        if not normalized_value:
            return ""

        try:
            numeric_value = float(normalized_value.replace(" ", "").replace(",", "."))
        except ValueError:
            return normalized_value

        if numeric_value.is_integer():
            return str(int(numeric_value))
        return normalized_value


class SalesPlanAccountingCategoryRepository:
    """Подготавливает и сохраняет текущий справочник учетной категории по `wild`.

    Бизнес-сценарий:
    для итогового плана продаж нужен единый справочник, где каждый `wild`
    входит только в одну учетную категорию. Историческое накопление здесь не
    требуется: таблица должна отражать актуальное состояние вкладки
    `Поквартально` и обновляться целиком раз в сутки.
    """

    def __init__(self) -> None:
        """Инициализирует источник данных из вкладки `Поквартально`.

        Бизнес-сценарий:
        новая витрина учетной категории строится на основе уже существующей
        таблицы `Годовой план закупа 2026`, поэтому repository переиспользует
        текущий класс чтения квартального плана без изменения архитектуры.
        """

        self._annual_procurement_plan = AnnualProcurementPlan()

    def sync_reference(self) -> SalesPlanAccountingCategorySyncResult:
        """Синхронизирует текущий справочник учетной категории по `wild` в PostgreSQL.

        Бизнес-сценарий:
        каждый `wild` должен существовать в таблице только один раз и только с
        одной учетной категорией. При ежедневном обновлении справочник должен
        совпадать с текущим содержимым `Поквартально`, поэтому отсутствующие в
        источнике `wild` удаляются из БД, а `created_at` для уже известных
        строк сохраняется.
        """

        source_dataframe = self._annual_procurement_plan.get_quarterly_plan_data()
        prepared_dataframe = self._prepare_reference_dataframe(source_dataframe)

        if prepared_dataframe.empty:
            raise ValueError(
                "Справочник учетной категории не может быть обновлен: после очистки во вкладке Поквартально не осталось строк с wild и предметом."
            )

        existing_reference = self._read_existing_reference()
        payload_dataframe, inserted_rows, updated_rows = self._build_database_payload(
            prepared_dataframe=prepared_dataframe,
            existing_reference=existing_reference,
        )
        Database.sync_data_to_postgres(
            table_name=SALES_PLAN_ACCOUNTING_CATEGORY_TABLE,
            data=payload_dataframe,
            schema_definition=SALES_PLAN_ACCOUNTING_CATEGORY_SCHEMA,
            unique_keys=SALES_PLAN_ACCOUNTING_CATEGORY_KEY_COLUMNS,
        )
        deleted_rows = self._delete_missing_wilds(
            actual_wilds=payload_dataframe["wild"].astype(str).tolist()
        )
        logger.info(
            "Справочник учетной категории синхронизирован с PostgreSQL | table=%s | source_rows=%s | rows_after_cleanup=%s | inserted_rows=%s | updated_rows=%s | deleted_rows=%s",
            SALES_PLAN_ACCOUNTING_CATEGORY_TABLE,
            len(source_dataframe.index),
            len(prepared_dataframe.index),
            inserted_rows,
            updated_rows,
            deleted_rows,
        )
        return SalesPlanAccountingCategorySyncResult(
            source_rows=len(source_dataframe.index),
            rows_after_cleanup=len(prepared_dataframe.index),
            inserted_rows=inserted_rows,
            updated_rows=updated_rows,
            deleted_rows=deleted_rows,
        )

    def _prepare_reference_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Готовит пары `wild -> предмет` из вкладки `Поквартально` к записи в БД.

        Бизнес-сценарий:
        будущий план продаж будет опираться на единственную учетную категорию
        для каждого `wild`. Поэтому функция очищает пустые строки, приводит
        ключи к стабильному формату и останавливает сценарий, если один и тот
        же `wild` встречен в нескольких разных категориях.
        """

        self._validate_quarterly_required_columns(dataframe)

        prepared_dataframe = pd.DataFrame(
            {
                "wild": dataframe[QUARTER_PLAN_WILD_COLUMN].map(
                    SalesPlanManagerReferenceRepository._normalize_wild
                ),
                "subject_name": dataframe[QUARTER_PLAN_SUBJECT_COLUMN].map(
                    SalesPlanManagerReferenceRepository._normalize_string
                ),
                "quarter_3_units_2026": dataframe[QUARTER_PLAN_3Q_2026_UNITS_COLUMN].map(
                    self._normalize_quarter_units_value
                ),
                "plan_price": dataframe[QUARTER_PLAN_PRICE_COLUMN].map(
                    self._normalize_plan_price_value
                ),
            }
        )
        prepared_dataframe = prepared_dataframe.loc[
            (prepared_dataframe["wild"] != "")
            & (prepared_dataframe["subject_name"] != "")
        ].copy()
        prepared_dataframe = prepared_dataframe.drop_duplicates().copy()

        self._raise_if_wild_has_multiple_subjects(prepared_dataframe)
        return prepared_dataframe.drop_duplicates(subset=["wild"], keep="last").copy()

    @staticmethod
    def _validate_quarterly_required_columns(dataframe: pd.DataFrame) -> None:
        """Проверяет наличие обязательных колонок `wild` и `предмет` в квартальном плане.

        Бизнес-сценарий:
        если во вкладке `Поквартально` изменили шапку, загрузка справочника
        должна остановиться сразу. Это защищает правило единственной учетной
        категории и не даёт записать в БД искаженный маппинг.
        """

        required_columns = {
            QUARTER_PLAN_WILD_COLUMN,
            QUARTER_PLAN_SUBJECT_COLUMN,
            QUARTER_PLAN_3Q_2026_UNITS_COLUMN,
            QUARTER_PLAN_PRICE_COLUMN,
        }
        missing_columns = sorted(required_columns - set(dataframe.columns))
        if missing_columns:
            raise ValueError(
                "Во вкладке Поквартально отсутствуют обязательные колонки для справочника учетной категории: "
                f"{missing_columns}"
            )

    @staticmethod
    def _normalize_quarter_units_value(value: object) -> float | None:
        """Готовит значение `3 квартал, шт 2026` для справочника учетной категории.

        Бизнес-сценарий:
        в новом справочнике нужно хранить не исходное квартальное количество, а
        среднемесячное значение для 3 квартала 2026. Поэтому непустое число
        делится на 3 и округляется до 2 знаков после запятой, а пустая ячейка
        сохраняется как `NULL`, чтобы не подменять отсутствие плана нулём.
        """

        normalized_value = SalesPlanManagerReferenceRepository._normalize_string(value)
        if not normalized_value:
            return None

        numeric_value = float(
            normalized_value.replace(" ", "").replace("\xa0", "").replace(",", ".")
        )
        return round(numeric_value / 3, 2)

    @staticmethod
    def _normalize_plan_price_value(value: object) -> float | None:
        """Нормализует плановую цену продажи для справочника учетной категории.

        Бизнес-сценарий:
        цена `цена продажная плановая` нужна в том же справочнике, где уже
        живет `wild`, чтобы дальше можно было считать план продаж без
        дополнительных соединений по Google Sheets. Пустое значение остается
        `NULL`, а число приводится к формату с 2 знаками после запятой.
        """

        normalized_value = SalesPlanManagerReferenceRepository._normalize_string(value)
        if not normalized_value:
            return None

        numeric_value = float(
            normalized_value.replace(" ", "").replace("\xa0", "").replace(",", ".")
        )
        return round(numeric_value, 2)

    @staticmethod
    def _raise_if_wild_has_multiple_subjects(dataframe: pd.DataFrame) -> None:
        """Проверяет обязательное бизнес-правило `один wild = один предмет`.

        Бизнес-сценарий:
        учетная категория нужна именно для устранения дублей плана по
        категориям. Если один `wild` приходит сразу с несколькими предметами,
        задача должна завершиться с ошибкой и подсветить конфликт, а не
        выбирать категорию молча.
        """

        if dataframe.empty:
            return

        grouped = dataframe.groupby("wild")["subject_name"].nunique()
        conflicting_wilds = grouped[grouped > 1].index.tolist()
        if not conflicting_wilds:
            return

        conflicts_preview = (
            dataframe.loc[dataframe["wild"].isin(conflicting_wilds), ["wild", "subject_name"]]
            .sort_values(["wild", "subject_name"])
            .head(10)
            .to_dict(orient="records")
        )
        raise ValueError(
            "Во вкладке Поквартально найден конфликт учетной категории: один wild связан с несколькими предметами. "
            f"Примеры конфликтов: {conflicts_preview}"
        )

    def _read_existing_reference(self) -> pd.DataFrame:
        """Читает текущее состояние справочника учетной категории из PostgreSQL.

        Бизнес-сценарий:
        при ежедневной актуализации нужно сохранить первоначальный `created_at`
        для уже известных `wild`. Поэтому перед upsert repository читает
        текущие строки и использует их как источник неизменяемой даты создания.
        """

        query = """
            SELECT
                wild,
                subject_name,
                created_at
            FROM sales_plan_accounting_category_reference
        """
        try:
            dataframe = Database.read_sql_to_dataframe(query)
        except Exception as error:
            error_message = str(error).lower()
            if "does not exist" in error_message or "undefinedtable" in error_message:
                logger.info(
                    "Таблица справочника учетной категории пока отсутствует в PostgreSQL, будет создана при первой записи."
                )
                return pd.DataFrame(columns=["wild", "subject_name", "created_at"])
            raise

        if dataframe.empty:
            return pd.DataFrame(columns=["wild", "subject_name", "created_at"])

        dataframe["wild"] = dataframe["wild"].map(
            SalesPlanManagerReferenceRepository._normalize_string
        )
        return dataframe

    def _build_database_payload(
        self,
        prepared_dataframe: pd.DataFrame,
        existing_reference: pd.DataFrame,
    ) -> tuple[pd.DataFrame, int, int]:
        """Формирует payload для upsert, сохраняя исторический `created_at`.

        Бизнес-сценарий:
        таблица учетной категории не накапливает ежедневные снимки, но должна
        помнить дату первого появления `wild`. Поэтому новые строки получают
        текущий `created_at`, а для уже существующих записей старая дата
        сохраняется без изменений.
        """

        existing_created_at_map = {}
        existing_subject_map = {}
        if not existing_reference.empty:
            existing_created_at_map = dict(
                zip(existing_reference["wild"], existing_reference["created_at"], strict=False)
            )
            existing_subject_map = dict(
                zip(existing_reference["wild"], existing_reference["subject_name"], strict=False)
            )

        payload_dataframe = prepared_dataframe.copy()
        current_timestamp = datetime.now(MOSCOW_TIMEZONE).replace(tzinfo=None)
        payload_dataframe["created_at"] = payload_dataframe["wild"].map(existing_created_at_map)
        payload_dataframe["created_at"] = payload_dataframe["created_at"].where(
            payload_dataframe["created_at"].notna(),
            current_timestamp,
        )

        existing_wild_mask = payload_dataframe["wild"].isin(existing_created_at_map)
        inserted_rows = int((~existing_wild_mask).sum())
        updated_mask = (
            payload_dataframe["wild"].map(existing_subject_map).fillna("")
            != payload_dataframe["subject_name"]
        )
        updated_rows = int((existing_wild_mask & updated_mask).sum())

        ordered_columns = list(SALES_PLAN_ACCOUNTING_CATEGORY_SCHEMA.keys())
        database_dataframe = payload_dataframe.loc[:, ordered_columns].copy()
        return (
            database_dataframe.astype(object).where(pd.notna(database_dataframe), None),
            inserted_rows,
            updated_rows,
        )

    def _delete_missing_wilds(self, actual_wilds: list[str]) -> int:
        """Удаляет из БД `wild`, которых больше нет во вкладке `Поквартально`.

        Бизнес-сценарий:
        справочник учетной категории должен отражать только актуальное
        состояние источника. Если `wild` удален из квартального плана, его
        нужно убрать и из БД, чтобы дальше не дублировать или не искажать
        плановые расчеты.
        """

        if not actual_wilds:
            raise ValueError(
                "Удаление отсутствующих wild из справочника учетной категории отменено: список актуальных ключей пуст."
            )

        delete_query = text(
            """
            DELETE FROM sales_plan_accounting_category_reference
            WHERE wild NOT IN :actual_wilds
            """
        ).bindparams(bindparam("actual_wilds", expanding=True))
        try:
            with Database.get_engine().begin() as connection:
                result = connection.execute(delete_query, {"actual_wilds": actual_wilds})
        except Exception as error:
            error_message = str(error).lower()
            if "does not exist" in error_message or "undefinedtable" in error_message:
                return 0
            raise
        return int(result.rowcount or 0)


class SalesWildStatusDailyRepository:
    """Подготавливает и сохраняет дневной snapshot статусов `wild` из квартального плана.

    Бизнес-сценарий:
    с сентября 2026 года план продаж должен учитывать количество дней, когда
    товар был в наличии. Для этого repository ежедневно сохраняет состояние
    `wild` по полю `Статус вилд` во вкладке `Поквартально` и превращает его в
    бинарный признак `is_active`.
    """

    def __init__(self) -> None:
        """Инициализирует чтение вкладки `Поквартально` как источника статусов.

        Бизнес-сценарий:
        накопление дней наличия должно использовать тот же бизнес-источник, что
        и будущий план продаж, чтобы не расходиться с квартальным контуром.
        """

        self._annual_procurement_plan = AnnualProcurementPlan()

    def sync_snapshot(self, snapshot_date: date | None = None) -> SalesWildStatusDailySyncResult:
        """Сохраняет ежедневный snapshot статусов `wild` в PostgreSQL.

        Бизнес-сценарий:
        функция формирует исторический слой для правила обнуления плана, если
        товар был в наличии меньше 15 дней за месяц. Один `wild` сохраняется
        один раз на дату snapshot-а, чтобы затем можно было считать дни по
        простому признаку `is_active = true`.
        """

        effective_snapshot_date = snapshot_date or datetime.now(MOSCOW_TIMEZONE).date()
        source_dataframe = self._annual_procurement_plan.get_quarterly_plan_data()
        prepared_dataframe, duplicate_rows = self._prepare_snapshot_dataframe(
            dataframe=source_dataframe,
            snapshot_date=effective_snapshot_date,
        )

        if prepared_dataframe.empty:
            raise ValueError(
                "Дневной справочник статусов wild не может быть обновлен: после очистки не осталось строк с wild."
            )

        Database.sync_data_to_postgres(
            table_name=SALES_WILD_STATUS_DAILY_TABLE,
            data=prepared_dataframe,
            schema_definition=SALES_WILD_STATUS_DAILY_SCHEMA,
            unique_keys=SALES_WILD_STATUS_DAILY_KEY_COLUMNS,
        )
        logger.info(
            "Дневной snapshot статусов wild сохранен в PostgreSQL | table=%s | source_rows=%s | rows_after_cleanup=%s | duplicate_rows=%s | written_rows=%s | snapshot_date=%s",
            SALES_WILD_STATUS_DAILY_TABLE,
            len(source_dataframe.index),
            len(prepared_dataframe.index),
            duplicate_rows,
            len(prepared_dataframe.index),
            effective_snapshot_date,
        )
        return SalesWildStatusDailySyncResult(
            source_rows=len(source_dataframe.index),
            rows_after_cleanup=len(prepared_dataframe.index),
            duplicate_rows=duplicate_rows,
            written_rows=len(prepared_dataframe.index),
            snapshot_date=effective_snapshot_date,
        )

    def _prepare_snapshot_dataframe(
        self,
        dataframe: pd.DataFrame,
        snapshot_date: date,
    ) -> tuple[pd.DataFrame, int]:
        """Нормализует `wild` и `Статус вилд` перед записью дневного snapshot-а.

        Бизнес-сценарий:
        слой дней наличия должен быть простым и проверяемым. Поэтому функция
        оставляет только ключ `wild`, вычисляет флаг `is_active` по
        согласованному списку статусов и убирает повторы по одному `wild` в
        рамках одной даты.
        """

        self._validate_required_columns(dataframe)

        prepared_dataframe = pd.DataFrame(
            {
                "wild": dataframe[QUARTER_PLAN_WILD_COLUMN].map(
                    SalesPlanManagerReferenceRepository._normalize_wild
                ),
                "is_active": dataframe[QUARTER_PLAN_WILD_STATUS_COLUMN].map(
                    self._status_to_is_active
                ),
            }
        )
        prepared_dataframe = prepared_dataframe.loc[
            prepared_dataframe["wild"] != ""
        ].copy()

        duplicate_rows = int(
            prepared_dataframe.duplicated(subset=["wild"], keep="last").sum()
        )
        if duplicate_rows:
            logger.warning(
                "Во вкладке Поквартально найдены повторяющиеся wild для дневного snapshot статусов, будут сохранены последние значения | duplicate_rows=%s | snapshot_date=%s",
                duplicate_rows,
                snapshot_date,
            )

        prepared_dataframe = prepared_dataframe.drop_duplicates(
            subset=["wild"],
            keep="last",
        ).copy()
        prepared_dataframe["date"] = snapshot_date
        prepared_dataframe["created_at"] = datetime.now(MOSCOW_TIMEZONE).replace(tzinfo=None)

        ordered_columns = list(SALES_WILD_STATUS_DAILY_SCHEMA.keys())
        database_dataframe = prepared_dataframe.loc[:, ordered_columns].copy()
        return database_dataframe.astype(object).where(pd.notna(database_dataframe), None), duplicate_rows

    @staticmethod
    def _validate_required_columns(dataframe: pd.DataFrame) -> None:
        """Проверяет наличие `wild` и `Статус вилд` во вкладке `Поквартально`.

        Бизнес-сценарий:
        если в источнике изменили названия ключевых колонок, накопление дней
        наличия должно остановиться явно. Иначе в БД может попасть неполный или
        искаженный дневной слой для обнуления плана.
        """

        required_columns = {
            QUARTER_PLAN_WILD_COLUMN,
            QUARTER_PLAN_WILD_STATUS_COLUMN,
        }
        missing_columns = sorted(required_columns - set(dataframe.columns))
        if missing_columns:
            raise ValueError(
                "Во вкладке Поквартально отсутствуют обязательные колонки для дневного статуса wild: "
                f"{missing_columns}"
            )

    @staticmethod
    def _status_to_is_active(value: object) -> bool:
        """Преобразует бизнес-статус `wild` в признак активного дня наличия.

        Бизнес-сценарий:
        правило обнуления плана опирается на число дней, когда товар считался
        активным в продаже. На стартовом этапе только статус `активно`
        приравнивается к `true`, а все остальные состояния считаются днем без
        подтвержденного наличия.
        """

        normalized_status = SalesPlanManagerReferenceRepository._normalize_string(value).lower()
        return normalized_status in ACTIVE_WILD_STATUSES
