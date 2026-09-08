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
    MISSING_PLAN_PRICE_VALUES,
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
    SALES_WILD_STATUS_BACKFILL_DATE_FROM,
    SALES_WILD_STATUS_BACKFILL_DATE_TO,
    SALES_WILD_STATUS_BACKFILL_NEXT_SNAPSHOT_DATE,
    SALES_WILD_STATUS_BACKFILL_PREVIOUS_SNAPSHOT_DATE,
    SOURCE_MANAGER_COLUMN,
    SOURCE_SUBJECT_COLUMN,
    SOURCE_WILD_COLUMN,
    SALES_PLAN_REPORT_COLUMN_COUNT,
    SALES_PLAN_REPORT_DOCUMENTATION_COLUMN,
    SALES_PLAN_REPORT_FIRST_DATA_ROW,
    SALES_PLAN_REPORT_HEADERS,
    SALES_PLAN_REPORT_LAST_DATA_COLUMN,
    SALES_PLAN_REPORT_UPDATED_AT_COLUMN,
    sales_plan_report_sheet,
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


@dataclass(slots=True)
class SalesWildStatusDailyBackfillResult:
    """Итог точечного восстановления пропущенных дней статусов `wild`."""

    stable_wilds: int
    changed_status_wilds: int
    confirmed_order_days: int
    skipped_existing_rows: int
    written_rows: int


@dataclass(slots=True)
class SalesPlanReportSyncResult:
    """Итог построения и публикации витрины плана продаж."""

    report_date: date
    written_rows: int


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
        для исторического справочника нужно сохранить предмет и менеджера в
        стабильных колонках БД. На этом шаге строки очищаются от пустых
        значений, а при повторении предмета сохраняется первая строка листа.
        Это защищает запись в БД, где на дату допускается только один менеджер
        для одного предмета.
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
                subset=["subject_name"],
                keep="first",
            ).sum()
        )
        if duplicate_rows:
            logger.warning(
                "В источнике найдены повторяющиеся предметы, сохранены первые строки листа | duplicate_rows=%s | snapshot_date=%s",
                duplicate_rows,
                snapshot_date,
            )

        prepared_dataframe = prepared_dataframe.drop_duplicates(
            subset=["subject_name"],
            keep="first",
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
        дополнительных соединений по Google Sheets. Пустое значение и
        служебная метка `нет цены` остаются `NULL`, а число приводится к
        формату с 2 знаками после запятой.
        """

        normalized_value = SalesPlanManagerReferenceRepository._normalize_string(value)
        if not normalized_value or normalized_value.lower() in MISSING_PLAN_PRICE_VALUES:
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

    def backfill_september_2026_gap(self) -> SalesWildStatusDailyBackfillResult:
        """Восстанавливает пропуск статусов `wild` со 2 по 7 сентября 2026 года.

        Бизнес-сценарий:
        cron не сохранил ежедневные snapshot-ы между подтвержденными датами
        1 и 8 сентября. Для товаров с одинаковым статусом в обе даты задача
        копирует этот статус на пропущенные дни. Для изменившихся товаров она
        записывает только дни с подтвержденным заказом из `funnel_daily` и не
        подменяет остальные даты предположением.
        """

        snapshots_dataframe = self._read_backfill_status_snapshots()
        funnel_dataframe = self._read_confirmed_order_days_for_backfill()
        existing_dataframe = self._read_existing_backfill_rows()
        (
            payload_dataframe,
            stable_wilds,
            changed_status_wilds,
            confirmed_order_days,
            skipped_existing_rows,
        ) = self._build_backfill_dataframe(
            snapshots_dataframe=snapshots_dataframe,
            funnel_dataframe=funnel_dataframe,
            existing_dataframe=existing_dataframe,
        )

        if not payload_dataframe.empty:
            Database.sync_data_to_postgres(
                table_name=SALES_WILD_STATUS_DAILY_TABLE,
                data=payload_dataframe,
                schema_definition=SALES_WILD_STATUS_DAILY_SCHEMA,
                unique_keys=SALES_WILD_STATUS_DAILY_KEY_COLUMNS,
            )

        logger.info(
            "Восстановление пропуска дневных статусов wild завершено | date_from=%s | date_to=%s | stable_wilds=%s | changed_status_wilds=%s | confirmed_order_days=%s | skipped_existing_rows=%s | written_rows=%s",
            SALES_WILD_STATUS_BACKFILL_DATE_FROM,
            SALES_WILD_STATUS_BACKFILL_DATE_TO,
            stable_wilds,
            changed_status_wilds,
            confirmed_order_days,
            skipped_existing_rows,
            len(payload_dataframe.index),
        )
        return SalesWildStatusDailyBackfillResult(
            stable_wilds=stable_wilds,
            changed_status_wilds=changed_status_wilds,
            confirmed_order_days=confirmed_order_days,
            skipped_existing_rows=skipped_existing_rows,
            written_rows=len(payload_dataframe.index),
        )

    @staticmethod
    def _read_backfill_status_snapshots() -> pd.DataFrame:
        """Читает два подтвержденных snapshot-а для восстановления пропуска.

        Бизнес-сценарий:
        восстановление использует только реально сохраненные статусы на
        границах пропуска. Текущий статус листа Google Sheets не участвует,
        чтобы не исказить историю сентября.
        """

        query = text(
            """
            SELECT
                date,
                wild,
                is_active
            FROM sales_wild_status_daily
            WHERE date IN :snapshot_dates
            """
        ).bindparams(
            bindparam(
                "snapshot_dates",
                expanding=True,
            )
        )
        return Database.read_sql_to_dataframe(
            query,
            params={
                "snapshot_dates": [
                    SALES_WILD_STATUS_BACKFILL_PREVIOUS_SNAPSHOT_DATE,
                    SALES_WILD_STATUS_BACKFILL_NEXT_SNAPSHOT_DATE,
                ]
            },
        )

    @staticmethod
    def _read_confirmed_order_days_for_backfill() -> pd.DataFrame:
        """Читает дни с заказами как подтверждение активности изменившихся товаров.

        Бизнес-сценарий:
        для `wild`, чей статус изменился между двумя snapshot-ами, заказ в
        конкретный день подтверждает активность товара. Отсутствие заказа не
        считается подтверждением неактивности и не создает строку статуса.
        """

        query = text(
            """
            SELECT DISTINCT
                fd.date,
                a.local_vendor_code AS wild
            FROM funnel_daily fd
            JOIN article a
                ON a.nm_id = fd.nm_id
            WHERE fd.date BETWEEN :date_from AND :date_to
              AND fd.orders_sum > 0
              AND a.local_vendor_code IS NOT NULL
            """
        )
        return Database.read_sql_to_dataframe(
            query,
            params={
                "date_from": SALES_WILD_STATUS_BACKFILL_DATE_FROM,
                "date_to": SALES_WILD_STATUS_BACKFILL_DATE_TO,
            },
        )

    @staticmethod
    def _read_existing_backfill_rows() -> pd.DataFrame:
        """Читает уже существующие строки пропущенного периода перед backfill.

        Бизнес-сценарий:
        повторный запуск восстановления не должен перезаписывать фактический
        snapshot, если часть дат уже была загружена вручную или cron-задачей.
        """

        query = text(
            """
            SELECT
                date,
                wild
            FROM sales_wild_status_daily
            WHERE date BETWEEN :date_from AND :date_to
            """
        )
        return Database.read_sql_to_dataframe(
            query,
            params={
                "date_from": SALES_WILD_STATUS_BACKFILL_DATE_FROM,
                "date_to": SALES_WILD_STATUS_BACKFILL_DATE_TO,
            },
        )

    @staticmethod
    def _build_backfill_dataframe(
        snapshots_dataframe: pd.DataFrame,
        funnel_dataframe: pd.DataFrame,
        existing_dataframe: pd.DataFrame,
    ) -> tuple[pd.DataFrame, int, int, int, int]:
        """Готовит безопасные строки для восстановления пропуска статусов `wild`.

        Бизнес-сценарий:
        одинаковый статус на обеих границах пропуска считается достаточным
        основанием для заполнения всех промежуточных дат. При смене статуса
        добавляются только дни с заказом, а уже существующие записи всегда
        исключаются из payload, чтобы не заменить фактические данные.
        """

        normalized_snapshots = snapshots_dataframe.copy()
        normalized_snapshots["date"] = pd.to_datetime(
            normalized_snapshots["date"]
        ).dt.date
        previous_snapshot = normalized_snapshots.loc[
            normalized_snapshots["date"] == SALES_WILD_STATUS_BACKFILL_PREVIOUS_SNAPSHOT_DATE,
            ["wild", "is_active"],
        ].rename(columns={"is_active": "is_active_before"})
        next_snapshot = normalized_snapshots.loc[
            normalized_snapshots["date"] == SALES_WILD_STATUS_BACKFILL_NEXT_SNAPSHOT_DATE,
            ["wild", "is_active"],
        ].rename(columns={"is_active": "is_active_after"})

        if previous_snapshot.empty or next_snapshot.empty:
            raise ValueError(
                "Восстановление статусов wild отменено: отсутствует один из подтвержденных snapshot-ов за 1 или 8 сентября 2026 года."
            )

        compared_statuses = previous_snapshot.merge(
            next_snapshot,
            on="wild",
            how="inner",
            validate="one_to_one",
        )
        stable_statuses = compared_statuses.loc[
            compared_statuses["is_active_before"] == compared_statuses["is_active_after"],
            ["wild", "is_active_before"],
        ].rename(columns={"is_active_before": "is_active"})
        changed_wilds = compared_statuses.loc[
            compared_statuses["is_active_before"] != compared_statuses["is_active_after"],
            "wild",
        ].tolist()

        missing_dates = [
            timestamp.date()
            for timestamp in pd.date_range(
                SALES_WILD_STATUS_BACKFILL_DATE_FROM,
                SALES_WILD_STATUS_BACKFILL_DATE_TO,
                freq="D",
            )
        ]
        stable_rows = pd.concat(
            [stable_statuses.assign(date=snapshot_date) for snapshot_date in missing_dates],
            ignore_index=True,
        )

        normalized_funnel = funnel_dataframe.copy()
        if normalized_funnel.empty:
            confirmed_order_rows = pd.DataFrame(columns=["wild", "is_active", "date"])
        else:
            normalized_funnel["date"] = pd.to_datetime(normalized_funnel["date"]).dt.date
            confirmed_order_rows = normalized_funnel.loc[
                normalized_funnel["wild"].isin(changed_wilds),
                ["wild", "date"],
            ].drop_duplicates()
            confirmed_order_rows["is_active"] = True
            confirmed_order_rows = confirmed_order_rows.loc[:, ["wild", "is_active", "date"]]

        candidates = pd.concat(
            [stable_rows, confirmed_order_rows],
            ignore_index=True,
        ).drop_duplicates(subset=["date", "wild"], keep="first")
        normalized_existing = existing_dataframe.copy()
        if normalized_existing.empty:
            existing_keys = pd.DataFrame(columns=["date", "wild"])
        else:
            normalized_existing["date"] = pd.to_datetime(normalized_existing["date"]).dt.date
            existing_keys = normalized_existing.loc[:, ["date", "wild"]].drop_duplicates()

        rows_with_marker = candidates.merge(
            existing_keys,
            on=["date", "wild"],
            how="left",
            indicator=True,
        )
        skipped_existing_rows = int((rows_with_marker["_merge"] == "both").sum())
        payload_dataframe = rows_with_marker.loc[
            rows_with_marker["_merge"] == "left_only",
            ["wild", "is_active", "date"],
        ].copy()
        payload_dataframe["created_at"] = datetime.now(MOSCOW_TIMEZONE).replace(tzinfo=None)
        payload_dataframe = payload_dataframe.loc[
            :, list(SALES_WILD_STATUS_DAILY_SCHEMA.keys())
        ].sort_values(["date", "wild"])

        return (
            payload_dataframe.astype(object).where(pd.notna(payload_dataframe), None),
            len(stable_statuses.index),
            len(changed_wilds),
            len(confirmed_order_rows.index),
            skipped_existing_rows,
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


class SalesPlanReportRepository:
    """Строит техническую витрину плана продаж и публикует её в Google Sheets.

    Бизнес-сценарий:
    витрина объединяет месячный план из справочника учетной категории,
    накопленные дни активности и факт заказов. Расчет ведется на уровне
    `wild`, чтобы план и факт не дублировались между категориями.
    """

    def __init__(self) -> None:
        """Инициализирует подключение к целевой вкладке плана продаж.

        Бизнес-сценарий:
        задача должна обновлять только технический диапазон отчета в заданной
        вкладке. Стабильный `spreadsheet_id` защищает выгрузку от ручного
        переименования документа.
        """

        self._connector = GoogleTabs(
            table_title=sales_plan_report_sheet["title"],
            sheet_title=sales_plan_report_sheet["sheet_title"],
            spreadsheet_id=sales_plan_report_sheet["spreadsheet_id"],
        )

    def sync_report(self, report_date: date | None = None) -> SalesPlanReportSyncResult:
        """Считает месячный план на дату отчета и заменяет данные витрины.

        Бизнес-сценарий:
        при обычном запуске используется текущая московская дата. Для каждого
        `wild` рассчитываются план, план на дату, факт, линейный прогноз и
        правило 15 дней. В Google Sheets обновляется только диапазон `A:R`
        с третьей строки: формулы итогов в первой строке, заголовки во второй
        строке и документация в столбце `T` сохраняются.
        """

        effective_report_date = report_date or datetime.now(MOSCOW_TIMEZONE).date()
        report_dataframe = self._read_report_dataframe(report_date=effective_report_date)
        prepared_dataframe = self._prepare_dataframe_for_sheet(report_dataframe)
        prepared_dataframe = self._add_updated_at_column(prepared_dataframe)
        self._write_report_dataframe(prepared_dataframe)

        return SalesPlanReportSyncResult(
            report_date=effective_report_date,
            written_rows=len(prepared_dataframe.index),
        )

    @staticmethod
    def _read_report_dataframe(report_date: date) -> pd.DataFrame:
        """Читает из PostgreSQL расчетные показатели плана продаж на дату.

        Бизнес-сценарий:
        план берется из единственной учетной категории `wild`, факт — из
        `funnel_daily`, а дни наличия — из дневных snapshot-ов статуса.
        Правило 15 дней применяется только к итоговому плану; оперативный
        план на дату рассчитывается от базового месячного плана.
        """

        query = text(
            """
            WITH params AS (
                SELECT
                    CAST(:report_date AS date) AS report_date,
                    date_trunc('month', CAST(:report_date AS date))::date AS month_start,
                    (
                        date_trunc('month', CAST(:report_date AS date))
                        + INTERVAL '1 month - 1 day'
                    )::date AS month_end,
                    to_char(CAST(:report_date AS date), 'MM-YYYY') AS month_label
            ),
            base AS (
                SELECT
                    sp.wild,
                    sp.subject_name,
                    ROUND(sp.quarter_3_units_2026) AS quantity_plan,
                    sp.plan_price,
                    ROUND(
                        sp.plan_price * ROUND(sp.quarter_3_units_2026),
                        2
                    ) AS plan_orders_rub
                FROM sales_plan_accounting_category_reference AS sp
            ),
            status_daily AS (
                SELECT
                    sd.wild,
                    COUNT(*) FILTER (WHERE sd.is_active IS TRUE) AS days_with_stocks
                FROM sales_wild_status_daily AS sd
                CROSS JOIN params AS p
                WHERE sd.date BETWEEN p.month_start AND p.month_end
                GROUP BY sd.wild
            ),
            manager_map AS (
                SELECT DISTINCT ON (cmr.subject_name)
                    cmr.subject_name,
                    cmr.manager_name
                FROM sales_plan_category_manager_reference AS cmr
                CROSS JOIN params AS p
                WHERE cmr.snapshot_date <= p.report_date
                ORDER BY cmr.subject_name, cmr.snapshot_date DESC
            ),
            funnel AS (
                SELECT
                    a.local_vendor_code AS wild,
                    SUM(fd.order_count) AS fact_order_count,
                    SUM(fd.orders_sum) AS fact_orders_rub
                FROM funnel_daily AS fd
                LEFT JOIN article AS a
                    ON a.nm_id = fd.nm_id
                CROSS JOIN params AS p
                WHERE fd.date BETWEEN p.month_start AND p.report_date
                    AND fd.orders_sum > 0
                GROUP BY a.local_vendor_code
            ),
            date_params AS (
                SELECT
                    month_label,
                    report_date - month_start + 1 AS elapsed_days,
                    month_end - month_start + 1 AS days_in_month
                FROM params
            ),
            prepared AS (
                SELECT
                    dp.month_label,
                    mm.manager_name,
                    sp.wild,
                    sp.subject_name,
                    COALESCE(sd.days_with_stocks, 0) AS days_with_stocks,
                    sp.quantity_plan,
                    COALESCE(fd.fact_order_count, 0) AS fact_order_count,
                    sp.plan_price,
                    ROUND(
                        COALESCE(fd.fact_orders_rub, 0)
                        / NULLIF(fd.fact_order_count, 0)
                    ) AS fact_price,
                    sp.plan_orders_rub,
                    ROUND(
                        sp.plan_orders_rub * dp.elapsed_days
                        / NULLIF(dp.days_in_month, 0),
                        2
                    ) AS plan_orders_rub_to_date,
                    CASE
                        WHEN COALESCE(sd.days_with_stocks, 0) < 15 THEN 0
                        ELSE sp.plan_orders_rub
                    END AS plan_orders_rub_after_15_days_rule,
                    COALESCE(fd.fact_orders_rub, 0) AS fact_orders_rub,
                    ROUND(
                        COALESCE(fd.fact_orders_rub, 0)
                        / NULLIF(dp.elapsed_days, 0)
                        * dp.days_in_month,
                        2
                    ) AS linear_forecast_rub
                FROM base AS sp
                CROSS JOIN date_params AS dp
                LEFT JOIN status_daily AS sd
                    ON sd.wild = sp.wild
                LEFT JOIN manager_map AS mm
                    ON mm.subject_name = sp.subject_name
                LEFT JOIN funnel AS fd
                    ON fd.wild = sp.wild
            ),
            calculated AS (
                SELECT
                    prepared.*,
                    ROUND(
                        prepared.fact_orders_rub
                        / NULLIF(prepared.plan_orders_rub_to_date, 0),
                        2
                    ) AS plan_execution_to_date_percent,
                    ROUND(
                        prepared.fact_orders_rub
                        / NULLIF(prepared.plan_orders_rub, 0),
                        2
                    ) AS fact_orders_percent,
                    ROUND(
                        prepared.linear_forecast_rub
                        / NULLIF(prepared.plan_orders_rub, 0),
                        2
                    ) AS forecast_percent,
                    ROUND(
                        prepared.fact_orders_rub
                        / NULLIF(prepared.plan_orders_rub_after_15_days_rule, 0),
                        2
                    ) AS fact_orders_percent_after_15_days_rule,
                    ROUND(
                        prepared.linear_forecast_rub
                        / NULLIF(prepared.plan_orders_rub_after_15_days_rule, 0),
                        2
                    ) AS forecast_percent_after_15_days_rule
                FROM prepared
            )
            SELECT
                month_label,
                manager_name,
                wild,
                subject_name,
                days_with_stocks,
                quantity_plan,
                fact_order_count,
                plan_price,
                fact_price,
                plan_orders_rub,
                plan_orders_rub_to_date,
                plan_execution_to_date_percent,
                plan_orders_rub_after_15_days_rule,
                fact_orders_rub,
                fact_orders_percent,
                linear_forecast_rub,
                forecast_percent,
                fact_orders_percent_after_15_days_rule,
                forecast_percent_after_15_days_rule
            FROM calculated
            ORDER BY subject_name, wild
            """
        )
        return Database.read_sql_to_dataframe(query, params={"report_date": report_date})

    @staticmethod
    def _prepare_dataframe_for_sheet(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Заменяет отсутствующие значения и служебную строку `[NULL]` на пустые ячейки.

        Бизнес-сценарий:
        пустая цена или показатель без доступных данных — нормальная ситуация
        для новинки или товара без заказов. В витрине такие случаи должны быть
        визуально пустыми, а не выглядеть как строковое значение `[NULL]`.
        """

        prepared_dataframe = dataframe.copy()
        prepared_dataframe = prepared_dataframe.replace(
            r"^\s*\[NULL\]\s*$",
            "",
            regex=True,
        )
        return prepared_dataframe.where(pd.notna(prepared_dataframe), "")

    @staticmethod
    def _add_updated_at_column(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Добавляет время публикации, общее для всех строк одной выгрузки.

        Бизнес-сценарий:
        пользователи витрины должны видеть, когда именно были актуализированы
        данные. Для сопоставимости всех строк запуска используется одно
        московское время, а имя колонки сохраняется как согласованное
        `updatet_at`.
        """

        prepared_dataframe = dataframe.copy()
        updated_at = datetime.now(MOSCOW_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        prepared_dataframe[SALES_PLAN_REPORT_UPDATED_AT_COLUMN] = updated_at
        return prepared_dataframe

    def _write_report_dataframe(self, dataframe: pd.DataFrame) -> None:
        """Перезаписывает данные отчета, не затрагивая формулы, заголовки и документацию.

        Бизнес-сценарий:
        витрина должна показывать только актуальный набор `wild`. Поэтому
        диапазон данных заполняется до последней доступной строки пустыми
        значениями после результата, что удаляет устаревшие строки предыдущей
        выгрузки. Области итогов, шапки и документации не изменяются.
        """

        if len(dataframe.columns) != SALES_PLAN_REPORT_COLUMN_COUNT:
            raise ValueError(
                "Выгрузка плана продаж отменена: число колонок расчета не совпадает "
                f"с техническим диапазоном | expected={SALES_PLAN_REPORT_COLUMN_COUNT} "
                f"| actual={len(dataframe.columns)}"
            )

        worksheet = self._connector.sheet_title
        available_rows = worksheet.row_count - SALES_PLAN_REPORT_FIRST_DATA_ROW + 1
        if len(dataframe.index) > available_rows:
            raise ValueError(
                "Выгрузка плана продаж отменена: в техническом диапазоне недостаточно строк "
                f"| available_rows={available_rows} | required_rows={len(dataframe.index)}"
            )

        self._prepare_sheet_layout(worksheet)

        values = dataframe.astype(object).values.tolist()
        empty_row = ["" for _ in range(SALES_PLAN_REPORT_COLUMN_COUNT)]
        values.extend(
            [empty_row.copy() for _ in range(available_rows - len(values))]
        )
        target_range = (
            f"A{SALES_PLAN_REPORT_FIRST_DATA_ROW}:"
            f"{SALES_PLAN_REPORT_LAST_DATA_COLUMN}{worksheet.row_count}"
        )
        self._connector.update_range(range_name=target_range, values=values)
        self._connector.format_range(
            range_name=(
                f"T{SALES_PLAN_REPORT_FIRST_DATA_ROW}:T{worksheet.row_count}"
            ),
            format_properties={
                "numberFormat": {
                    "type": "DATE_TIME",
                    "pattern": "yyyy-mm-dd hh:mm:ss",
                }
            },
        )
        logger.info(
            "Витрина плана продаж обновлена в Google Sheets | report_rows=%s | range=%s",
            len(dataframe.index),
            target_range,
        )

    def _prepare_sheet_layout(self, worksheet) -> None:
        """Подготавливает колонки витрины после добавления менеджера в столбец `B`.

        Бизнес-сценарий:
        менеджер нужен для сводных итогов, поэтому его добавляют рядом с
        месяцем. Метод переносит документацию из прежнего столбца `T` в `U`,
        обновляет заголовки и пересчитывает адреса итоговых формул так, чтобы
        сдвиг данных не исказил суммарные показатели.
        """

        layout_values = worksheet.get(
            f"T1:{SALES_PLAN_REPORT_DOCUMENTATION_COLUMN}{worksheet.row_count}",
            value_render_option="FORMULA",
        )
        legacy_documentation_header = ""
        current_documentation_header = ""
        if len(layout_values) > 1:
            legacy_documentation_header = layout_values[1][0] if layout_values[1] else ""
            current_documentation_header = (
                layout_values[1][1] if len(layout_values[1]) > 1 else ""
            )

        if (
            legacy_documentation_header == "Документация"
            and current_documentation_header != "Документация"
        ):
            documentation_values = []
            for row_index in range(worksheet.row_count):
                row = layout_values[row_index] if row_index < len(layout_values) else []
                documentation_values.append([row[0] if row else ""])
            self._connector.update_range(
                range_name=(
                    f"{SALES_PLAN_REPORT_DOCUMENTATION_COLUMN}1:"
                    f"{SALES_PLAN_REPORT_DOCUMENTATION_COLUMN}{worksheet.row_count}"
                ),
                values=documentation_values,
            )

        self._connector.update_range(
            range_name="A1:T1",
            values=[["" for _ in range(SALES_PLAN_REPORT_COLUMN_COUNT)]],
        )
        self._connector.update_range(
            range_name="F1:Q1",
            values=[[
                "=SUBTOTAL(9;F3:F)",
                "=SUBTOTAL(9;G3:G)",
                "",
                "",
                "=SUBTOTAL(9;J3:J)",
                "=SUBTOTAL(9;K3:K)",
                "",
                "=SUBTOTAL(9;M3:M)",
                "=SUBTOTAL(9;N3:N)",
                "=N1/J1",
                "=SUBTOTAL(9;P3:P)",
                "=P1/J1",
            ]],
        )
        self._connector.update_range(
            range_name="A2:T2",
            values=[list(SALES_PLAN_REPORT_HEADERS)],
        )
