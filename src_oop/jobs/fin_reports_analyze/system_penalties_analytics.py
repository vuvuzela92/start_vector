from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine, URL

from src_oop.core.database import Database

load_dotenv()

logger = logging.getLogger(__name__)

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
DETAIL_TABLE = "penalties_root_cause_orders"
SUMMARY_VIEW = "penalties_root_cause_summary"
TARGET_PENALTY_MARKER = "невыполненный заказ"

DETAIL_COLUMNS = [
    "analysis_month_msk",
    "realizationreport_id",
    "order_dt",
    "account",
    "sa_name",
    "subject_name",
    "bonus_type_name",
    "assembly_id",
    "nm_id",
    "penalty",
    "wild",
    "stock_on_order_date",
    "stock_state",
    "acceptance_act_found",
    "acceptance_document_numbers",
    "acceptance_act_date",
    "acceptance_row_count",
    "service_task_found",
    "wb_created_at",
    "fbs_real_status",
    "status_row_count",
    "shipped_at",
    "wb_status_sorted",
    "has_shipped",
    "has_wb_sorted",
    "hours_created_to_shipped",
    "order_processing_hours",
    "hours_shipped_to_wb_sorted",
    "hours_created_to_wb_sorted",
    "event_state",
    "event_data_quality",
    "responsibility_stage",
    "probable_responsible_department",
    "classification_confidence",
    "classification_reason",
    "is_focus_penalty",
    "loaded_at",
]


@dataclass(slots=True)
class PenaltiesAnalysisResult:
    """Результат построения витрины штрафов за выбранный период.

    Бизнес-сценарий: результат передает число записей, контрольные суммы и
    диагностические показатели оператору, чтобы запуск можно было проверить
    без ручного сравнения нескольких запросов.
    """

    month_start: date
    date_from: date
    date_to: date
    rolling_window: bool
    rows_written: int
    total_penalty: Decimal
    focus_penalty: Decimal
    unmatched_fbs_orders: int
    missing_status_orders: int
    inconsistent_event_orders: int


class FbsDatabase:
    """Read-only подключение к БД сервиса сборочных заданий.

    Бизнес-сценарий: аналитика читает операционные события с хоста FBS,
    отделенного от финансовой БД. Класс намеренно не содержит методов записи,
    чтобы аналитический запуск не мог изменить сервисные таблицы.
    """

    _engine: Engine | None = None

    @classmethod
    def get_engine(cls) -> Engine:
        """Создает и переиспользует read-only engine для FBS-БД.

        Бизнес-правило: параметры подключения берутся только из `DB_*_FBS`,
        чтобы данные хоста 155 не смешивались с основной БД хоста 149.
        """

        if cls._engine is None:
            required = {
                "user": os.getenv("DB_USER_FBS"),
                "password": os.getenv("DB_PASSWORD_FBS"),
                "host": os.getenv("DB_HOST_FBS"),
                "port": os.getenv("DB_PORT_FBS"),
                "database": os.getenv("DB_NAME_FBS"),
            }
            missing = [name for name, value in required.items() if not value]
            if missing:
                raise RuntimeError(
                    "Не заданы параметры подключения к FBS-БД: "
                    + ", ".join(missing)
                )

            url = URL.create(
                drivername="postgresql",
                username=required["user"],
                password=required["password"],
                host=required["host"],
                port=required["port"],
                database=required["database"],
            )
            cls._engine = create_engine(
                url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=5,
                connect_args={"connect_timeout": 15},
            )
        return cls._engine

    @classmethod
    def read_sql_to_dataframe(cls, query: Any, params: dict[str, Any]) -> pd.DataFrame:
        """Читает DataFrame из FBS-БД без возможности записи.

        Бизнес-правило: операционные события загружаются батчами по известным
        сборочным заданиям, чтобы не сканировать всю историю сервиса.
        """

        with cls.get_engine().connect() as connection:
            return pd.read_sql(query, connection, params=params)


class SystemPenaltiesAnalyzer:
    """Строит и сохраняет системную витрину штрафов.

    Бизнес-сценарий: финансовые строки за московский месяц агрегируются на
    основной БД, затем обогащаются событиями заказа из FBS-БД и остатками,
    классифицируются по этапу возникновения риска и сохраняются в PostgreSQL
    с контрольной сверкой сумм.
    """

    def __init__(
        self,
        month_start: date | str | None = None,
        rolling_days: int = 28,
        database_cls: type[Database] = Database,
        fbs_database_cls: type[FbsDatabase] = FbsDatabase,
    ) -> None:
        """Инициализирует период и источники данных аналитики.

        Бизнес-правило: без явно заданного месяца используется скользящее
        окно из 28 календарных дней, включая текущую дату в `Europe/Moscow`.
        Явно заданный месяц по-прежнему обрабатывается полностью.
        """

        if rolling_days <= 0:
            raise ValueError("Размер скользящего окна должен быть положительным.")

        self.rolling_window = month_start is None
        if self.rolling_window:
            today_msk = datetime.now(MOSCOW_TZ).date()
            self.date_from = today_msk - timedelta(days=rolling_days - 1)
            self.date_to = today_msk + timedelta(days=1)
            self.month_start = self.date_from.replace(day=1)
            self.month_end = self.date_to
        else:
            self.month_start = self._normalize_month_start(month_start)
            self.month_end = self._next_month(self.month_start)
            self.date_from = self.month_start
            self.date_to = self.month_end
        self.database_cls = database_cls
        self.fbs_database_cls = fbs_database_cls

    @staticmethod
    def _normalize_month_start(month_start: date | str | None) -> date:
        """Приводит входной период к первому дню месяца.

        Бизнес-правило: при явном выборе месяца витрина строится по полному
        календарному месяцу, поэтому дата нормализуется до первого числа.
        """

        if month_start is None:
            current = datetime.now(MOSCOW_TZ).date()
        elif isinstance(month_start, datetime):
            current = month_start.date()
        elif isinstance(month_start, date):
            current = month_start
        else:
            current = pd.Timestamp(month_start).date()
        return current.replace(day=1)

    @staticmethod
    def _next_month(month_start: date) -> date:
        """Возвращает правую границу календарного месяца.

        Бизнес-правило: SQL-фильтры используют полуоткрытый интервал
        `[month_start, month_end)`, чтобы не зависеть от времени последнего дня.
        """

        if month_start.month == 12:
            return date(month_start.year + 1, 1, 1)
        return date(month_start.year, month_start.month + 1, 1)

    @staticmethod
    def _chunks(values: list[int], size: int = 500) -> list[list[int]]:
        """Разбивает идентификаторы на батчи для параметризованных запросов.

        Бизнес-правило: большие списки `ANY` разбиваются, чтобы чтение
        операционных таблиц не создавало чрезмерный запрос и не перегружало БД.
        """

        if size <= 0:
            raise ValueError("Размер батча должен быть положительным.")
        return [values[index:index + size] for index in range(0, len(values), size)]

    def _load_financial_penalties(self) -> pd.DataFrame:
        """Загружает все ненулевые штрафы выбранного московского месяца.

        Бизнес-правило: финансовая сверка строится по всем основаниям, а не
        только по невыполненным заказам. Группировка выполняется до соединений,
        чтобы статусы и остатки не раздували сумму штрафов.
        """

        query = text(
            """
            WITH article_codes AS (
                SELECT nm_id, MAX(local_vendor_code) AS wild
                FROM public.article
                GROUP BY nm_id
            )
            SELECT
                f.realizationreport_id,
                DATE_TRUNC(
                    'month',
                    f.order_dt AT TIME ZONE 'Europe/Moscow'
                )::date AS analysis_month_msk,
                MIN(f.order_dt) AS order_dt,
                MAX(f.account) AS account,
                MAX(f.sa_name) AS sa_name,
                MAX(f.subject_name) AS subject_name,
                f.bonus_type_name,
                f.assembly_id,
                f.nm_id,
                SUM(f.penalty)::numeric AS penalty,
                ac.wild
            FROM public.daily_fin_reports_full f
            LEFT JOIN article_codes ac ON ac.nm_id = f.nm_id
            WHERE (f.order_dt AT TIME ZONE 'Europe/Moscow')::date >= :date_from
              AND (f.order_dt AT TIME ZONE 'Europe/Moscow')::date < :date_to
              AND f.penalty != 0
              AND f.assembly_id != 0
            GROUP BY
                f.realizationreport_id,
                DATE_TRUNC(
                    'month',
                    f.order_dt AT TIME ZONE 'Europe/Moscow'
                )::date,
                f.bonus_type_name,
                f.assembly_id,
                f.nm_id,
                ac.wild
            """
        )
        params = {
            "date_from": self.date_from,
            "date_to": self.date_to,
        }
        dataframe = self.database_cls.read_sql_to_dataframe(query, params=params)
        if dataframe.empty:
            return pd.DataFrame(columns=DETAIL_COLUMNS)
        dataframe["order_dt"] = pd.to_datetime(dataframe["order_dt"], utc=True)
        dataframe["penalty"] = pd.to_numeric(dataframe["penalty"], errors="coerce")
        dataframe["assembly_id"] = pd.to_numeric(
            dataframe["assembly_id"], errors="coerce"
        ).astype("Int64")
        dataframe["nm_id"] = pd.to_numeric(dataframe["nm_id"], errors="coerce").astype("Int64")
        dataframe["wild"] = dataframe["wild"].astype("string").str.strip()
        logger.info(
            "Загружены финансовые штрафы за период | date_from=%s | date_to=%s | rows=%s | sum=%.2f",
            self.date_from,
            self.date_to - timedelta(days=1),
            len(dataframe),
            dataframe["penalty"].sum(),
        )
        return dataframe

    def _load_fbs_orders(self, assembly_ids: list[int]) -> pd.DataFrame:
        """Загружает задания и агрегированные статусы из FBS-БД.

        Бизнес-правило: по каждому `wb_assembly_task.id` возвращается ровно
        одна строка. Отгрузкой считается самое раннее событие с
        `real_status = 'shipped'`, а сортировкой WB — последнее непустое
        значение `wb_status_sorted`.
        """

        columns = [
            "assembly_id",
            "service_task_found",
            "wb_created_at",
            "service_nm_id",
            "service_article",
            "service_account",
            "warehouse_id",
            "office_id",
            "fbs_real_status",
            "status_row_count",
            "shipped_at",
            "wb_status_sorted",
            "has_shipped",
            "has_wb_sorted",
        ]
        if not assembly_ids:
            return pd.DataFrame(columns=columns)

        query = text(
            """
            SELECT
                t.id AS assembly_id,
                TRUE AS service_task_found,
                t.wb_created_at,
                t.nm_id AS service_nm_id,
                t.article AS service_article,
                t.account AS service_account,
                t.warehouse_id,
                t.office_id,
                (ARRAY_AGG(s.real_status ORDER BY s.real_status_date_changed DESC NULLS LAST))[1]
                    AS fbs_real_status,
                COUNT(s.id)::integer AS status_row_count,
                MIN(s.real_status_date_changed) FILTER (
                    WHERE LOWER(s.real_status) = 'shipped'
                ) AS shipped_at,
                MAX(s.wb_status_sorted) AS wb_status_sorted
            FROM public.wb_assembly_task t
            LEFT JOIN public.wb_assembly_task_status s ON s.id = t.id
            WHERE t.id = ANY(:assembly_ids)
            GROUP BY
                t.id,
                t.wb_created_at,
                t.nm_id,
                t.article,
                t.account,
                t.warehouse_id,
                t.office_id
            """
        )
        frames: list[pd.DataFrame] = []
        for batch in self._chunks(assembly_ids):
            frames.append(
                self.fbs_database_cls.read_sql_to_dataframe(
                    query,
                    params={"assembly_ids": batch},
                )
            )
        dataframe = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)
        if dataframe.empty:
            return dataframe
        for column in ("wb_created_at", "shipped_at", "wb_status_sorted"):
            dataframe[column] = pd.to_datetime(dataframe[column], utc=True, errors="coerce")
        dataframe["has_shipped"] = dataframe["shipped_at"].notna()
        dataframe["has_wb_sorted"] = dataframe["wb_status_sorted"].notna()
        return dataframe.drop_duplicates("assembly_id", keep="first")

    def _load_acceptance_acts(self, assembly_ids: list[int]) -> pd.DataFrame:
        """Загружает признаки сформированных площадкой актов приемки.

        Бизнес-правило: `acceptance_fbs_acts_new` подтверждает, что площадка
        сформировала документ по сборочному заданию. На один заказ возвращается
        одна строка: список номеров документов, максимальная дата документа и
        количество исходных строк.
        """

        columns = [
            "assembly_id",
            "acceptance_act_found",
            "acceptance_document_numbers",
            "acceptance_act_date",
            "acceptance_row_count",
        ]
        if not assembly_ids:
            return pd.DataFrame(columns=columns)

        query = text(
            """
            SELECT
                af.order_number::bigint AS assembly_id,
                TRUE AS acceptance_act_found,
                STRING_AGG(
                    DISTINCT af.document_number,
                    ' | ' ORDER BY af.document_number
                ) AS acceptance_document_numbers,
                MAX(af.date) AS acceptance_act_date,
                COUNT(*)::integer AS acceptance_row_count
            FROM public.acceptance_fbs_acts_new af
            WHERE af.order_number ~ '^[0-9]+$'
              AND af.order_number::bigint = ANY(:assembly_ids)
            GROUP BY af.order_number::bigint
            """
        )
        frames: list[pd.DataFrame] = []
        for batch in self._chunks(assembly_ids):
            frames.append(
                self.database_cls.read_sql_to_dataframe(
                    query,
                    params={"assembly_ids": batch},
                )
            )
        dataframe = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)
        if dataframe.empty:
            return dataframe
        dataframe["acceptance_act_date"] = pd.to_datetime(
            dataframe["acceptance_act_date"], errors="coerce"
        ).dt.date
        return dataframe.drop_duplicates("assembly_id", keep="first")

    def _load_stock_snapshot(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Загружает остаток на дату заказа из `public.wms_stock`.

        Бизнес-правило: остаток используется только как объясняющий признак
        для штрафов по невыполненным заказам; остальные основания не требуют
        дополнительного чтения дневной WMS-витрины. В поле
        `stock_on_order_date` переносится общий остаток `wms_stock.stock_qty`.
        """

        target = dataframe[
            dataframe["bonus_type_name"].fillna("").str.contains(
                TARGET_PENALTY_MARKER,
                case=False,
                na=False,
            )
        ].copy()
        target = target.dropna(subset=["wild"])
        if target.empty:
            return pd.DataFrame(columns=["assembly_id", "stock_on_order_date"])

        wilds = target["wild"].astype(str).unique().tolist()
        query = text(
            """
            SELECT
                ws.product_id AS wild,
                ws.balance_date AS transaction_date,
                MAX(ws.stock_qty)::numeric AS stock_on_order_date,
                COUNT(*)::integer AS stock_row_count
            FROM public.wms_stock ws
            WHERE ws.product_id = ANY(:wilds)
              AND ws.balance_date >= :date_from
              AND ws.balance_date < :date_to
            GROUP BY ws.product_id, ws.balance_date
            """
        )
        stock_frames: list[pd.DataFrame] = []
        # Список артикулов может быть большим, поэтому батчим отдельно от id заказов.
        for start in range(0, len(wilds), 500):
            stock_frames.append(
                self.database_cls.read_sql_to_dataframe(
                    query,
                    params={
                        "wilds": wilds[start:start + 500],
                        "date_from": self.date_from,
                        "date_to": self.date_to,
                    },
                )
            )
        stock = pd.concat(stock_frames, ignore_index=True) if stock_frames else pd.DataFrame()
        if stock.empty:
            return pd.DataFrame(columns=["assembly_id", "stock_on_order_date"])

        stock["transaction_date"] = pd.to_datetime(stock["transaction_date"], errors="coerce").dt.date
        target["order_date"] = pd.to_datetime(target["order_dt"], utc=True).dt.tz_convert(
            MOSCOW_TZ
        ).dt.date
        target["wild"] = target["wild"].astype(str).str.strip()
        stock["wild"] = stock["wild"].astype(str).str.strip()
        merged = target[["assembly_id", "nm_id", "wild", "order_date"]].merge(
            stock,
            left_on=["wild", "order_date"],
            right_on=["wild", "transaction_date"],
            how="left",
        )
        return merged[["assembly_id", "nm_id", "stock_on_order_date"]].drop_duplicates(
            ["assembly_id", "nm_id"], keep="first"
        )

    @staticmethod
    def _calculate_intervals(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Рассчитывает интервалы между операционными событиями.

        Бизнес-правило: интервалы считаются только при наличии обеих дат;
        отрицательные интервалы не скрываются и помечаются как аномалии.
        """

        dataframe = dataframe.copy()
        for column in ("wb_created_at", "shipped_at", "wb_status_sorted"):
            dataframe[column] = pd.to_datetime(dataframe[column], utc=True, errors="coerce")
        dataframe["hours_created_to_shipped"] = (
            dataframe["shipped_at"] - dataframe["wb_created_at"]
        ).dt.total_seconds() / 3600
        dataframe["order_processing_hours"] = dataframe["hours_created_to_shipped"]
        dataframe["hours_shipped_to_wb_sorted"] = (
            dataframe["wb_status_sorted"] - dataframe["shipped_at"]
        ).dt.total_seconds() / 3600
        dataframe["hours_created_to_wb_sorted"] = (
            dataframe["wb_status_sorted"] - dataframe["wb_created_at"]
        ).dt.total_seconds() / 3600

        dataframe["event_state"] = "нет данных о статусах"
        dataframe.loc[dataframe["service_task_found"].eq(False), "event_state"] = (
            "нет задания в FBS-сервисе"
        )
        dataframe.loc[
            dataframe["service_task_found"].eq(True) & dataframe["has_shipped"].eq(False),
            "event_state",
        ] = "не отгружен нашей системой"
        dataframe.loc[
            dataframe["has_shipped"] & dataframe["has_wb_sorted"].eq(False),
            "event_state",
        ] = "отгружен, но не отсортирован WB"
        dataframe.loc[
            dataframe["has_shipped"] & dataframe["has_wb_sorted"],
            "event_state",
        ] = "отгружен и отсортирован WB"
        dataframe.loc[
            dataframe.get(
                "acceptance_act_found",
                pd.Series(False, index=dataframe.index),
            ).fillna(False),
            "event_state",
        ] = "сформирован акт приемки WB"

        dataframe["event_data_quality"] = "ok"
        invalid = (
            dataframe["hours_created_to_shipped"].lt(0)
            | dataframe["hours_shipped_to_wb_sorted"].lt(0)
            | dataframe["hours_created_to_wb_sorted"].lt(0)
        )
        dataframe.loc[invalid, "event_data_quality"] = "аномальная последовательность дат"
        dataframe.loc[
            dataframe["service_task_found"].eq(False), "event_data_quality"
        ] = "нет задания в FBS-сервисе"
        dataframe.loc[
            dataframe["service_task_found"].eq(True)
            & dataframe["status_row_count"].fillna(0).eq(0),
            "event_data_quality",
        ] = "нет строк статусов"
        return dataframe

    @staticmethod
    def _classify(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Назначает этап и вероятное подразделение по наблюдаемым признакам.

        Бизнес-правило: автоматическая классификация не называет причину
        доказанной. Для нецелевых оснований сохраняется финансовая строка без
        ложного назначения подразделения.
        """

        result = dataframe.copy()
        reason = result["bonus_type_name"].fillna("").str.lower()
        focus = reason.str.contains(TARGET_PENALTY_MARKER, na=False)
        wrong_item = reason.str.contains("отправка товара отличного", na=False)
        stock = pd.to_numeric(result["stock_on_order_date"], errors="coerce")
        zero_stock = stock.le(0)
        positive_stock = stock.gt(0)
        has_task = result["service_task_found"].fillna(False)
        shipped = result["has_shipped"].fillna(False)
        sorted_wb = result["has_wb_sorted"].fillna(False)

        result["is_focus_penalty"] = focus
        result["responsibility_stage"] = "не в фокусе root-cause"
        result["probable_responsible_department"] = "не классифицируется"
        result["classification_confidence"] = "not_applicable"
        result["classification_reason"] = "Основание не относится к фокусу невыполненных заказов."

        target = focus
        result.loc[target, "classification_confidence"] = "low"
        result.loc[target, "responsibility_stage"] = "данных недостаточно"
        result.loc[target, "probable_responsible_department"] = "ручная проверка"
        result.loc[target, "classification_reason"] = "Недостаточно операционных событий для вывода."

        result.loc[target & wrong_item, "responsibility_stage"] = "комплектация до отгрузки"
        result.loc[target & wrong_item, "probable_responsible_department"] = (
            "склад / комплектация"
        )
        result.loc[target & wrong_item, "classification_confidence"] = "high"
        result.loc[target & wrong_item, "classification_reason"] = (
            "Основание штрафа указывает на отправку отличного товара."
        )

        no_ship = target & ~wrong_item & has_task & ~shipped
        result.loc[no_ship & zero_stock, "responsibility_stage"] = "до отгрузки: дефицит"
        result.loc[no_ship & zero_stock, "probable_responsible_department"] = (
            "планирование / закупки / остатки"
        )
        result.loc[no_ship & zero_stock, "classification_confidence"] = "high"
        result.loc[no_ship & zero_stock, "classification_reason"] = (
            "Заказ не отгружен, а остаток на дату заказа нулевой или отрицательный."
        )

        result.loc[no_ship & positive_stock, "responsibility_stage"] = "до отгрузки: операция"
        result.loc[no_ship & positive_stock, "probable_responsible_department"] = (
            "склад / операционный блок FBS"
        )
        result.loc[no_ship & positive_stock, "classification_confidence"] = "medium"
        result.loc[no_ship & positive_stock, "classification_reason"] = (
            "Заказ не отгружен при положительном остатке."
        )

        after_ship = target & ~wrong_item & shipped & ~sorted_wb
        result.loc[after_ship, "responsibility_stage"] = "после отгрузки до сортировки WB"
        result.loc[after_ship, "probable_responsible_department"] = (
            "склад / передача в WB / WB"
        )
        result.loc[after_ship, "classification_confidence"] = "low"
        result.loc[after_ship, "classification_reason"] = (
            "Наша система зафиксировала отгрузку, но сортировка WB не подтверждена."
        )

        after_sort = target & ~wrong_item & shipped & sorted_wb
        result.loc[after_sort, "responsibility_stage"] = "после сортировки WB"
        result.loc[after_sort, "probable_responsible_department"] = "WB / спорный кейс"
        result.loc[after_sort, "classification_confidence"] = "low"
        result.loc[after_sort, "classification_reason"] = (
            "Заказ отгружен и отсортирован WB, требуется проверка основания штрафа."
        )

        has_acceptance = result.get(
            "acceptance_act_found",
            pd.Series(False, index=result.index),
        ).fillna(False)
        after_acceptance = target & ~wrong_item & has_acceptance
        result.loc[after_acceptance, "responsibility_stage"] = (
            "после формирования акта приемки WB"
        )
        result.loc[after_acceptance, "probable_responsible_department"] = "WB / спорный кейс"
        result.loc[after_acceptance, "classification_confidence"] = "low"
        result.loc[after_acceptance, "classification_reason"] = (
            "По заказу площадка сформировала акт приемки; нужно проверить причину штрафа."
        )

        result.loc[target & ~has_task, "responsibility_stage"] = "до получения задания"
        result.loc[target & ~has_task, "probable_responsible_department"] = (
            "техблок / ручная проверка"
        )
        result.loc[target & ~has_task, "classification_confidence"] = "low"
        result.loc[target & ~has_task, "classification_reason"] = (
            "Сборочное задание не найдено в FBS-сервисе."
        )
        return result

    def build_dataframe(self) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Строит обогащенную детализацию и контрольные показатели.

        Бизнес-сценарий: метод выполняет полный read-only сбор финансовых
        штрафов, событий FBS и остатков, после чего возвращает готовую к
        upsert витрину и значения, которые должны быть проверены до записи.
        """

        financial = self._load_financial_penalties()
        if financial.empty:
            return financial, {"total_penalty": Decimal("0"), "focus_penalty": Decimal("0")}

        assembly_ids = financial["assembly_id"].dropna().astype(int).unique().tolist()
        fbs = self._load_fbs_orders(assembly_ids)
        acceptance = self._load_acceptance_acts(assembly_ids)
        stock = self._load_stock_snapshot(financial)
        dataframe = financial.merge(fbs, how="left", on="assembly_id", validate="many_to_one")
        dataframe = dataframe.merge(
            acceptance,
            how="left",
            on="assembly_id",
            validate="many_to_one",
        )
        dataframe = dataframe.merge(
            stock,
            how="left",
            on=["assembly_id", "nm_id"],
            validate="many_to_one",
        )
        dataframe["acceptance_act_found"] = (
            dataframe["acceptance_act_found"].fillna(False).astype(bool)
        )
        dataframe["service_task_found"] = dataframe["service_task_found"].fillna(False).astype(bool)
        dataframe["has_shipped"] = dataframe["has_shipped"].fillna(False).astype(bool)
        dataframe["has_wb_sorted"] = dataframe["has_wb_sorted"].fillna(False).astype(bool)
        dataframe["stock_state"] = "unknown_stock"
        dataframe.loc[dataframe["stock_on_order_date"].gt(0), "stock_state"] = "positive_stock"
        dataframe.loc[dataframe["stock_on_order_date"].le(0), "stock_state"] = (
            "zero_or_negative_stock"
        )
        dataframe = self._calculate_intervals(dataframe)
        dataframe = self._classify(dataframe)
        dataframe["loaded_at"] = pd.Timestamp.now(tz=MOSCOW_TZ)
        dataframe = dataframe[DETAIL_COLUMNS].copy()

        total_penalty = Decimal(str(round(float(financial["penalty"].sum()), 2)))
        persisted_total = Decimal(str(round(float(dataframe["penalty"].sum()), 2)))
        if total_penalty != persisted_total:
            raise RuntimeError(
                "Контрольная сумма штрафов изменилась после обогащения: "
                f"до={total_penalty}, после={persisted_total}"
            )

        focus_mask = dataframe["is_focus_penalty"].eq(True)
        checks = {
            "total_penalty": total_penalty,
            "focus_penalty": Decimal(
                str(round(float(dataframe.loc[focus_mask, "penalty"].sum()), 2))
            ),
            "assembly_count_before": len(assembly_ids),
            "assembly_count_after": dataframe["assembly_id"].nunique(),
            "unmatched_fbs_orders": int((~dataframe["service_task_found"]).groupby(
                dataframe["assembly_id"]
            ).any().sum()),
            "missing_status_orders": int(
                dataframe.loc[dataframe["service_task_found"], "status_row_count"]
                .fillna(0)
                .eq(0)
                .groupby(dataframe.loc[dataframe["service_task_found"], "assembly_id"])
                .any()
                .sum()
            ),
            "inconsistent_event_orders": int(
                dataframe["event_data_quality"].eq("аномальная последовательность дат")
                .groupby(dataframe["assembly_id"])
                .any()
                .sum()
            ),
        }
        if checks["assembly_count_before"] != checks["assembly_count_after"]:
            raise RuntimeError("После обогащения изменилось число уникальных сборочных заданий.")
        logger.info(
            "Построена системная витрина штрафов | date_from=%s | date_to=%s | rows=%s | total=%.2f | focus=%.2f",
            self.date_from,
            self.date_to - timedelta(days=1),
            len(dataframe),
            float(total_penalty),
            float(checks["focus_penalty"]),
        )
        return dataframe, checks

    @staticmethod
    def _table_ddl() -> str:
        """Возвращает DDL детальной витрины и ее индексов.

        Бизнес-правило: уникальный ключ соответствует зерну аналитики и делает
        повторный запуск за месяц безопасным через upsert.
        """

        return f"""
        CREATE TABLE IF NOT EXISTS public.{DETAIL_TABLE} (
            analysis_month_msk date NOT NULL,
            realizationreport_id bigint,
            order_dt timestamptz,
            account text,
            sa_name text,
            subject_name text,
            bonus_type_name text NOT NULL,
            assembly_id bigint NOT NULL,
            nm_id bigint,
            penalty numeric(18, 2) NOT NULL,
            wild text,
            stock_on_order_date numeric(18, 2),
            stock_state text,
            acceptance_act_found boolean NOT NULL,
            acceptance_document_numbers text,
            acceptance_act_date date,
            acceptance_row_count integer,
            service_task_found boolean NOT NULL,
            wb_created_at timestamptz,
            fbs_real_status text,
            status_row_count integer,
            shipped_at timestamptz,
            wb_status_sorted timestamptz,
            has_shipped boolean NOT NULL,
            has_wb_sorted boolean NOT NULL,
            hours_created_to_shipped numeric(18, 3),
            order_processing_hours numeric(18, 3),
            hours_shipped_to_wb_sorted numeric(18, 3),
            hours_created_to_wb_sorted numeric(18, 3),
            event_state text,
            event_data_quality text,
            responsibility_stage text,
            probable_responsible_department text,
            classification_confidence text,
            classification_reason text,
            is_focus_penalty boolean NOT NULL,
            loaded_at timestamptz NOT NULL,
            CONSTRAINT uq_{DETAIL_TABLE}_grain
                UNIQUE (analysis_month_msk, assembly_id, bonus_type_name, nm_id)
        );
        ALTER TABLE public.{DETAIL_TABLE}
            ADD COLUMN IF NOT EXISTS acceptance_act_found boolean NOT NULL DEFAULT FALSE;
        ALTER TABLE public.{DETAIL_TABLE}
            ADD COLUMN IF NOT EXISTS acceptance_document_numbers text;
        ALTER TABLE public.{DETAIL_TABLE}
            ADD COLUMN IF NOT EXISTS acceptance_act_date date;
        ALTER TABLE public.{DETAIL_TABLE}
            ADD COLUMN IF NOT EXISTS acceptance_row_count integer;
        ALTER TABLE public.{DETAIL_TABLE}
            ADD COLUMN IF NOT EXISTS order_processing_hours numeric(18, 3);
        UPDATE public.{DETAIL_TABLE}
        SET order_processing_hours = hours_created_to_shipped
        WHERE order_processing_hours IS NULL
          AND hours_created_to_shipped IS NOT NULL;
        CREATE INDEX IF NOT EXISTS ix_{DETAIL_TABLE}_month
            ON public.{DETAIL_TABLE} (analysis_month_msk);
        CREATE INDEX IF NOT EXISTS ix_{DETAIL_TABLE}_assembly
            ON public.{DETAIL_TABLE} (assembly_id);
        """

    @staticmethod
    def _view_ddl() -> str:
        """Возвращает DDL управленческого агрегированного представления.

        Бизнес-правило: сводка считает сумму и уникальные заказы из уже
        проверенной детализации, поэтому не повторяет логику источников.
        """

        return f"""
        CREATE OR REPLACE VIEW public.{SUMMARY_VIEW} AS
        SELECT
            analysis_month_msk,
            bonus_type_name,
            is_focus_penalty,
            responsibility_stage,
            probable_responsible_department,
            classification_confidence,
            COUNT(DISTINCT assembly_id) AS orders_count,
            COUNT(*) AS detail_rows_count,
            SUM(penalty) AS penalty_sum,
            AVG(penalty) AS avg_penalty,
            COUNT(*) FILTER (WHERE service_task_found) AS matched_fbs_rows,
            COUNT(*) FILTER (WHERE has_shipped) AS shipped_rows,
            COUNT(*) FILTER (WHERE has_wb_sorted) AS wb_sorted_rows,
            COUNT(*) FILTER (WHERE acceptance_act_found) AS acceptance_act_rows
        FROM public.{DETAIL_TABLE}
        GROUP BY
            analysis_month_msk,
            bonus_type_name,
            is_focus_penalty,
            responsibility_stage,
            probable_responsible_department,
            classification_confidence;
        """

    def _ensure_objects(self) -> None:
        """Создает аналитическую таблицу, индексы и агрегированное view.

        Бизнес-правило: операция меняет только объекты аналитического контура
        на основной БД и не затрагивает исходные финансовые или FBS-таблицы.
        """

        with self.database_cls.get_engine().begin() as connection:
            connection.execute(text(self._table_ddl()))
            connection.execute(text(self._view_ddl()))
        logger.info(
            "Проверены объекты системной аналитики штрафов | table=%s | view=%s",
            DETAIL_TABLE,
            SUMMARY_VIEW,
        )

    @staticmethod
    def _records(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
        """Преобразует DataFrame в безопасные записи для PostgreSQL upsert.

        Бизнес-правило: pandas-пропуски должны передаваться как SQL NULL,
        иначе они могут попасть в PostgreSQL как неподдерживаемые `NaT` или
        `numpy.nan`.
        """

        records: list[dict[str, Any]] = []
        for row in dataframe.to_dict(orient="records"):
            clean: dict[str, Any] = {}
            for key, value in row.items():
                if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
                    clean[key] = None
                elif isinstance(value, pd.Timestamp):
                    clean[key] = value.to_pydatetime()
                elif hasattr(value, "item"):
                    clean[key] = value.item()
                else:
                    clean[key] = value
            records.append(clean)
        return records

    def _upsert(self, dataframe: pd.DataFrame) -> None:
        """Идемпотентно записывает детали в основную PostgreSQL-витрину.

        Бизнес-правило: повторный запуск одного месяца обновляет результат по
        тому же зерну, но не создает дубли и не удаляет строки других месяцев.
        """

        if dataframe.empty:
            logger.info("Запись витрины пропущена: за период нет ненулевых штрафов.")
            return
        table = MetaData()
        table.reflect(bind=self.database_cls.get_engine(), only=[DETAIL_TABLE], schema="public")
        target = table.tables[f"public.{DETAIL_TABLE}"]
        records = self._records(dataframe)
        unique_columns = ["analysis_month_msk", "assembly_id", "bonus_type_name", "nm_id"]
        with self.database_cls.get_engine().begin() as connection:
            for start in range(0, len(records), 1000):
                batch = records[start:start + 1000]
                statement = insert(target).values(batch)
                updates = {
                    column.name: getattr(statement.excluded, column.name)
                    for column in target.columns
                    if column.name not in unique_columns
                }
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=unique_columns,
                        set_=updates,
                    )
                )
        logger.info("Витрина штрафов записана в PostgreSQL | rows=%s", len(records))

    def _clear_period(self) -> None:
        """Удаляет текущий период перед полной пересборкой витрины.

        Бизнес-правило: скользящее обновление должно отражать не только новые
        и измененные строки, но и исчезнувшие или обнуленные штрафы. Удаление
        ограничено московским интервалом текущего запуска и не затрагивает
        исходные таблицы или записи за пределами периода.
        """

        query = text(
            f"""
            DELETE FROM public.{DETAIL_TABLE}
            WHERE (order_dt AT TIME ZONE 'Europe/Moscow')::date >= :date_from
              AND (order_dt AT TIME ZONE 'Europe/Moscow')::date < :date_to
            """
        )
        with self.database_cls.get_engine().begin() as connection:
            result = connection.execute(
                query,
                {"date_from": self.date_from, "date_to": self.date_to},
            )
        logger.info(
            "Очищены строки витрины перед пересборкой | date_from=%s | date_to=%s | rows=%s",
            self.date_from,
            self.date_to - timedelta(days=1),
            result.rowcount,
        )

    def run(self, ensure_objects: bool = True) -> PenaltiesAnalysisResult:
        """Запускает полный сценарий системной аналитики штрафов за период.

        Полный бизнес-сценарий: читает все штрафы основной БД по московскому
        периоду, обогащает их заданиями и статусами FBS, считает интервалы,
        классифицирует фокусные основания, проверяет суммы, пересобирает
        выбранное окно и записывает только аналитическую витрину.
        """

        dataframe, checks = self.build_dataframe()
        if ensure_objects:
            self._ensure_objects()
        self._clear_period()
        self._upsert(dataframe)
        return PenaltiesAnalysisResult(
            month_start=self.month_start,
            date_from=self.date_from,
            date_to=self.date_to - timedelta(days=1),
            rolling_window=self.rolling_window,
            rows_written=len(dataframe),
            total_penalty=checks["total_penalty"],
            focus_penalty=checks["focus_penalty"],
            unmatched_fbs_orders=checks.get("unmatched_fbs_orders", 0),
            missing_status_orders=checks.get("missing_status_orders", 0),
            inconsistent_event_orders=checks.get("inconsistent_event_orders", 0),
        )


def run_system_penalties_analysis(
    month_start: date | str | None = None,
    rolling_days: int = 28,
) -> PenaltiesAnalysisResult:
    """Запускает системную аналитику штрафов за выбранный период.

    Бизнес-сценарий: без `month_start` entrypoint обновляет последние 28
    календарных дней в московской таймзоне; при переданном месяце выполняется
    полная пересборка этого месяца. В обоих случаях создаются или обновляются
    аналитические объекты, выполняется финансовая сверка и возвращается
    краткий результат оператору.
    """

    return SystemPenaltiesAnalyzer(
        month_start=month_start,
        rolling_days=rolling_days,
    ).run()
