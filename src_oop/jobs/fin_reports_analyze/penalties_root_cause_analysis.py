from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src_oop.core.database import Database

logger = logging.getLogger(__name__)

# Названия WB-полей опираются на документацию финансовых отчетов WB:
# https://dev.wildberries.ru/openapi/financial-reports-and-accounting/
RUSSIAN_COLUMN_NAMES: dict[str, str] = {
    "assembly_id": "Номер сборочного задания",
    "nm_id": "Артикул WB",
    "order_date": "Дата заказа (МСК)",
    "penalty": "Сумма штрафа",
    "bonus_type_name": "Обоснование удержания",
    "account": "Личный кабинет",
    "srid": "SRID",
    "subject_name": "Предмет",
    "wild": "Артикул продавца",
    "our_status": "Наш статус",
    "wb_status": "Статус WB",
    "supplier_status": "Статус поставщика",
    "supply_id": "Номер поставки",
    "smv_order_date": "Дата заказа из статусной модели",
    "hours_to_wb_scan": "Часов до сканирования стикера WB",
    "status_received_at": "Статус получен в БД",
    "stock_on_order_date": "Остаток на дату заказа",
    "acceptance_status": "Статус приемки WB",
    "act_date": "Дата акта приемки",
    "first_log_at": "Первое событие в логе",
    "last_log_at": "Последнее событие в логе",
    "log_rows": "Количество событий в логе",
    "has_status_new": "Есть статус NEW",
    "has_status_in_technical_supply": "Есть статус IN_TECHNICAL_SUPPLY",
    "has_status_in_hanging_supply": "Есть статус IN_HANGING_SUPPLY",
    "has_status_in_final_supply": "Есть статус IN_FINAL_SUPPLY",
    "has_status_delivered": "Есть статус DELIVERED",
    "has_status_fictitious_delivered": "Есть статус FICTITIOUS_DELIVERED",
    "log_statuses": "Статусы из лога заказа",
    "root_cause_group": "Группа первопричины",
    "responsible_team": "Вероятно ответственное подразделение",
    "confidence": "Уровень уверенности",
    "recommendation": "Рекомендация",
    "stock_state": "Состояние остатка",
    "supply_movement_state": "Состояние движения по поставке",
    "orders_count": "Количество заказов",
    "penalty_sum": "Сумма штрафов",
    "avg_penalty": "Средний штраф",
}


@dataclass(slots=True)
class PenaltiesAnalysisConfig:
    """Настройки анализа штрафов по невыполненным заказам.

    Бизнес-сценарий: аналитик запускает разбор штрафов за выбранный месяц, чтобы
    понять первопричину по каждому заказу, назначить вероятный ответственный блок
    и выгрузить результат в понятный Excel для ручной проверки и управленческих
    выводов.
    """

    month_start: str
    output_dir: Path
    output_suffix: str = ""

    @property
    def month_end(self) -> pd.Timestamp:
        """Возвращает правую границу месяца для SQL-фильтра и подписей в отчете.

        Бизнес-правило: анализ должен охватывать полный календарный месяц, а не
        только первый день месяца или скользящий период.
        """

        month_start_ts = pd.Timestamp(self.month_start)
        return month_start_ts + pd.offsets.MonthBegin(1)

    @property
    def safe_month_label(self) -> str:
        """Готовит безопасную метку месяца для имени итогового файла.

        В отчете дата используется в имени файла, чтобы разные месяцы не
        перезаписывали друг друга и их можно было сравнивать между собой.
        """

        return pd.Timestamp(self.month_start).strftime("%Y_%m")

    @property
    def output_path(self) -> Path:
        """Возвращает путь к итоговому Excel-файлу с аналитикой.

        Бизнес-правило: детальная витрина и сводки должны сохраняться в одном
        файле, чтобы руководитель и аналитик работали с одинаковым источником.
        """

        suffix = f"_{self.output_suffix}" if self.output_suffix else ""
        return self.output_dir / f"penalties_root_cause_analysis_{self.safe_month_label}{suffix}.xlsx"


class PenaltiesRootCauseAnalyzer:
    """Строит pandas-аналитику штрафов по невыполненным заказам.

    Сценарий объединяет финансовые штрафы, статусную модель заказа, историю
    логов движения заказа, исторические остатки и дату акта приемки WB. На
    основе этих данных формируется витрина одного штрафного заказа и сводки по
    вероятным причинам и ответственным подразделениям.
    """

    def __init__(
        self,
        config: PenaltiesAnalysisConfig,
        database_cls: type[Database] = Database,
    ) -> None:
        """Подключает настройки периода и общий слой чтения из PostgreSQL."""

        self.config = config
        self.database_cls = database_cls

    @staticmethod
    def _chunk_list(values: list[object], chunk_size: int) -> list[list[object]]:
        """Разбивает список на небольшие батчи для стабильного чтения из БД.

        Бизнес-правило: тяжелые источники вроде статусной модели могут отвечать
        медленно на один большой `ANY`, поэтому безопаснее читать их небольшими
        порциями и затем собирать итог в pandas.
        """

        if chunk_size <= 0:
            raise ValueError("chunk_size должен быть положительным числом.")
        return [values[index:index + chunk_size] for index in range(0, len(values), chunk_size)]

    def _load_penalties_base(self) -> pd.DataFrame:
        """Загружает базовый реестр штрафных заказов месяца.

        Бизнес-правило: перед любым join штрафы агрегируются до уровня одного
        заказа, чтобы статусные таблицы и логи не раздували сумму штрафов при
        множественных совпадениях.
        """

        query = text(
            """
            WITH penalties AS (
                SELECT
                    f.assembly_id,
                    f.nm_id,
                    MIN((f.order_dt AT TIME ZONE 'Europe/Moscow')::date) AS order_date,
                    SUM(f.penalty) AS penalty,
                    STRING_AGG(DISTINCT f.bonus_type_name, ' | ') AS bonus_type_name,
                    MAX(f.account) AS account,
                    MAX(f.srid) AS srid,
                    MAX(f.subject_name) AS subject_name
                FROM public.daily_fin_reports_full f
                WHERE f.bonus_type_name ILIKE '%Невыполненный заказ%'
                  AND f.penalty != 0
                  AND f.assembly_id != 0
                  AND (f.order_dt AT TIME ZONE 'Europe/Moscow')::date >= :month_start
                  AND (f.order_dt AT TIME ZONE 'Europe/Moscow')::date < :month_end
                GROUP BY f.assembly_id, f.nm_id
            )
            SELECT
                p.assembly_id,
                p.nm_id,
                p.order_date,
                p.penalty,
                p.bonus_type_name,
                p.account,
                p.srid,
                p.subject_name,
                a.local_vendor_code AS wild
            FROM penalties p
            LEFT JOIN public.article a
                ON a.nm_id = p.nm_id
            """
        )
        params = {
            "month_start": pd.Timestamp(self.config.month_start).date().isoformat(),
            "month_end": self.config.month_end.date().isoformat(),
        }
        df = self.database_cls.read_sql_to_dataframe(query, params=params)
        if not df.empty:
            df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.normalize()
            df["wild"] = df["wild"].astype("string").str.strip()
        logger.info("Загружена базовая витрина штрафов | rows=%s", len(df))
        return df

    def _load_status_snapshot(self, assembly_ids: list[int]) -> pd.DataFrame:
        """Загружает статусную модель только для нужных штрафных заказов.

        Бизнес-правило: вместо полного join ко всей `status_model_view` читаем
        только нужные `assembly_id`, чтобы уменьшить время ответа и снизить риск
        зависания тяжелой view.
        """

        if not assembly_ids:
            return pd.DataFrame(
                columns=[
                    "assembly_id",
                    "our_status",
                    "wb_status",
                    "supplier_status",
                    "supply_id",
                    "smv_order_date",
                    "hours_to_wb_scan",
                    "status_received_at",
                ]
            )

        query = text(
            """
            SELECT DISTINCT ON (smv."Номер_СЗ")
                smv."Номер_СЗ" AS assembly_id,
                smv."Наш_статус" AS our_status,
                smv."Статус_ВБ" AS wb_status,
                smv."Статус_поставщика" AS supplier_status,
                smv."Номер_поставки" AS supply_id,
                smv."Дата_заказа" AS smv_order_date,
                smv."Часов прошло" AS hours_to_wb_scan,
                smv."Получен_в_БД" AS status_received_at
            FROM public.status_model_view smv
            WHERE smv."Номер_СЗ" = ANY(:assembly_ids)
            ORDER BY smv."Номер_СЗ", smv."Получен_в_БД" DESC NULLS LAST
            """
        )
        dataframes: list[pd.DataFrame] = []
        for batch_index, batch_ids in enumerate(self._chunk_list(assembly_ids, 50), start=1):
            logger.info(
                "Загрузка статусной модели по батчу | batch_index=%s | batch_size=%s",
                batch_index,
                len(batch_ids),
            )
            batch_df = self.database_cls.read_sql_to_dataframe(
                query,
                params={"assembly_ids": batch_ids},
            )
            dataframes.append(batch_df)

        df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
        if not df.empty:
            df["smv_order_date"] = pd.to_datetime(df["smv_order_date"], errors="coerce").dt.normalize()
            df["status_received_at"] = pd.to_datetime(df["status_received_at"], errors="coerce")
        df = df.drop_duplicates(subset=["assembly_id"], keep="first")
        logger.info("Загружен статусный срез по штрафным заказам | rows=%s", len(df))
        return df

    def _load_stock_snapshot(self, wilds: list[str], month_start: str, month_end: str) -> pd.DataFrame:
        """Загружает снимки остатков только по нужным SKU и периоду анализа.

        Бизнес-правило: для root-cause нам нужен остаток только на даты штрафных
        заказов текущего месяца, а не вся историческая таблица остатков.
        """

        filtered_wilds = [wild for wild in wilds if wild]
        if not filtered_wilds:
            return pd.DataFrame(columns=["wild", "transaction_date", "stock_on_order_date"])

        query = text(
            """
            SELECT
                hs.wild,
                hs.transaction_date,
                hs.end_of_day_balance AS stock_on_order_date
            FROM public.historical_stocks_fbs_service hs
            WHERE hs.wild = ANY(:wilds)
              AND hs.transaction_date >= :month_start
              AND hs.transaction_date < :month_end
            """
        )
        dataframes: list[pd.DataFrame] = []
        for batch_index, batch_wilds in enumerate(self._chunk_list(filtered_wilds, 200), start=1):
            logger.info(
                "Загрузка исторических остатков по батчу | batch_index=%s | batch_size=%s",
                batch_index,
                len(batch_wilds),
            )
            params = {
                "wilds": batch_wilds,
                "month_start": month_start,
                "month_end": month_end,
            }
            batch_df = self.database_cls.read_sql_to_dataframe(query, params=params)
            dataframes.append(batch_df)

        df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
        if not df.empty:
            df["wild"] = df["wild"].astype("string").str.strip()
            df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.normalize()
            df["stock_on_order_date"] = pd.to_numeric(df["stock_on_order_date"], errors="coerce")
        logger.info("Загружены исторические остатки по штрафным SKU | rows=%s", len(df))
        return df

    def _load_acceptance_snapshot(self, assembly_ids: list[int]) -> pd.DataFrame:
        """Загружает данные актов приемки WB только для штрафных заказов.

        Бизнес-правило: дата акта приемки нужна как внешний признак прохождения
        заказа через WB. Источник — `acceptance_fbs_acts_new`, где по одному
        `order_number` может быть несколько строк, поэтому на уровне заказа
        берется максимальная дата акта.
        """

        if not assembly_ids:
            return pd.DataFrame(columns=["assembly_id", "acceptance_status", "act_date"])

        query = text(
            """
            SELECT
                af.order_number::bigint AS assembly_id,
                STRING_AGG(DISTINCT af.document, ' | ' ORDER BY af.document) AS acceptance_status,
                MAX(af.date) AS act_date
            FROM public.acceptance_fbs_acts_new af
            WHERE af.order_number ~ '^[0-9]+$'
              AND af.order_number::bigint = ANY(:assembly_ids)
            GROUP BY af.order_number::bigint
            """
        )
        dataframes: list[pd.DataFrame] = []
        for batch_index, batch_ids in enumerate(self._chunk_list(assembly_ids, 100), start=1):
            logger.info(
                "Загрузка приемки WB по батчу | batch_index=%s | batch_size=%s",
                batch_index,
                len(batch_ids),
            )
            batch_df = self.database_cls.read_sql_to_dataframe(
                query,
                params={"assembly_ids": batch_ids},
            )
            dataframes.append(batch_df)

        df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
        if not df.empty:
            df["act_date"] = pd.to_datetime(df["act_date"], errors="coerce").dt.normalize()
        df = df.drop_duplicates(subset=["assembly_id"], keep="first")
        logger.info("Загружены данные приемки WB по штрафным заказам | rows=%s", len(df))
        return df

    def _load_order_logs(self, assembly_ids: list[int]) -> pd.DataFrame:
        """Загружает агрегированные логи движения штрафных заказов.

        Бизнес-правило: для root-cause анализа не нужен каждый лог отдельно;
        достаточно агрегатов по заказу, которые показывают, дошел ли заказ до
        технической поставки, зависал ли в поставке и был ли финально доставлен.
        """

        if not assembly_ids:
            return pd.DataFrame(columns=["assembly_id"])

        query = text(
            """
            SELECT
                osl.order_id AS assembly_id,
                MIN(osl.created_at) AS first_log_at,
                MAX(osl.created_at) AS last_log_at,
                COUNT(*) AS log_rows,
                BOOL_OR(osl.status = 'NEW') AS has_status_new,
                BOOL_OR(osl.status = 'IN_TECHNICAL_SUPPLY') AS has_status_in_technical_supply,
                BOOL_OR(osl.status = 'IN_HANGING_SUPPLY') AS has_status_in_hanging_supply,
                BOOL_OR(osl.status = 'IN_FINAL_SUPPLY') AS has_status_in_final_supply,
                BOOL_OR(osl.status = 'DELIVERED') AS has_status_delivered,
                BOOL_OR(osl.status = 'FICTITIOUS_DELIVERED') AS has_status_fictitious_delivered,
                STRING_AGG(DISTINCT osl.status, ' | ' ORDER BY osl.status) AS log_statuses
            FROM public.order_status_log osl
            WHERE osl.order_id = ANY(:assembly_ids)
            GROUP BY osl.order_id
            """
        )
        dataframes: list[pd.DataFrame] = []
        for batch_index, batch_ids in enumerate(self._chunk_list(assembly_ids, 100), start=1):
            logger.info(
                "Загрузка логов заказов по батчу | batch_index=%s | batch_size=%s",
                batch_index,
                len(batch_ids),
            )
            batch_df = self.database_cls.read_sql_to_dataframe(
                query,
                params={"assembly_ids": batch_ids},
            )
            dataframes.append(batch_df)

        df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
        if not df.empty:
            df["first_log_at"] = pd.to_datetime(df["first_log_at"], errors="coerce")
            df["last_log_at"] = pd.to_datetime(df["last_log_at"], errors="coerce")
        df = df.drop_duplicates(subset=["assembly_id"], keep="first")
        logger.info("Загружены агрегированные логи заказов | rows=%s", len(df))
        return df

    @staticmethod
    def _prepare_merge_keys(df: pd.DataFrame) -> pd.DataFrame:
        """Нормализует ключи соединения перед merge в pandas.

        Бизнес-правило: строки штрафов не должны теряться из-за разного типа дат
        или служебных пробелов в `wild`. Для анализа важнее стабильное и
        воспроизводимое соединение, чем попытка полагаться на неявные приведения
        типов со стороны pandas.
        """

        prepared_df = df.copy()
        if "wild" in prepared_df.columns:
            prepared_df["wild"] = prepared_df["wild"].astype("string").str.strip()
        if "order_date" in prepared_df.columns:
            prepared_df["order_date"] = pd.to_datetime(
                prepared_df["order_date"],
                errors="coerce",
            ).dt.normalize()
        if "transaction_date" in prepared_df.columns:
            prepared_df["transaction_date"] = pd.to_datetime(
                prepared_df["transaction_date"],
                errors="coerce",
            ).dt.normalize()
        return prepared_df

    @staticmethod
    def _prepare_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Нормализует числовые поля для дальнейшей классификации.

        Бизнес-правило: штрафы, часы и остатки участвуют в правилах отнесения
        причины, поэтому пропуски и строки нужно привести к числам заранее.
        """

        prepared_df = df.copy()
        for column_name in ("penalty", "hours_to_wb_scan", "stock_on_order_date"):
            prepared_df[column_name] = pd.to_numeric(
                prepared_df.get(column_name),
                errors="coerce",
            )
        return prepared_df

    @staticmethod
    def _normalize_boolean_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Заполняет булевы признаки логов безопасными значениями.

        Бизнес-правило: отсутствие логов не должно ломать классификацию; в таком
        случае признаки событий считаются ложными, а кейс уходит в низкую
        уверенность.
        """

        prepared_df = df.copy()
        boolean_columns = [
            "has_status_new",
            "has_status_in_technical_supply",
            "has_status_in_hanging_supply",
            "has_status_in_final_supply",
            "has_status_delivered",
            "has_status_fictitious_delivered",
        ]
        for column_name in boolean_columns:
            prepared_df[column_name] = prepared_df.get(column_name).fillna(False).astype(bool)
        return prepared_df

    @staticmethod
    def _classify_row(row: pd.Series) -> pd.Series:
        """Классифицирует один штрафной заказ по причине и зоне ответственности.

        Бизнес-логика:
        - явные штрафы за отправку не того товара относятся к комплектации/складу;
        - отмены продавцом при нулевом остатке указывают на дефицит или ошибку
          управления остатками;
        - положительный остаток без движения до доставки чаще говорит об
          операционной проблеме FBS-склада или синхронизации остатков;
        - наличие поставочных/доставочных статусов при штрафе переносит кейс в
          спорную или внешнюю зону WB, потому что заказ уже вышел за рамки
          простого отсутствия товара.
        """

        bonus_type = str(row.get("bonus_type_name") or "")
        stock_value = row.get("stock_on_order_date")
        has_stock = pd.notna(stock_value) and stock_value > 0
        zero_stock = pd.notna(stock_value) and stock_value <= 0
        reached_supply = bool(
            row.get("has_status_in_technical_supply")
            or row.get("has_status_in_hanging_supply")
            or row.get("has_status_in_final_supply")
            or row.get("has_status_delivered")
            or row.get("has_status_fictitious_delivered")
            or pd.notna(row.get("act_date"))
        )

        if "отправка товара отличного от заявленного" in bonus_type.lower():
            return pd.Series(
                {
                    "root_cause_group": "Ошибка комплектации / отправлен не тот товар",
                    "responsible_team": "Склад / комплектовка",
                    "confidence": "high",
                    "recommendation": "Проверить комплектовку, маркировку и контроль соответствия заказа карточке.",
                }
            )

        if "отмена продавцом" in bonus_type.lower():
            if zero_stock:
                return pd.Series(
                    {
                        "root_cause_group": "Отмена продавцом при нулевом остатке",
                        "responsible_team": "Планирование / закупки / управление остатками",
                        "confidence": "high",
                        "recommendation": "Проверить причины дефицита и корректность обновления доступного остатка на дату заказа.",
                    }
                )
            if has_stock and not reached_supply:
                return pd.Series(
                    {
                        "root_cause_group": "Отмена продавцом при положительном остатке без движения в поставку",
                        "responsible_team": "Склад / операционный блок FBS",
                        "confidence": "medium",
                        "recommendation": "Проверить, почему заказ не был собран или передан в поставку при наличии остатка.",
                    }
                )
            if has_stock and reached_supply:
                return pd.Series(
                    {
                        "root_cause_group": "Отмена продавцом после движения заказа по логам",
                        "responsible_team": "Спорный кейс: склад / техблок / WB",
                        "confidence": "low",
                        "recommendation": "Нужна ручная проверка цепочки статусов и причины отмены, так как заказ уже имел движение.",
                    }
                )

        if zero_stock:
            return pd.Series(
                {
                    "root_cause_group": "Невыполненный заказ при нулевом остатке",
                    "responsible_team": "Планирование / закупки / управление остатками",
                    "confidence": "medium",
                    "recommendation": "Проверить дефицит и своевременность обновления виртуального остатка.",
                }
            )

        if has_stock and reached_supply:
            return pd.Series(
                {
                    "root_cause_group": "Заказ дошел до поставки или приемки, но все равно получил штраф",
                    "responsible_team": "Спорный кейс / WB / логистика маркетплейса",
                    "confidence": "low",
                    "recommendation": "Проверить правила начисления штрафа, приемку WB и финальную причину в кабинете.",
                }
            )

        if has_stock and not reached_supply:
            return pd.Series(
                {
                    "root_cause_group": "Невыполненный заказ при положительном остатке без подтвержденной поставки",
                    "responsible_team": "Склад / операционный блок FBS",
                    "confidence": "medium",
                    "recommendation": "Проверить сборку, передачу в поставку и внутренний контроль обработки заказов.",
                }
            )

        return pd.Series(
            {
                "root_cause_group": "Недостаточно признаков для уверенной классификации",
                "responsible_team": "Ручная проверка",
                "confidence": "low",
                "recommendation": "Проверить карточку заказа вручную и уточнить фактическую причину штрафа.",
            }
        )

    def build_detailed_report(self) -> pd.DataFrame:
        """Строит детальную витрину штрафов с вероятной причиной и ответственным блоком.

        Бизнес-сценарий: каждая строка итогового датафрейма соответствует одному
        штрафному заказу и пригодна для ручной сверки, фильтрации в Excel и
        последующей калибровки правил аналитики.
        """

        penalties_df = self._load_penalties_base()
        assembly_ids = penalties_df["assembly_id"].dropna().astype(int).unique().tolist()
        wilds = penalties_df["wild"].dropna().astype(str).unique().tolist()
        month_start = self.config.month_start
        month_end = self.config.month_end.date().isoformat()

        status_df = self._load_status_snapshot(assembly_ids=assembly_ids)
        logs_df = self._load_order_logs(assembly_ids=assembly_ids)
        stock_df = self._load_stock_snapshot(
            wilds=wilds,
            month_start=month_start,
            month_end=month_end,
        )
        acceptance_df = self._load_acceptance_snapshot(assembly_ids=assembly_ids)

        penalties_df = self._prepare_merge_keys(penalties_df)
        stock_df = self._prepare_merge_keys(stock_df)

        merged_df = penalties_df.merge(status_df, how="left", on="assembly_id")
        merged_df = merged_df.merge(
            stock_df,
            how="left",
            left_on=["wild", "order_date"],
            right_on=["wild", "transaction_date"],
        )
        merged_df = merged_df.merge(acceptance_df, how="left", on="assembly_id")
        merged_df = merged_df.merge(logs_df, how="left", on="assembly_id")
        merged_df = merged_df.drop(columns=["transaction_date"], errors="ignore")
        merged_df = self._prepare_numeric_columns(merged_df)
        merged_df = self._normalize_boolean_columns(merged_df)

        classification_df = merged_df.apply(self._classify_row, axis=1)
        detailed_df = pd.concat([merged_df, classification_df], axis=1)

        detailed_df["stock_state"] = pd.Series(pd.NA, index=detailed_df.index, dtype="object")
        detailed_df.loc[detailed_df["stock_on_order_date"].gt(0), "stock_state"] = "positive_stock"
        detailed_df.loc[detailed_df["stock_on_order_date"].le(0), "stock_state"] = "zero_or_negative_stock"
        detailed_df["stock_state"] = detailed_df["stock_state"].fillna("unknown_stock")

        detailed_df["supply_movement_state"] = "no_supply_movement"
        detailed_df.loc[
            detailed_df[
                [
                    "has_status_in_technical_supply",
                    "has_status_in_hanging_supply",
                    "has_status_in_final_supply",
                    "has_status_delivered",
                    "has_status_fictitious_delivered",
                ]
            ].any(axis=1),
            "supply_movement_state",
        ] = "has_supply_movement"
        detailed_df.loc[detailed_df["act_date"].notna(), "supply_movement_state"] = "has_wb_acceptance"

        detailed_df = detailed_df.sort_values(
            by=["penalty", "order_date"],
            ascending=[False, True],
        ).reset_index(drop=True)
        logger.info("Построена детальная витрина штрафов | rows=%s", len(detailed_df))
        return detailed_df

    @staticmethod
    def build_summary_by_responsibility(detailed_df: pd.DataFrame) -> pd.DataFrame:
        """Собирает сводку по вероятному ответственному подразделению.

        Бизнес-правило: для руководителя важнее не каждый заказ по отдельности, а
        сумма штрафов и число кейсов по зонам ответственности.
        """

        summary_df = (
            detailed_df.groupby(["responsible_team", "confidence"], dropna=False)
            .agg(
                orders_count=("assembly_id", "nunique"),
                penalty_sum=("penalty", "sum"),
                avg_penalty=("penalty", "mean"),
            )
            .reset_index()
            .sort_values(by=["penalty_sum", "orders_count"], ascending=[False, False])
        )
        return summary_df

    @staticmethod
    def build_summary_by_root_cause(detailed_df: pd.DataFrame) -> pd.DataFrame:
        """Собирает сводку по группам первопричин штрафов.

        Сводка показывает, какие сценарии реально формируют денежный риск, а не
        просто встречаются по количеству заказов.
        """

        summary_df = (
            detailed_df.groupby(["root_cause_group", "bonus_type_name"], dropna=False)
            .agg(
                orders_count=("assembly_id", "nunique"),
                penalty_sum=("penalty", "sum"),
            )
            .reset_index()
            .sort_values(by=["penalty_sum", "orders_count"], ascending=[False, False])
        )
        return summary_df

    @staticmethod
    def build_summary_by_stock_state(detailed_df: pd.DataFrame) -> pd.DataFrame:
        """Показывает, как штрафы распределяются по состоянию остатка на дату заказа.

        Бизнес-правило: это основной срез для разделения проблем дефицита и
        проблем операционного исполнения при наличии товара.
        """

        summary_df = (
            detailed_df.groupby(["stock_state", "responsible_team"], dropna=False)
            .agg(
                orders_count=("assembly_id", "nunique"),
                penalty_sum=("penalty", "sum"),
            )
            .reset_index()
            .sort_values(by=["penalty_sum", "orders_count"], ascending=[False, False])
        )
        return summary_df

    def export_to_excel(self, detailed_df: pd.DataFrame) -> Path:
        """Сохраняет детальную витрину и сводки в Excel.

        Бизнес-сценарий: один файл нужен для передачи результата коллегам без
        запуска Python и без ручной сборки дополнительных pivot-таблиц.
        """

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        responsibility_df = self.build_summary_by_responsibility(detailed_df)
        root_cause_df = self.build_summary_by_root_cause(detailed_df)
        stock_state_df = self.build_summary_by_stock_state(detailed_df)
        detailed_export_df = detailed_df.rename(columns=RUSSIAN_COLUMN_NAMES)
        responsibility_export_df = responsibility_df.rename(columns=RUSSIAN_COLUMN_NAMES)
        root_cause_export_df = root_cause_df.rename(columns=RUSSIAN_COLUMN_NAMES)
        stock_state_export_df = stock_state_df.rename(columns=RUSSIAN_COLUMN_NAMES)

        with pd.ExcelWriter(self.config.output_path, engine="openpyxl") as writer:
            detailed_export_df.to_excel(writer, sheet_name="Детализация", index=False)
            responsibility_export_df.to_excel(
                writer,
                sheet_name="Ответственные",
                index=False,
            )
            root_cause_export_df.to_excel(
                writer,
                sheet_name="Первопричины",
                index=False,
            )
            stock_state_export_df.to_excel(
                writer,
                sheet_name="Остатки",
                index=False,
            )

        logger.info("Excel-отчет по штрафам сохранен | path=%s", self.config.output_path)
        return self.config.output_path

    def run(self) -> Path:
        """Запускает полный сценарий построения root-cause аналитики штрафов.

        Полный бизнес-сценарий:
        1. Читает штрафные заказы за выбранный месяц.
        2. Обогащает их статусами, логами, остатками и приемкой.
        3. Классифицирует вероятную причину и ответственный блок.
        4. Сохраняет Excel со сводками для управленческого разбора.
        """

        detailed_df = self.build_detailed_report()
        return self.export_to_excel(detailed_df)


def run_penalties_root_cause_analysis(
    month_start: str = "2026-08-01",
    output_dir: str | None = None,
    output_suffix: str = "",
) -> Path:
    """Запускает анализ штрафов за месяц и возвращает путь к готовому Excel.

    Функция нужна как удобная точка входа для ручного запуска аналитики без
    встраивания в cron или реестр задач, пока бизнес-правила классификации еще
    калибруются.
    """

    resolved_output_dir = Path(output_dir) if output_dir else Path.cwd()
    config = PenaltiesAnalysisConfig(
        month_start=month_start,
        output_dir=resolved_output_dir,
        output_suffix=output_suffix,
    )
    analyzer = PenaltiesRootCauseAnalyzer(config=config)
    return analyzer.run()
