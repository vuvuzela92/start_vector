from __future__ import annotations

import logging
import time
from datetime import datetime

import gspread
import numpy as np
import pandas as pd
import requests
from gspread_dataframe import set_with_dataframe

from src.core.utils_gspread import safe_open_spreadsheet
from src_oop.jobs.bukh_docs.config import (
    WEEK_N_REDEEM_SHEET_TITLE,
    WEEK_N_REDEEM_TABLE_TITLE,
)
from src_oop.jobs.bukh_docs.models import WeekNRedeemRunResult
from src_oop.jobs.bukh_docs.week_n_redeem_repository import WeekNRedeemRepository

logger = logging.getLogger(__name__)

TRUNCATED_SQL_COLUMNS = {
    "Компенсация_скидки_по_программе_л": "Компенсация_скидки_по_программе_лояльности",
    "Сумма_удержанная_в_счёт_обеспечен": "Сумма_удержанная_в_счёт_обеспечения_организации_платежа",
    "Возмещение_за_выдачу_и_возврат_тов": "Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ",
}

RENAMED_COLUMNS = {
    "Всего_товара": "Всего_стоимость_реализованного_товара",
    "Всего_товара_БЕЗ_НДС": "Всего_стоимость_реализованного_товара_без_НДС",
    "Компенсации_ущерба": "Компенсации_ущерба",
    "Прочие_выплаты": "Прочие_выплаты",
    "Компенсация_скидки_по_программе_лояльности": "Компенсация_скидки_по_программе_лояльности",
    "Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ": "Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ",
}

GOOGLE_WRITE_RETRY_ATTEMPTS = 5
GOOGLE_WRITE_RETRY_DELAY_SECONDS = 10
GOOGLE_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class WeekNRedeemService:
    """Orchestration-слой для подготовки и выгрузки week_n_redeem."""

    def __init__(self, repository: WeekNRedeemRepository | None = None) -> None:
        self._repository = repository or WeekNRedeemRepository()

    def run(self, write_to_google: bool = True) -> WeekNRedeemRunResult:
        logger.info("Старт week_n_redeem job: write_to_google=%s", write_to_google)
        result = WeekNRedeemRunResult(status="success")

        try:
            raw_dataframe = self._repository.fetch_dataframe()
            result.sql_rows = len(raw_dataframe.index)
            result.accounts_in_sql = self._extract_accounts(raw_dataframe)
            result.unique_accounts = len(result.accounts_in_sql)
            result.missing_redeem_notifications_in_sql = (
                self._count_missing_redeem_notifications(raw_dataframe)
            )
            result.duplicate_rows_by_account_report = self._count_duplicate_rows(raw_dataframe)
            self._log_dataframe_snapshot("sql_result", raw_dataframe)

            processed_dataframe = self._prepare_dataframe(raw_dataframe)
            result.rows_after_processing = len(processed_dataframe.index)
            self._log_dataframe_snapshot("processed_result", processed_dataframe)

            dataframe_to_update = self._sort_dataframe_for_google(
                self._filter_dataframe(processed_dataframe)
            )
            result.rows_after_filter = len(dataframe_to_update.index)
            result.accounts_after_filter = self._extract_accounts(dataframe_to_update)
            result.missing_redeem_notifications_after_filter = (
                self._count_missing_redeem_notifications(dataframe_to_update)
            )
            self._log_dataframe_snapshot("filtered_result", dataframe_to_update)

            if write_to_google:
                self._write_to_google(dataframe_to_update)
        except Exception as error:
            logger.exception("Ошибка выполнения week_n_redeem: %s", error)
            result.status = "failed"
            result.errors.append(str(error))
            return result

        logger.info(
            "Завершен week_n_redeem job: status=%s sql_rows=%s rows_after_processing=%s rows_after_filter=%s unique_accounts=%s missing_redeem_sql=%s missing_redeem_filtered=%s duplicate_rows=%s",
            result.status,
            result.sql_rows,
            result.rows_after_processing,
            result.rows_after_filter,
            result.unique_accounts,
            result.missing_redeem_notifications_in_sql,
            result.missing_redeem_notifications_after_filter,
            result.duplicate_rows_by_account_report,
        )
        logger.info(
            "week_n_redeem accounts summary: sql_accounts=%s filtered_accounts=%s",
            result.accounts_in_sql,
            result.accounts_after_filter,
        )
        return result

    def _prepare_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Готовит витрину week_n_redeem перед выгрузкой в Google Sheets.

        Бизнес-сценарий объединяет данные еженедельных отчетов реализации,
        финансовых отчетов и уведомлений о выкупе, рассчитывает итоговые
        расходы и сумму к перечислению. В конце добавляется ``updated_at``,
        чтобы в таблице было видно фактическое время обновления выгрузки.
        """
        result = dataframe.copy()
        result = self._normalize_column_names(result)
        result = result.rename(columns=RENAMED_COLUMNS)
        self._validate_required_columns(result)
        result = result.sort_values(by=["Начало_периода", "account"], ascending=[True, True])

        result["Итого_расходы"] = np.where(
            result["Вознаграждение"] < 0,
            result["Вовзрат_выкупа"]
            + result["Возмещение расходов по перевозке"]
            + result["Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ"]
            + result["Штрафы"]
            + result["Прочие удержания"]
            + result["Удержания_в_пользу_третьих_лиц"]
            + result["Сумма_удержанная_в_счёт_обеспечения_организации_платежа"],
            result["Вовзрат_выкупа"]
            + result["Возмещение расходов по перевозке"]
            + result["Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ"]
            + result["Штрафы"]
            + result["Прочие удержания"]
            + result["Удержания_в_пользу_третьих_лиц"]
            + result["Сумма_удержанная_в_счёт_обеспечения_организации_платежа"]
            + result["Вознаграждение"],
        )

        result["К_перечислению_по_отчетам"] = (
            result["Всего_стоимость_реализованного_товара"]
            + result["Компенсации_ущерба"]
            + result["Прочие_выплаты"]
            + result["Выкуплено_по_уведомлению"]
            + result["Вознагрожденение_в_доход"]
            - result["Итого_расходы"]
        )
        result = self._append_updated_at_column(result)
        return result

    def _append_updated_at_column(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Добавляет техническую колонку времени обновления в конец выгрузки.

        Колонка нужна пользователям Google Sheets как контроль свежести данных:
        по ней видно, когда именно строки week_n_redeem были подготовлены и
        отправлены в лист БД.
        """
        result = dataframe.copy()
        result["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return result

    def _normalize_column_names(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        renamed = dataframe.rename(columns=TRUNCATED_SQL_COLUMNS)
        renamed_columns = {
            source: target
            for source, target in TRUNCATED_SQL_COLUMNS.items()
            if source in dataframe.columns
        }
        if renamed_columns:
            logger.info(
                "week_n_redeem normalized truncated SQL columns: %s",
                renamed_columns,
            )
        return renamed

    def _validate_required_columns(self, dataframe: pd.DataFrame) -> None:
        required_columns = [
            "account",
            "Начало_периода",
            "Конец_периода",
            "Всего_стоимость_реализованного_товара",
            "Компенсации_ущерба",
            "Прочие_выплаты",
            "Выкуплено_по_уведомлению",
            "Вознагрожденение_в_доход",
            "Вознаграждение",
            "Вовзрат_выкупа",
            "Возмещение расходов по перевозке",
            "Возмещение_за_выдачу_и_возврат_товаров_на_ПВЗ",
            "Штрафы",
            "Прочие удержания",
            "Удержания_в_пользу_третьих_лиц",
            "Сумма_удержанная_в_счёт_обеспечения_организации_платежа",
        ]
        missing_columns = [
            column for column in required_columns if column not in dataframe.columns
        ]
        if missing_columns:
            logger.error(
                "week_n_redeem missing required columns: missing=%s available=%s",
                missing_columns,
                list(dataframe.columns),
            )
            raise KeyError(
                f"Missing required week_n_redeem columns: {missing_columns}"
            )

    def _filter_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Исключает строки, которые не должны попадать в лист БД.

        По бизнес-логике период с ``Конец_периода = 2025-12-31`` не выгружается
        в итоговую витрину week_n_redeem, поэтому фильтр применяется перед
        записью в Google Sheets.
        """
        result = dataframe.copy()
        result["Конец_периода"] = result["Конец_периода"].astype(str)
        filtered = result[result["Конец_периода"] != "2025-12-31"].copy()
        logger.info(
            "Фильтр Конец_периода != 2025-12-31: before=%s after=%s",
            len(result.index),
            len(filtered.index),
        )
        return filtered

    def _sort_dataframe_for_google(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Сортирует финальную витрину перед записью в лист БД.

        Пользователи читают лист БД как хронологическую выгрузку, поэтому строки
        должны идти по возрастанию ``Конец_периода``. Дополнительные поля в
        сортировке дают стабильный порядок внутри одного периода без изменения
        расчетной логики.
        """
        sort_columns = [
            column
            for column in (
                "Конец_периода",
                "account",
                "Номер_еженедельного_отчета",
            )
            if column in dataframe.columns
        ]
        if not sort_columns:
            return dataframe

        sorted_dataframe = dataframe.copy()
        sorted_dataframe["_sort_Конец_периода"] = pd.to_datetime(
            sorted_dataframe["Конец_периода"],
            errors="coerce",
        )
        sorted_dataframe = sorted_dataframe.sort_values(
            by=["_sort_Конец_периода", *sort_columns[1:]],
            ascending=True,
            na_position="last",
        )
        sorted_dataframe = sorted_dataframe.drop(columns=["_sort_Конец_периода"])
        logger.info(
            "Финальная сортировка week_n_redeem перед записью в Google Sheets выполнена: sort_columns=%s rows=%s",
            sort_columns,
            len(sorted_dataframe.index),
        )
        return sorted_dataframe.reset_index(drop=True)

    def _write_to_google(self, dataframe: pd.DataFrame) -> None:
        """Записывает финальную витрину week_n_redeem в Google Sheets с retry.

        Google Sheets иногда временно недоступен из-за DNS, сетевых сбоев,
        лимитов 429 или ошибок 5xx. Для бизнес-сценария важно не терять уже
        подготовленные данные, поэтому повторяется только операция открытия и
        записи листа БД, без повторного расчета SQL-витрины.
        """
        for attempt in range(1, GOOGLE_WRITE_RETRY_ATTEMPTS + 1):
            try:
                table = safe_open_spreadsheet(WEEK_N_REDEEM_TABLE_TITLE)
                sheet = table.worksheet(WEEK_N_REDEEM_SHEET_TITLE)
                set_with_dataframe(sheet, dataframe)
                logger.info(
                    "Данные week_n_redeem записаны в Google Sheets: table=%s sheet=%s rows=%s attempt=%s",
                    WEEK_N_REDEEM_TABLE_TITLE,
                    WEEK_N_REDEEM_SHEET_TITLE,
                    len(dataframe.index),
                    attempt,
                )
                return
            except Exception as error:
                if not self._is_retryable_google_error(error):
                    raise
                if attempt == GOOGLE_WRITE_RETRY_ATTEMPTS:
                    raise

                logger.warning(
                    "Временная ошибка записи week_n_redeem в Google Sheets, повторяем попытку: attempt=%s/%s wait_seconds=%s error_type=%s",
                    attempt,
                    GOOGLE_WRITE_RETRY_ATTEMPTS,
                    GOOGLE_WRITE_RETRY_DELAY_SECONDS,
                    type(error).__name__,
                )
                time.sleep(GOOGLE_WRITE_RETRY_DELAY_SECONDS)

    def _is_retryable_google_error(self, error: Exception) -> bool:
        """Определяет временные ошибки Google Sheets, которые можно повторить.

        Retry нужен только для сбоев инфраструктуры: DNS, сетевое подключение,
        timeout, лимиты 429 и серверные ошибки Google. Ошибки прав доступа или
        неверной структуры таблицы не повторяются, чтобы оператор сразу увидел
        реальную проблему в листе БД.
        """
        if isinstance(
            error,
            (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ),
        ):
            return True

        if isinstance(error, gspread.exceptions.APIError):
            status_code = getattr(error.response, "status_code", None)
            return status_code in GOOGLE_RETRYABLE_STATUS_CODES

        return False

    def _log_dataframe_snapshot(self, stage: str, dataframe: pd.DataFrame) -> None:
        logger.info(
            "week_n_redeem snapshot: stage=%s rows=%s columns=%s",
            stage,
            len(dataframe.index),
            list(dataframe.columns),
        )
        if "account" in dataframe.columns and not dataframe.empty:
            logger.info(
                "week_n_redeem snapshot: stage=%s accounts=%s",
                stage,
                sorted(dataframe["account"].astype(str).unique().tolist()),
            )
        if "Уведомление_о_выкупе_№" in dataframe.columns:
            logger.info(
                "week_n_redeem snapshot: stage=%s missing_redeem_notifications=%s",
                stage,
                int(dataframe["Уведомление_о_выкупе_№"].isna().sum()),
            )

    def _extract_accounts(self, dataframe: pd.DataFrame) -> list[str]:
        if "account" not in dataframe.columns or dataframe.empty:
            return []
        return sorted(dataframe["account"].dropna().astype(str).unique().tolist())

    def _count_missing_redeem_notifications(self, dataframe: pd.DataFrame) -> int:
        column_name = "Уведомление_о_выкупе_№"
        if column_name not in dataframe.columns:
            return 0
        return int(dataframe[column_name].isna().sum())

    def _count_duplicate_rows(self, dataframe: pd.DataFrame) -> int:
        duplicate_subset = ["account", "Номер_еженедельного_отчета"]
        if any(column not in dataframe.columns for column in duplicate_subset):
            return 0
        return int(dataframe.duplicated(subset=duplicate_subset, keep=False).sum())
