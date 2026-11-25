from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime

import pandas as pd

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs

from src_oop.jobs.fbs_warehouses.config import ACCOUNT_ENV
from src_oop.jobs.fbs_stocks.config import (
    UNIT_FBO_COLUMNS,
    UNIT_FBO_QUERY,
    UNIT_FBO_SHEET_TITLE,
    UNIT_FBO_SPREADSHEET_ID,
)
from src_oop.jobs.fbs_stocks.service import FBSStocksService
from src_oop.jobs.fbs_stocks.telegram.models import FBSJobFailureContext
from src_oop.jobs.fbs_stocks.telegram.notifier import FBSStocksTelegramNotifier

logger = logging.getLogger(__name__)


def _resolve_account_scope() -> str:
    """Определяет текстовое представление области запуска FBS-сценария для уведомлений.

    Бизнес-сценарий: Telegram-сообщение должно сразу показывать, ограничен ли запуск одним ЛК или
    выполнялся по всем доступным кабинетам, чтобы оператор не перепутал тестовый и массовый прогон.
    """
    account = os.getenv(ACCOUNT_ENV, "").strip()
    return account if account else "все ЛК"


async def update_fbs_stocks_in_unit_async() -> None:
    """Запускает обновление текущих FBS-остатков WB в тестовой UNIT-таблице.

    Бизнес-сценарий: задача перечитывает фактические остатки по всем активным
    внутренним складам аккаунта и обновляет поле `ФБС общий остаток` в UNIT,
    чтобы ручные и автоматические сценарии опирались на актуальные данные WB.
    """
    notifier = FBSStocksTelegramNotifier()
    try:
        logger.info("Старт обновления FBS-остатков в UNIT.")
        summary = await FBSStocksService().update_current_fbs_stocks()
        if summary.notification_events:
            await notifier.notify_events(
                job_name="update_stocks_in_unit",
                account_scope=_resolve_account_scope(),
                events=summary.notification_events,
            )
        logger.info(
            "FBS-остатки в UNIT обновлены | unit_rows=%s | articles_with_chrt_id=%s | wb_requests=%s | updated_columns=%s",
            summary.unit_rows,
            summary.articles_with_chrt_id,
            summary.wb_requests,
            summary.updated_columns,
        )
    except Exception as error:
        await notifier.notify_full_failure(
            FBSJobFailureContext(
                job_name="update_stocks_in_unit",
                reason="Не удалось обновить текущие FBS-остатки в UNIT",
                error_type=type(error).__name__,
                account_scope=_resolve_account_scope(),
                detail=str(error),
                happened_at=datetime.now(),
            )
        )
        raise


async def apply_new_fbs_stocks_from_unit_async() -> None:
    """Запускает только ручной сценарий отправки новых FBS-остатков из UNIT.

    Бизнес-сценарий: задача читает управляющие поля `Новый остаток для всех
    складов` и `Новый остаток Вешки`, отправляет новые остатки в WB, затем
    очищает успешно примененные ячейки и обновляет `ФБС общий остаток` из WB.
    Проверка `Минимальный остаток` и автопополнение выполняются отдельной
    задачей, чтобы ручная команда не смешивалась с cron-логикой.
    """
    notifier = FBSStocksTelegramNotifier()
    try:
        service = FBSStocksService()
        logger.info("Старт отправки новых FBS-остатков из UNIT в WB.")
        summary = await service.apply_new_fbs_stocks()
        if summary.notification_events:
            await notifier.notify_events(
                job_name="apply_new_fbs_stocks_from_unit",
                account_scope=_resolve_account_scope(),
                events=summary.notification_events,
            )
        logger.info(
            "Сценарий новых FBS-остатков завершен | requested_rows=%s | prepared_rows=%s | skipped_rows=%s | wb_requests=%s | cleared_cells=%s | excluded_rows=%s | refreshed_columns=%s | applied=%s",
            summary.requested_rows,
            summary.prepared_rows,
            summary.skipped_rows,
            summary.wb_requests,
            summary.cleared_cells,
            summary.excluded_rows,
            summary.refreshed_columns,
            summary.applied,
        )
    except Exception as error:
        await notifier.notify_full_failure(
            FBSJobFailureContext(
                job_name="apply_new_fbs_stocks_from_unit",
                reason="Не удалось отправить новые FBS-остатки из UNIT",
                error_type=type(error).__name__,
                account_scope=_resolve_account_scope(),
                detail=str(error),
                happened_at=datetime.now(),
            )
        )
        raise


async def auto_refill_fbs_stocks_from_unit_async() -> None:
    """Запускает cron-сценарий автопополнения FBS-остатков из UNIT.

    Бизнес-сценарий: задача проверяет средний остаток на внутренний склад
    относительно колонки `Минимальный остаток` и при необходимости либо
    устанавливает значение `Добавляем` из листа `Сопост` на каждом активном
    FBS-складе, либо при включенном флаге `WB_FBS_AUTO_REFILL_VESHKI_ONLY`
    поддерживает запас только на Вешках.
    """
    notifier = FBSStocksTelegramNotifier()
    try:
        logger.info("Старт автопополнения FBS-остатков из UNIT в WB.")
        summary = await FBSStocksService().auto_refill_fbs_stocks()
        if not summary.applied:
            await notifier.notify_dry_run(
                job_name="auto_refill_fbs_stocks_from_unit",
                account_scope=_resolve_account_scope(),
                checked_rows=summary.checked_rows,
                prepared_rows=summary.prepared_rows,
                skipped_rows=summary.skipped_rows,
            )
        if summary.notification_events:
            await notifier.notify_events(
                job_name="auto_refill_fbs_stocks_from_unit",
                account_scope=_resolve_account_scope(),
                events=summary.notification_events,
            )
        logger.info(
            "Сценарий автопополнения FBS-остатков завершен | checked_rows=%s | triggered_rows=%s | prepared_rows=%s | skipped_rows=%s | wb_requests=%s | excluded_rows=%s | refreshed_columns=%s | applied=%s",
            summary.checked_rows,
            summary.triggered_rows,
            summary.prepared_rows,
            summary.skipped_rows,
            summary.wb_requests,
            summary.excluded_rows,
            summary.refreshed_columns,
            summary.applied,
        )
    except Exception as error:
        await notifier.notify_full_failure(
            FBSJobFailureContext(
                job_name="auto_refill_fbs_stocks_from_unit",
                reason="Не удалось выполнить автопополнение FBS-остатков",
                error_type=type(error).__name__,
                account_scope=_resolve_account_scope(),
                detail=str(error),
                happened_at=datetime.now(),
            )
        )
        raise


def _update_fbo_stocks_in_unit() -> None:
    """Выгружает сводку ФБО за текущую дату PostgreSQL в отдельный лист UNIT.

    Запрос берёт все склады и аккаунты, группирует по локальному артикулу и
    добавляет максимальную закупочную цену. Пустой снимок не очищает лист.
    Перед полной перезаписью проверяются заголовки, чтобы не стереть чужие
    поля или формулы. Ошибка БД либо Google Sheets прерывает этап с ФБО.
    """

    logger.info("Старт обновления остатков ФБО в UNIT.")
    dataframe = Database.read_sql_to_dataframe(UNIT_FBO_QUERY)
    if dataframe.empty:
        logger.warning(
            "Остатки ФБО за текущую дату пусты: лист не изменён, "
            "этап с ФБО завершён."
        )
        return

    dataframe = dataframe.reindex(columns=UNIT_FBO_COLUMNS)
    for column in UNIT_FBO_COLUMNS[1:]:
        dataframe[column] = pd.to_numeric(dataframe[column], errors="raise")

    google_tabs = GoogleTabs(
        table_title="UNIT 2.0 (tested)",
        sheet_title=UNIT_FBO_SHEET_TITLE,
        spreadsheet_id=UNIT_FBO_SPREADSHEET_ID,
    )
    headers = google_tabs.sheet_title.row_values(1)
    if headers and headers not in (UNIT_FBO_COLUMNS, UNIT_FBO_COLUMNS + ["updated_at"]):
        raise ValueError(
            "Лист 'Остатки ФБО' не обновлён: заголовки не совпадают "
            "с пятью полями выгрузки."
        )
    google_tabs.set_df_to_google(dataframe)
    logger.info("Остатки ФБО в UNIT обновлены: строк=%s.", len(dataframe.index))


def update_stocks_in_unit() -> None:
    """Обновляет FBS и ФБО в UNIT в рамках одного запуска задачи.

    FBS-этап получает фактические остатки через WB API и обновляет основную
    колонку UNIT. Независимый ФБО-этап читает снимок PostgreSQL и перезаписывает
    лист «Остатки ФБО». Оба этапа выполняются даже при сбое одного из них;
    ошибки возвращаются вызывающему коду после завершения второго этапа.
    """

    async def run_both() -> None:
        """Запускает оба источника остатков, не блокируя FBS во время чтения БД.

        Для общего сценария UNIT важно попробовать обновить оба листа, даже
        если WB API или PostgreSQL/Google Sheets временно недоступны.
        """

        results = await asyncio.gather(
            update_fbs_stocks_in_unit_async(),
            asyncio.to_thread(_update_fbo_stocks_in_unit),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, Exception)]
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise ExceptionGroup("Не удалось обновить FBS и ФБО остатки в UNIT.", errors)

    asyncio.run(run_both())


def apply_new_fbs_stocks_from_unit() -> None:
    """Синхронный entrypoint для ручной отправки новых FBS-остатков из UNIT.

    Бизнес-сценарий: запускает только ручное применение значений из колонок
    `Новый остаток для всех складов` и `Новый остаток Вешки`, без автопополнения
    по минимальному остатку в этом же прогоне.
    """
    asyncio.run(apply_new_fbs_stocks_from_unit_async())


def auto_refill_fbs_stocks_from_unit() -> None:
    """Синхронный entrypoint для cron-автопополнения FBS-остатков из UNIT.

    Бизнес-сценарий: запускает отдельную проверку минимального остатка и при
    необходимости пополняет либо все активные внутренние склады по данным
    листа `Сопост`, либо только Вешки при специальном булевом флаге.
    """
    asyncio.run(auto_refill_fbs_stocks_from_unit_async())
