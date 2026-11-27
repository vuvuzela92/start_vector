"""Клиенты интеграций WB и Google Sheets для старых витрин сборки."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.assembly_market_sheets.config import AssemblyMarketSheetsSettings

logger = logging.getLogger(__name__)


class AssemblyOrdersClient:
    """Загружает заказы WB, нужные тестовой таблице сборочных заданий."""

    URL = "https://marketplace-api.wildberries.ru/api/v3/orders"

    def __init__(self, settings: AssemblyMarketSheetsSettings | None = None) -> None:
        """Создает клиент с ограничениями нагрузки и времени ожидания WB."""
        self.settings = settings or AssemblyMarketSheetsSettings()
        self._semaphore = asyncio.Semaphore(self.settings.max_concurrent_accounts)

    async def fetch_account_orders(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        begin: int,
        end: int,
    ) -> list[dict[str, Any]]:
        """Постранично получает заказы аккаунта за заданный период.

        Повторы с паузой сохраняют сценарий выгрузки при временных сбоях WB;
        при 429 ожидание увеличивается, чтобы не усугублять ограничение API.
        """
        result: list[dict[str, Any]] = []
        cursor = 0
        pages = 0
        started_at = time.perf_counter()
        async with self._semaphore:
            while True:
                params = {"limit": 1000, "next": cursor, "dateFrom": begin, "dateTo": end}
                for attempt in range(1, self.settings.max_retries + 1):
                    try:
                        async with session.get(
                            self.URL,
                            params=params,
                            headers={"Authorization": token},
                            timeout=aiohttp.ClientTimeout(
                                total=self.settings.request_timeout_seconds
                            ),
                        ) as response:
                            if response.status == 429 or response.status >= 500:
                                if attempt == self.settings.max_retries:
                                    response.raise_for_status()
                                retry_after = response.headers.get("Retry-After")
                                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 60)
                                if response.status == 429:
                                    delay = max(delay, 60)
                                logger.warning(
                                    "WB временно ограничил выгрузку заказов; повторяем запрос | account=%s | status=%s | attempt=%s/%s | delay=%s",
                                    account, response.status, attempt, self.settings.max_retries, delay,
                                )
                                await asyncio.sleep(delay)
                                continue
                            response.raise_for_status()
                            payload = await response.json()
                        break
                    except (aiohttp.ClientError, TimeoutError) as error:
                        if attempt == self.settings.max_retries:
                            logger.error(
                                "Не удалось получить заказы WB; выгрузка этого аккаунта прервана | account=%s | error_type=%s",
                                account, type(error).__name__,
                            )
                            raise
                        await asyncio.sleep(min(2 ** attempt, 60))
                else:
                    break

                result.extend({**order, "account": account} for order in payload.get("orders", []))
                pages += 1
                cursor = int(payload.get("next", 0) or 0)
                if cursor == 0:
                    logger.info(
                        "Заказы WB загружены по страницам | account=%s | rows=%s | pages=%s | seconds=%.1f",
                        account, len(result), pages, time.perf_counter() - started_at,
                    )
                    return result
                await asyncio.sleep(self.settings.request_delay_seconds)


class AssemblyMarketSheetsClient:
    """Читает и обновляет листы, связанные с операционными заданиями WB."""

    def __init__(self, settings: AssemblyMarketSheetsSettings | None = None) -> None:
        """Сохраняет имена таблиц; подключение открывается только при операции."""
        self.settings = settings or AssemblyMarketSheetsSettings()

    def upload(self, dataframe: pd.DataFrame, table: str, sheet: str) -> None:
        """Полностью обновляет лист порциями и отмечает московское время выгрузки.

        Порционная запись ограничивает размер HTTP-тела и защищает выгрузки
        больших выборок от таймаута при отправке одного запроса в Google Sheets.
        Старые строки после более короткого снимка очищаются, как и при полной
        перезаписи листа.
        """
        connector = GoogleTabs(table_title=table, sheet_title=sheet)
        started_at = time.perf_counter()
        exported = dataframe.copy()
        exported[self.settings.export_time_column] = datetime.now(
            ZoneInfo("Europe/Moscow")
        ).strftime("%Y-%m-%d %H:%M:%S")
        worksheet = connector.sheet_title
        read_started_at = time.perf_counter()
        old_values = connector.get_all_values_with_retry()
        old_rows = len(old_values)
        old_columns = max((len(row) for row in old_values), default=0)
        read_seconds = time.perf_counter() - read_started_at
        headers = exported.columns.astype(str).tolist()
        chunk_size = self.settings.sheets_write_chunk_rows
        if chunk_size <= 0:
            raise ValueError("Размер порции записи в Google Sheets должен быть больше нуля.")

        connector.update_range(
            "A1:" + rowcol_to_a1(1, len(headers)),
            [headers],
        )
        write_started_at = time.perf_counter()
        for start in range(0, len(exported), chunk_size):
            chunk_dataframe = exported.iloc[start : start + chunk_size]
            chunk = (
                chunk_dataframe.astype(object)
                .where(pd.notnull(chunk_dataframe), "")
                .values.tolist()
            )
            start_row = start + 2
            end_row = start_row + len(chunk) - 1
            connector.update_range(
                f"A{start_row}:{rowcol_to_a1(end_row, len(headers))}",
                chunk,
            )

        new_rows = len(exported) + 1
        target_rows = max(old_rows, new_rows)
        target_columns = max(old_columns, len(headers))
        if old_rows > new_rows and target_columns:
            clear_start = new_rows + 1
            clear_end = target_rows
            connector.update_range(
                f"A{clear_start}:{rowcol_to_a1(clear_end, target_columns)}",
                [[""] * target_columns for _ in range(clear_end - clear_start + 1)],
            )
        logger.info(
            "Лист Google Sheets полностью обновлен порциями | table=%s | sheet=%s | rows=%s | chunk_rows=%s | chunks=%s | read_seconds=%.1f | write_seconds=%.1f | total_seconds=%.1f",
            table,
            getattr(worksheet, "title", sheet),
            len(exported),
            chunk_size,
            (len(exported) + chunk_size - 1) // chunk_size,
            read_seconds,
            time.perf_counter() - write_started_at,
            time.perf_counter() - started_at,
        )

    def read_values(self, table: str, sheet: str) -> list[list[str]]:
        """Читает строки листа для переноса сборочных заданий между таблицами."""
        return GoogleTabs(table_title=table, sheet_title=sheet).get_all_values_with_retry()

    def update_statuses(self, dataframe: pd.DataFrame) -> None:
        """Записывает объединенные статусы заказов на выделенный лист проекта."""
        self.upload(dataframe, self.settings.market_table, self.settings.market_status_sheet)


def normalize_orders_for_sheet(rows: list[dict[str, Any]], offset_hours: int) -> pd.DataFrame:
    """Формирует колонночный формат тестовой таблицы заказов и московское время."""
    columns = ["id", "nmId", "deliveryType", "article", "createdAt", "account"]
    dataframe = pd.DataFrame(rows)
    if dataframe.empty:
        return pd.DataFrame(columns=["id", "Артикул ВБ", "Тип доставки", "Артикул поставщика", "Создано", "ЛК", "Дата", "Время", "wild"])
    dataframe = dataframe.reindex(columns=columns)
    created_at = pd.to_datetime(dataframe["createdAt"], errors="coerce") + pd.Timedelta(hours=offset_hours)
    dataframe["createdAt"] = created_at
    dataframe["date"] = created_at.dt.date
    dataframe["time"] = created_at.dt.time
    dataframe["wild"] = dataframe["article"].astype("string").str.extract(r"(wild\d+)", expand=False)
    dataframe = dataframe.rename(columns={
        "nmId": "Артикул ВБ", "deliveryType": "Тип доставки", "article": "Артикул поставщика",
        "createdAt": "Создано", "account": "ЛК", "date": "Дата", "time": "Время",
    })
    for column in ("Создано", "Дата", "Время"):
        dataframe[column] = dataframe[column].astype(str)
    return dataframe


def normalize_market_statuses(
    values: list[list[str]], statuses: pd.DataFrame
) -> pd.DataFrame:
    """Сопоставляет ID заданий из листа с актуальными статусами PostgreSQL."""
    columns = ["supplier_status", "wb_status", "id", "ЛК"]
    if not values:
        return pd.DataFrame(columns=columns)
    sheet_dataframe = pd.DataFrame(values[1:], columns=values[0])
    if not {"id", "ЛК"}.issubset(sheet_dataframe.columns):
        raise ValueError("На листе сборочных заданий отсутствуют обязательные колонки id и ЛК.")
    sheet_dataframe = sheet_dataframe[["id", "ЛК"]].copy()
    sheet_dataframe["id"] = pd.to_numeric(sheet_dataframe["id"], errors="coerce")
    sheet_dataframe = sheet_dataframe.dropna(subset=["id"])
    sheet_dataframe["id"] = sheet_dataframe["id"].astype("int64")
    merged = sheet_dataframe.merge(
        statuses, how="left", left_on="id", right_on="assembly_task_id"
    )
    return merged.reindex(columns=columns)
