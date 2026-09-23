"""Сервис полного цикла загрузки и подготовки сборочных заданий WB."""

from __future__ import annotations

import asyncio
import logging

import aiohttp
import pandas as pd

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.assembly_info.client import AssemblyInfoClient
from src_oop.jobs.assembly_info.repository import AssemblyInfoRepository

logger = logging.getLogger(__name__)


class AssemblyInfoService:
    """Оркестрирует загрузку заказов, статусов, нормализацию и запись витрины."""

    def __init__(self, client: AssemblyInfoClient | None = None,
                 repository: AssemblyInfoRepository | None = None) -> None:
        """Собирает зависимости job, позволяя подменять API и БД в тестах."""
        self.client = client or AssemblyInfoClient()
        self.repository = repository or AssemblyInfoRepository()

    async def run(self) -> None:
        """Запускает полный сценарий синхронизации сборочных заданий и статусов WB."""
        tokens = {str(account): str(token) for account, token in load_api_tokens().items()}
        async with aiohttp.ClientSession() as session:
            orders = await asyncio.gather(*(self.client.fetch_orders(session, account, token)
                                            for account, token in tokens.items()))
            orders_df = self._create_orders_dataframe(orders)
            if orders_df.empty:
                logger.warning("Синхронизация сборочных заданий завершена: данные не получены.")
                return
            grouped = orders_df.groupby("account")["id"].apply(list).to_dict()
            terminal_ids = self.repository.fetch_terminal_order_ids(
                self.client.settings.terminal_wb_statuses,
                grouped,
            )
            candidate_ids_by_account = {
                account: self._select_status_candidates(
                    account,
                    ids,
                    terminal_ids,
                    orders_df=orders_df,
                    lookback_days=self.client.settings.status_lookback_days,
                )
                for account, ids in grouped.items()
                if account in tokens
            }
            statuses = await asyncio.gather(*(
                self.client.fetch_statuses(session, account, tokens[account], order_ids)
                for account, order_ids in candidate_ids_by_account.items()
                if order_ids
            ))
        status_rows = [row for batch in statuses for row in batch]
        if not status_rows:
            logger.info(
                "Синхронизация сборочных заданий завершена: новых или активных статусов для записи нет."
            )
            return
        result = self._prepare_dataframe(orders_df, pd.DataFrame(status_rows))
        self.repository.save(result)
        logger.info("Синхронизация сборочных заданий завершена | rows=%s", len(result.index))

    @staticmethod
    def _select_status_candidates(
        account: str,
        order_ids: list[int],
        terminal_ids: dict[str, set[int]],
        orders_df: pd.DataFrame,
        lookback_days: int,
    ) -> list[int]:
        """Оставляет недавние и незавершенные задания для частого запроса статусов WB."""
        terminal = terminal_ids.get(account, set())
        account_orders = orders_df.loc[orders_df["account"] == account]
        recent_ids = set(
            account_orders.loc[
                account_orders["createdAt_msk"]
                >= pd.Timestamp.now(tz="Europe/Moscow") - pd.Timedelta(days=lookback_days),
                "id",
            ].astype(int)
        )
        candidates = [
            int(order_id)
            for order_id in order_ids
            if int(order_id) in recent_ids and int(order_id) not in terminal
        ]
        logger.info(
            "Сформирован список заданий для проверки статусов | account=%s | total=%s | lookback_days=%s | candidates=%s",
            account, len(order_ids), lookback_days, len(candidates),
        )
        return candidates

    @staticmethod
    def _create_orders_dataframe(batches: list[list[dict[str, object]]]) -> pd.DataFrame:
        """Формирует заказы и добавляет московское время для бизнес-даты витрины."""
        dataframe = pd.DataFrame([row for batch in batches for row in batch])
        if dataframe.empty:
            return dataframe
        dataframe["createdAt"] = pd.to_datetime(dataframe["createdAt"], utc=True)
        dataframe["createdAt_msk"] = dataframe["createdAt"].dt.tz_convert("Europe/Moscow")
        dataframe["local_vendor_code"] = dataframe["article"].astype("string").str.extract(r"(wild\d+)", expand=False)
        return dataframe

    @staticmethod
    def _prepare_dataframe(orders: pd.DataFrame, statuses: pd.DataFrame) -> pd.DataFrame:
        """Объединяет WB-данные, переводит цены в рубли и применяет бизнес-фильтры."""
        dataframe = orders.merge(statuses, how="left", on=["id", "account"])
        dataframe = dataframe.rename(columns={
            "deliveryType": "delivery_type", "orderUid": "order_uid", "colorCode": "color_code",
            "createdAt": "created_at", "warehouseId": "warehouse_id", "chrtId": "chrt_id",
            "convertedPrice": "converted_price", "currencyCode": "currency_code",
            "convertedCurrencyCode": "converted_currency_code", "cargoType": "cargo_type",
            "isZeroOrder": "is_zero_order", "officeId": "office_id", "createdAt_msk": "created_at_msk",
            "nmId": "nm_id", "article": "vendor_code", "supplyId": "supply_id",
            "supplierStatus": "supplier_status", "wbStatus": "wb_status",
        })
        columns = ["date", "nm_id", "local_vendor_code", "vendor_code", "id", "supplier_status", "wb_status",
                   "supply_id", "address", "scan_price", "price", "converted_price", "comment", "delivery_type",
                   "order_uid", "color_code", "rid", "created_at", "created_at_msk", "offices", "skus", "warehouse_id",
                   "chrt_id", "currency_code", "converted_currency_code", "cargo_type", "is_zero_order", "options", "office_id", "account"]
        # Поля статуса могут отсутствовать в частичном ответе WB; пустые значения
        # сохраняют строку заказа и позволяют batch-upsert обработать остальные данные.
        for column in columns:
            if column not in dataframe:
                dataframe[column] = None
        dataframe["date"] = pd.to_datetime(dataframe["created_at_msk"]).dt.date
        for column in ("offices", "skus", "options"):
            dataframe[column] = dataframe[column].astype(str).str.replace(r"[\[\]{}]", "", regex=True)
        for column in ("scan_price", "price", "converted_price"):
            dataframe[column] = (pd.to_numeric(dataframe[column], errors="coerce") / 100).round(3)
        dataframe = dataframe[columns].drop_duplicates(["id", "supplier_status", "wb_status"])
        dataframe = dataframe[dataframe["wb_status"].notna() & dataframe["wb_status"].ne("NaN")]
        return dataframe[~((dataframe["wb_status"] == "waiting") & (dataframe["supplier_status"] == "cancel"))]
