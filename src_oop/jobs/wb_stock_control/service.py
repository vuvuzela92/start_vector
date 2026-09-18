"""Сервис полного контроля опубликованных FBS-остатков WB."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import UTC, datetime

import aiohttp

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.bitrix_chat_control.config import BitrixChatControlSettings
from src_oop.jobs.bitrix_chat_control.rest_client import ReadonlyBitrixRESTClient
from src_oop.jobs.wb_stock_control.client import WBStockControlClient
from src_oop.jobs.wb_stock_control.config import StockControlSettings
from src_oop.jobs.wb_stock_control.google_sheets_client import read_unit_article_ids
from src_oop.jobs.wb_stock_control.models import StockObservation, StockTarget
from src_oop.jobs.wb_stock_control.repository import WBStockControlRepository

logger = logging.getLogger(__name__)


class WBStockControlService:
    """Оркестрирует чтение UNIT, WB, ClickHouse и уведомления Bitrix24."""

    def __init__(
        self,
        repository: WBStockControlRepository | None = None,
        client: WBStockControlClient | None = None,
        settings: StockControlSettings | None = None,
    ) -> None:
        """Собирает зависимости read-only проверки без изменения остатков WB."""
        self.repository = repository or WBStockControlRepository()
        self.client = client or WBStockControlClient()
        self.settings = settings or StockControlSettings.from_env()

    async def run(self) -> None:
        """Проверяет все активные ЛК/склады, сохраняет снимок и сообщает о рисках.

        Бизнес-сценарий: карточки вне UNIT также проверяются, но job никогда не
        отправляет команды изменения остатков в WB. Исключения читаются из
        рабочей UNIT 2.0 (tested); ошибка её чтения прерывает весь сценарий.
        """
        unit_article_ids = read_unit_article_ids()
        targets = self.repository.fetch_targets(unit_article_ids)
        self.repository.ensure_tables()
        if not targets:
            logger.warning("Контроль остатков WB завершен: активные товары для проверки не найдены.")
            return

        tokens = {str(key).strip().casefold(): value for key, value in load_api_tokens().items()}
        grouped: dict[tuple[str, int], list[StockTarget]] = defaultdict(list)
        for target in targets:
            grouped[(target.account, target.wb_warehouse_id)].append(target)

        observations: list[StockObservation] = []
        semaphore = asyncio.Semaphore(self.settings.concurrency)
        async with aiohttp.ClientSession() as session:
            async def check_group(group: tuple[str, int], rows: list[StockTarget]) -> list[StockObservation]:
                account, wb_warehouse_id = group
                async with semaphore:
                    token = tokens.get(account)
                    now = datetime.now(UTC)
                    if not token:
                        return [StockObservation(row, None, "error", "token_missing", now) for row in rows]
                    amounts, complete = await self.client.fetch_stocks(
                        session, account, token, wb_warehouse_id, [row.chrt_id for row in rows]
                    )
                    return [
                        StockObservation(
                            row,
                            amounts.get(row.chrt_id) if complete else None,
                            "ok" if complete else "incomplete",
                            None if complete else "incomplete_response",
                            now,
                        )
                        for row in rows
                    ]

            results = await asyncio.gather(*(check_group(group, rows) for group, rows in grouped.items()))
        observations = [item for result in results for item in result]
        previous = self.repository.fetch_previous_states(
            [
                (item.target.account, item.target.wb_warehouse_id, item.target.chrt_id)
                for item in observations
            ]
        )
        self.repository.save_observations(observations)
        await self._notify(observations, previous)
        logger.info("Контроль остатков WB завершен | targets=%s | observations=%s", len(targets), len(observations))

    async def _notify(
        self,
        observations: list[StockObservation],
        previous: dict[tuple[str, int, int], tuple[int | None, str]],
    ) -> None:
        """Отправляет новые или изменившиеся риски в рабочий чат Bitrix24.

        Бизнес-правило: сотрудникам передаются строки на уровне SKU с ЛК и
        `wild`; технические идентификаторы склада и `chrt_id` не показываются.
        Если все позиции вне UNIT проверены успешно и имеют нулевой остаток,
        вместо пустого вывода печатается явное сообщение об отсутствии остатков.
        """
        grouped: dict[tuple[str, str, int], dict[str, object]] = {}
        previous_grouped: dict[tuple[str, str, int], dict[str, object]] = {}
        for observation in observations:
            target = observation.target
            if target.is_in_unit:
                continue
            group_key = (target.account, target.wild, target.article_id)
            previous_state = previous.get(
                (target.account, target.wb_warehouse_id, target.chrt_id)
            )
            if previous_state is not None:
                old_item = previous_grouped.setdefault(
                    group_key,
                    {"amount": 0, "has_amount": False, "incomplete": False},
                )
                old_amount, old_status = previous_state
                if old_amount is not None and old_status == "ok":
                    old_item["amount"] = int(old_item["amount"]) + old_amount
                    old_item["has_amount"] = True
                else:
                    old_item["incomplete"] = True
            item = grouped.setdefault(
                group_key,
                {"amount": 0, "has_amount": False, "incomplete": False},
            )
            if observation.amount is not None and observation.check_status == "ok":
                item["amount"] = int(item["amount"]) + observation.amount
                item["has_amount"] = True
            else:
                item["incomplete"] = True

        messages: list[str] = []
        for (account, wild, article_id), item in sorted(grouped.items()):
            if item["incomplete"] and not item["has_amount"]:
                text = (
                    f"ЛК: {account} | wild: {wild} | SKU: {article_id} | "
                    "остаток: не удалось определить."
                )
                state = "incomplete"
            elif item["amount"] > 0:
                text = (
                    f"ЛК: {account} | wild: {wild} | SKU: {article_id} | "
                    f"остаток: {item['amount']} шт."
                )
                state = f"positive:{item['amount']}"
            else:
                text = (
                    f"ЛК: {account} | wild: {wild} | SKU: {article_id} | "
                    "остаток: 0 шт."
                )
                state = "zero"
            # Положительный остаток вне UNIT всегда является операционным
            # сигналом и должен повторяться в каждом отчёте, пока он существует.
            if state.startswith("positive:"):
                messages.append(text)
                continue
            old_item = previous_grouped.get((account, wild, article_id))
            old_state = None
            if old_item is not None:
                if old_item["incomplete"] and not old_item["has_amount"]:
                    old_state = "incomplete"
                elif old_item["amount"] > 0:
                    old_state = f"positive:{old_item['amount']}"
                else:
                    old_state = "zero"
            if old_state == state:
                continue
            messages.append(text)

        # Нулевой результат важен для оператора: отсутствие строк не должно
        # выглядеть как отсутствие запуска или ошибка формирования отчёта.
        has_incomplete = any(bool(item["incomplete"]) for item in grouped.values())
        has_positive = any(int(item["amount"]) > 0 for item in grouped.values())
        if not has_incomplete and not has_positive:
            print("Склад ФБС: остатков по wild вне юнитки не обнаружено.")
            return

        if not messages:
            return
        positive = [
            message
            for message in messages
            if "шт." in message and "остаток: 0 шт." not in message
        ]
        zero = [message for message in messages if "не обнаружен" in message]
        incomplete = [message for message in messages if "не удалось проверить" in message]
        sections = []
        if positive:
            sections.append("Склад ФБС: остатки по wild вне юнитки:\n" + "\n".join(positive))
        if zero:
            sections.append("Склад ФБС: нулевые остатки по SKU вне юнитки:\n" + "\n".join(zero))
        if incomplete:
            sections.append("Склад ФБС: остаток по wild не удалось проверить:\n" + "\n".join(incomplete))
        notification_text = "\n\n".join(sections)[:4000]
        print(notification_text)
        if not self.settings.bitrix_enabled:
            logger.info(
                "Отправка уведомления контроля остатков в Bitrix24 отключена настройкой; текст выведен в консоль."
            )
            return
        if not self.settings.bitrix_dialog_id:
            logger.error(
                "Уведомление контроля остатков не отправлено: не задан ID чата Bitrix24."
            )
            return
        bitrix_client = ReadonlyBitrixRESTClient(BitrixChatControlSettings.from_env())
        await bitrix_client.send_chat_message(
            self.settings.bitrix_dialog_id,
            notification_text,
        )
        logger.info(
            "Уведомление контроля остатков отправлено в Bitrix24 | rows=%s",
            len(messages),
        )
