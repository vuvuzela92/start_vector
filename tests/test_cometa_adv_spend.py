from __future__ import annotations

import os
import unittest

import aiohttp

from src_oop.jobs.autopilot.cometa_adv_spend import CometaAdvSpendClient


class StubCometaAdvSpendClient(CometaAdvSpendClient):
    def __init__(self, payload: list[dict[str, object]]) -> None:
        """
        Создает тестовый клиент Cometa с заранее заданным ответом API.

        Бизнес-логика:
        позволяет проверить правила агрегации рекламных расходов без сетевого запроса и без
        использования реального COMETA_API_KEY.
        """
        super().__init__()
        self.payload = payload

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
    ) -> object:
        """
        Возвращает тестовый ответ Cometa вместо HTTP-запроса.

        Бизнес-логика:
        изолирует проверку расчета `adv_spend` от доступности внешнего сервиса и состояния кабинета.
        """
        return self.payload


class CometaAdvSpendClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_today_spend_keeps_stopped_autopilot_with_today_spend(self) -> None:
        """
        Проверяет, что остановленный автопилот с расходом за сегодня попадает в `adv_spend`.

        Бизнес-логика:
        Cometa может показывать расход за сегодня по уже остановленному автопилоту, поэтому фильтр
        по текущему статусу не должен обнулять рекламные затраты в ПУ.
        """
        os.environ["COMETA_API_KEY"] = "test-token"
        client = StubCometaAdvSpendClient(
            payload=[
                {
                    "active": False,
                    "product_id": 1286381113,
                    "budget_spent_today": 205,
                },
                {
                    "active": True,
                    "product_id": 1286381124,
                    "budget_spent_today": 0,
                },
                {
                    "active": True,
                    "product_id": 999,
                    "budget_spent_today": 100,
                },
            ]
        )

        result = await client.fetch_today_spend(articles=[1286381113, 1286381124])

        self.assertEqual({1286381113: 225.5}, result)
