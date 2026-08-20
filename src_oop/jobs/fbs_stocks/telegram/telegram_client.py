from __future__ import annotations

import aiohttp


class FBSStocksTelegramClient:
    """Отправляет подготовленные FBS-уведомления в Telegram через Bot API.

    Бизнес-сценарий: FBS-контур должен быстро сообщать оператору о сбоях и dry-run-запусках. При
    этом ошибки Telegram не должны ломать основной бизнес-сценарий, поэтому клиент остается простым
    и безопасным по входным данным.
    """

    def __init__(self, request_timeout_seconds: int) -> None:
        """Настраивает таймаут доставки уведомлений в Telegram."""
        self.request_timeout_seconds = request_timeout_seconds

    async def send_message(self, bot_token: str, chat_id: str, text: str) -> None:
        """Отправляет одно текстовое сообщение в Telegram без вывода секретов в логи.

        Бизнес-правило: токен бота и содержимое технических исключений не должны попадать в логи.
        Клиент отправляет только уже сформированный текст и в случае ошибки возвращает безопасное
        исключение вызывающему коду.
        """
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as response:
                if response.status >= 400:
                    raise RuntimeError(
                        "Telegram Bot API вернул ошибку при отправке FBS-уведомления "
                        f"| status={response.status}"
                    )
