"""Общий сервис отправки сервисных Telegram-сообщений."""

from __future__ import annotations

import logging

import aiohttp
from src_oop.core.telegram.config import TelegramCoreSettings

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Отправляет сервисные сообщения в Telegram без бизнес-зависимостей.

    Этот слой нужен для daily/weekly-рассылок и технических уведомлений. Он не
    знает ничего о конкретных задачах проекта и только безопасно доставляет уже
    подготовленный текст в заранее разрешённые сервисные чаты.
    """

    def __init__(self, settings: TelegramCoreSettings | None = None) -> None:
        """Подключает общую Telegram-конфигурацию проекта."""
        self.settings = settings or TelegramCoreSettings.from_env()

    async def send_to_service_chats(self, text: str) -> int:
        """Отправляет сообщение во все сервисные Telegram-чаты из конфигурации.

        Бизнес-правило: отсутствие токена или списка сервисных чатов не должно
        валить основную job-задачу. В таком случае сообщение просто не уходит, а
        вызывающий код может продолжить основную бизнес-логику.
        """
        if not self.settings.bot_token:
            logger.warning(
                "Сервисное Telegram-сообщение пропущено: не задан TELEGRAM_BOT_TOKEN."
            )
            return 0
        if not self.settings.service_chat_ids:
            logger.warning(
                "Сервисное Telegram-сообщение пропущено: не заданы TELEGRAM_SERVICE_CHAT_IDS."
            )
            return 0

        sent_count = 0
        timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout_seconds)
        url = f"https://api.telegram.org/bot{self.settings.bot_token}/sendMessage"
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for chat_id in self.settings.service_chat_ids:
                payload = {
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                }
                try:
                    async with session.post(url, json=payload) as response:
                        if response.status >= 400:
                            logger.error(
                                "Telegram Bot API вернул ошибку при сервисной отправке | status=%s | chat_id=%s",
                                response.status,
                                chat_id,
                            )
                            continue
                except Exception as error:
                    logger.error(
                        "Не удалось отправить сервисное Telegram-сообщение | error_type=%s | chat_id=%s",
                        type(error).__name__,
                        chat_id,
                    )
                    continue
                sent_count += 1
        return sent_count
