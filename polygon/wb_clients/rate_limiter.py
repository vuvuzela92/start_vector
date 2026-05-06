import asyncio
from collections import defaultdict
from dataclasses import dataclass
import time
from typing import Optional

from loguru import logger


@dataclass
class RateLimitConfig:
    """Конфигурация рейт-лимитинга."""
    base_interval: float = 0.2
    safety_margin: float = 1.2
    endpoint_overrides: Optional[dict[str, float]] = None
    max_retries: int = 3
    backoff_base: float = 2.0
    max_backoff: float = 10.0


class RateLimiter:
    """Управление рейт-лимитами для запросов."""

    def __init__(
        self,
        config: RateLimitConfig,
        limiter_name: str = "default",
        locks: Optional[defaultdict[asyncio.Lock]] = None,
        last_request_times: Optional[defaultdict[float]] = None,
    ):
        self.limiter_name = limiter_name.capitalize()
        self.config = config
        self._locks = locks or defaultdict(asyncio.Lock)
        self._last_request_times = last_request_times or defaultdict(float)

    def _get_interval(self, endpoint: str) -> float:
        """Получить интервал для эндпоинта."""
        raw_interval = self.config.endpoint_overrides.get(
            endpoint, self.config.base_interval
        ) if self.config.endpoint_overrides else self.config.base_interval
        return raw_interval * self.config.safety_margin

    def _get_lock_key(self, endpoint: str) -> str:
        """Получить ключ для блокировки."""
        if self.config.endpoint_overrides and endpoint in self.config.endpoint_overrides:
            return endpoint
        return "default"

    async def acquire(self, endpoint: str) -> None:
        """Дождаться разрешения на запрос."""
        lock_key = self._get_lock_key(endpoint)
        interval = self._get_interval(endpoint)
        
        async with self._locks[lock_key]:
            now = time.monotonic()
            elapsed = now - self._last_request_times[lock_key]
            
            if elapsed < interval:
                delay = interval - elapsed
                logger.debug(f"[{self.limiter_name}] Задержка рейт-лимитинга: {delay:.2f}с для {endpoint}")
                await asyncio.sleep(delay)
            
            self._last_request_times[lock_key] = time.monotonic()


WB_CONTENT_CARDS_UPLOAD = "/content/v2/cards/upload"
WB_CONTENT_CARDS_UPDATE = "/content/v2/cards/update"
WB_CONTENT_CARDS_ERROR_LIST = "/content/v2/cards/error/list"
WB_CONTENT_CARDS_TRASH = "/content/v2/cards/delete/trash"
WB_CONTENT_CARDS_RECOVER = "/content/v2/cards/recover"


class _GlobalWBRateLimiter:
    """Глобальный менеджер рейт-лимитеров по аккаунтам."""

    def __init__(self):
        self._content_rate_limiters: dict[str, RateLimiter] = {}

    def get_content_rate_limiter(self, account_name: str) -> RateLimiter:
        """Получить рейт-лимитер для эндпоинтов категории Контент"""
        account = account_name.capitalize()

        if account not in self._content_rate_limiters:
            rate_limit_config = RateLimitConfig(
                base_interval=0.6,
                endpoint_overrides={
                    WB_CONTENT_CARDS_UPLOAD: 6.0,
                    WB_CONTENT_CARDS_UPDATE: 6.0,
                    WB_CONTENT_CARDS_ERROR_LIST: 6.0,
                    WB_CONTENT_CARDS_TRASH: 20.0,
                    WB_CONTENT_CARDS_RECOVER: 20.0,
                }
            )
            self._content_rate_limiters[account] = RateLimiter(limiter_name=f"{account}-wb-content", config=rate_limit_config)

        return self._content_rate_limiters[account]

global_rate_limiter = _GlobalWBRateLimiter()
