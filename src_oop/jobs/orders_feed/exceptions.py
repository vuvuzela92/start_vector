"""Доменные исключения загрузки WB Order Feed."""

from __future__ import annotations

from typing import Any


class OrderFeedError(Exception):
    """Базовая ошибка сценария Order Feed."""


class OrderFeedAPIError(OrderFeedError):
    """Ошибка, которую WB вернул HTTP-ответом."""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        account: str,
        request_id: str | None = None,
        detail: str | None = None,
        response_body: Any = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.account = account
        self.request_id = request_id
        self.detail = detail
        self.response_body = response_body


class OrderFeedBadRequestError(OrderFeedAPIError):
    """WB отклонил параметры или тело запроса (HTTP 400)."""


class OrderFeedAuthenticationError(OrderFeedAPIError):
    """Токен отсутствует, повреждён или просрочен (HTTP 401)."""


class OrderFeedAuthorizationError(OrderFeedAPIError):
    """У токена или тарифа нет доступа к методу (HTTP 402/403)."""


class OrderFeedRateLimitError(OrderFeedAPIError):
    """Исчерпаны повторы после ограничения частоты запросов (HTTP 429)."""


class OrderFeedUpstreamError(OrderFeedAPIError):
    """WB вернул прочую HTTP-ошибку, включая исчерпанные повторы 5xx."""


class OrderFeedResponseValidationError(OrderFeedError):
    """Успешный ответ WB не соответствует ожидаемому контракту."""


class OrderFeedTransportError(OrderFeedError):
    """Не удалось связаться с WB после повторов."""


class OrderFeedRepositoryError(OrderFeedError):
    """Операция с таблицей Order Feed не была выполнена."""
