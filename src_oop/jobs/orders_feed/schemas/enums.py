"""Перечисления допустимых бизнес-значений WB Order Feed."""

from enum import StrEnum


class OrderStatus(StrEnum):
    """Допустимые бизнес-статусы заказа в ответе WB Order Feed."""

    CREATED = "created"
    BUYOUT = "buyout"
    CANCEL = "cancel"
    RETURN = "return"
    RETURN_DEFECTIVE = "returnDefective"


class CancelType(StrEnum):
    """Причины отмены, которые WB возвращает только для отменённого заказа."""

    APP = "app"
    RECEIPT = "receipt"
    EXPIRE = "expire"
    OTHER = "other"


class WarehouseType(StrEnum):
    """Понятное представление булевого признака WB `isMp`."""

    SELLER = "seller"
    WB = "wb"


class SaleType(StrEnum):
    """Тип покупателя, полученный из булевого признака WB `isB2b`."""

    B2B = "b2b"
    B2C = "b2c"


class DataSource(StrEnum):
    """Источник строки для будущего объединения новой и исторических витрин."""

    ORDER_FEED = "order_feed"
    ORDERS = "orders"
    SALES = "sales"
