"""Перечисления допустимых бизнес-значений WB Order Feed."""

from enum import StrEnum


class OrderStatus(StrEnum):
    """Известные текущей версии приложения статусы WB Order Feed."""

    CREATED = "created"
    BUYOUT = "buyout"
    CANCEL = "cancel"
    RETURN = "return"
    RETURN_DEFECTIVE = "returnDefective"


class CancelType(StrEnum):
    """Известные причины отмены, описанные в текущем контракте WB."""

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
