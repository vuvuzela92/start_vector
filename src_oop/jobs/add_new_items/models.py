from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True, slots=True)
class NewItemCard:
    """Нормализованная строка из вкладки 'Для юнит'."""

    row_number: int
    supplier_name: str
    sku: int
    client: str
    supplier_code_duplicates: str
    status: str
    item_name: str
    category: str
    supplier_code_unique: str
    purchase_price: str
    manager: str

    @property
    def wild(self) -> str:
        return self.supplier_code_unique

    @property
    def autopilot_client(self) -> str:
        # В Автопилоте исторически используется client в верхнем регистре.
        return self.client.upper()

    @property
    def normalized_purchase_price(self) -> str:
        value = self.purchase_price.strip()
        if not value:
            return ""

        normalized = value.replace(" ", "").replace(",", ".")
        try:
            decimal_value = Decimal(normalized)
        except InvalidOperation:
            return value

        normalized_decimal = decimal_value.normalize()
        if normalized_decimal == normalized_decimal.to_integral():
            return str(int(normalized_decimal))

        return format(normalized_decimal, "f")
