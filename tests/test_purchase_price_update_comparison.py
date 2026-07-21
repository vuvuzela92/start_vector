from __future__ import annotations

import unittest

import pandas as pd

from src_oop.jobs.purchase_price_update.comparison import compare_with_legacy
from src_oop.jobs.purchase_price_update.processor import (
    build_unit_sheet_dataframe,
    prepare_unit_state,
)


class PurchasePriceUpdateComparisonTests(unittest.TestCase):
    def test_compare_with_legacy_reports_equivalent_result(self) -> None:
        unit_state = prepare_unit_state(
            build_unit_sheet_dataframe(
                values=[
                    ["wild", "Стоимость в закупке (руб.)", "Неизменяемая цена"],
                    ["wild2", "200", ""],
                    ["wild1", "100", ""],
                    ["wild3", "300", "1"],
                ],
                header_row_index=0,
                data_row_index=1,
            )
        )
        db_dataframe = pd.DataFrame(
            [
                self._db_row("wild1", 120, "Item 1"),
                self._db_row("wild2", 190, "Item 2"),
                self._db_row("wild3", 290, "Item 3"),
                self._db_row("wild4", 250, "Item 4"),
            ]
        )

        summary, new_result = compare_with_legacy(
            db_dataframe=db_dataframe,
            unit_state=unit_state,
            round_price=False,
        )

        self.assertTrue(summary.same_row_count)
        self.assertTrue(summary.same_order)
        self.assertTrue(summary.same_prices)
        self.assertEqual(tuple(), summary.only_in_legacy)
        self.assertEqual(tuple(), summary.only_in_new)
        self.assertTrue(summary.price_mismatches.empty)
        self.assertEqual(2, len(new_result.changed_rows.index))

    @staticmethod
    def _db_row(code: str, price: float, name: str) -> dict[str, object]:
        return {
            "supply_date": pd.Timestamp("2026-07-20"),
            "guid": f"guid-{code}",
            "document_number": f"doc-{code}",
            "local_vendor_code": code,
            "product_name": name,
            "amount_with_vat": 1000,
            "quantity": 10,
            "latest_price_per_item": price,
            "price_per_item": price,
            "currency": "643",
            "planned_cost": None,
            "alarm_flag": None,
        }


if __name__ == "__main__":
    unittest.main()
