from __future__ import annotations

import unittest

from src_oop.jobs.purchase_price_update.processor import (
    DuplicateBusinessKeyError,
    build_report_dataframe,
    build_unit_sheet_dataframe,
    prepare_purchase_price_updates,
    prepare_unit_state,
)


class PurchasePriceUpdateProcessorTests(unittest.TestCase):
    def test_prepare_purchase_price_updates_matches_prices_and_preserves_db_order(self) -> None:
        unit_values = [
            ["wild", "Стоимость в закупке (руб.)", "Неизменяемая цена"],
            ["wild2", "210", ""],
            ["wild1", "100", ""],
            ["wild3", "300", "1"],
            ["", "", ""],
        ]
        unit_dataframe = build_unit_sheet_dataframe(
            values=unit_values,
            header_row_index=0,
            data_row_index=1,
        )
        unit_state = prepare_unit_state(unit_dataframe)

        db_dataframe = self._build_db_dataframe(
            rows=[
                {
                    "local_vendor_code": "wild1",
                    "product_name": "Item 1",
                    "price_per_item": 120,
                },
                {
                    "local_vendor_code": "wild2",
                    "product_name": "Item 2",
                    "price_per_item": 200,
                },
                {
                    "local_vendor_code": "wild3",
                    "product_name": "Item 3",
                    "price_per_item": 310,
                },
                {
                    "local_vendor_code": "wild4",
                    "product_name": "Item 4",
                    "price_per_item": 410,
                },
            ]
        )

        result = prepare_purchase_price_updates(
            db_dataframe=db_dataframe,
            unit_state=unit_state,
            round_price=False,
        )

        self.assertEqual(["wild1", "wild2"], result.changed_rows["local_vendor_code"].tolist())
        self.assertEqual(( "wild3",), result.excluded_locked_codes)
        self.assertEqual(( "wild4",), result.absent_in_unit_codes)
        self.assertEqual(2, len(result.prepared_rows.index))
        self.assertEqual(-20.0, result.changed_rows.iloc[0]["price_diff_rub"])

    def test_prepare_purchase_price_updates_skips_rows_without_price(self) -> None:
        unit_state = prepare_unit_state(
            build_unit_sheet_dataframe(
                values=[
                    ["wild", "Стоимость в закупке (руб.)", "Неизменяемая цена"],
                    ["wild1", "100", ""],
                ],
                header_row_index=0,
                data_row_index=1,
            )
        )
        db_dataframe = self._build_db_dataframe(
            rows=[
                {
                    "local_vendor_code": "wild1",
                    "product_name": "Item 1",
                    "price_per_item": None,
                }
            ]
        )

        result = prepare_purchase_price_updates(
            db_dataframe=db_dataframe,
            unit_state=unit_state,
            round_price=False,
        )

        self.assertEqual(("wild1",), result.missing_price_codes)
        self.assertTrue(result.changed_rows.empty)

    def test_prepare_purchase_price_updates_uses_legacy_rounding(self) -> None:
        unit_state = prepare_unit_state(
            build_unit_sheet_dataframe(
                values=[
                    ["wild", "Стоимость в закупке (руб.)", "Неизменяемая цена"],
                    ["wild1", "11", ""],
                ],
                header_row_index=0,
                data_row_index=1,
            )
        )
        db_dataframe = self._build_db_dataframe(
            rows=[
                {
                    "local_vendor_code": "wild1",
                    "product_name": "Item 1",
                    "price_per_item": 10.99,
                }
            ]
        )

        result = prepare_purchase_price_updates(
            db_dataframe=db_dataframe,
            unit_state=unit_state,
            round_price=True,
        )

        self.assertTrue(result.changed_rows.empty)
        self.assertEqual(11, result.prepared_rows.iloc[0]["price_per_item"])

    def test_prepare_unit_state_detects_duplicate_business_keys(self) -> None:
        unit_dataframe = build_unit_sheet_dataframe(
            values=[
                ["wild", "Стоимость в закупке (руб.)", "Неизменяемая цена"],
                ["wild1", "100", ""],
                ["wild1", "200", ""],
            ],
            header_row_index=0,
            data_row_index=1,
        )

        with self.assertRaises(DuplicateBusinessKeyError):
            prepare_unit_state(unit_dataframe)

    def test_build_report_dataframe_formats_output_columns(self) -> None:
        unit_state = prepare_unit_state(
            build_unit_sheet_dataframe(
                values=[
                    ["wild", "Стоимость в закупке (руб.)", "Неизменяемая цена"],
                    ["wild1", "100", ""],
                ],
                header_row_index=0,
                data_row_index=1,
            )
        )
        db_dataframe = self._build_db_dataframe(
            rows=[
                {
                    "local_vendor_code": "wild1",
                    "product_name": "Item 1",
                    "price_per_item": 120,
                }
            ]
        )
        result = prepare_purchase_price_updates(
            db_dataframe=db_dataframe,
            unit_state=unit_state,
            round_price=False,
        )

        report_dataframe = build_report_dataframe(result.changed_rows)

        self.assertEqual(
            [
                "product_name",
                "local_vendor_code",
                "guid",
                "document_number",
                "quantity",
                "unit_price",
                "price_per_item",
                "price_diff_rub",
                "supply_date",
                "insert_date",
            ],
            list(report_dataframe.columns),
        )
        self.assertEqual("wild1", report_dataframe.iloc[0]["local_vendor_code"])

    @staticmethod
    def _build_db_dataframe(rows: list[dict[str, object]]):
        import pandas as pd

        base_rows = []
        for index, row in enumerate(rows, start=1):
            base_rows.append(
                {
                    "supply_date": pd.Timestamp("2026-07-20"),
                    "guid": f"guid-{index}",
                    "document_number": f"doc-{index}",
                    "local_vendor_code": row["local_vendor_code"],
                    "product_name": row["product_name"],
                    "amount_with_vat": 1000,
                    "quantity": 10,
                    "latest_price_per_item": row["price_per_item"],
                    "price_per_item": row["price_per_item"],
                    "currency": "643",
                    "planned_cost": None,
                    "alarm_flag": None,
                }
            )
        return pd.DataFrame(base_rows)


if __name__ == "__main__":
    unittest.main()
