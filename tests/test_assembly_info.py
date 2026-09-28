import pandas as pd

from src_oop.jobs.assembly_info.service import AssemblyInfoService


def test_prepare_dataframe_applies_business_filters_and_converts_prices():
    orders = [[{
        "id": 1, "createdAt": "2026-09-23T00:00:00Z", "article": "wild123",
        "nmId": 10, "account": "demo", "createdAt_msk": "2026-09-23T03:00:00+03:00",
    }]]
    orders_df = AssemblyInfoService._create_orders_dataframe(orders)
    statuses = pd.DataFrame([{
        "id": 1, "account": "demo", "supplierStatus": "confirm",
        "wbStatus": "sorted", "price": 12345, "convertedPrice": 10000,
    }])
    result = AssemblyInfoService._prepare_dataframe(orders_df, statuses)
    assert result.iloc[0]["price"] == 123.45
    assert result.iloc[0]["converted_price"] == 100.0
    assert result.iloc[0]["vendor_code"] == "wild123"


def test_select_status_candidates_skips_only_known_terminal_orders():
    """Проверяет, что инкрементальный режим не исключает активные задания."""
    orders = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "account": ["demo", "demo", "demo"],
            "createdAt_msk": pd.to_datetime(
                ["2026-09-22T12:00:00+03:00"] * 3
            ),
        }
    )
    result = AssemblyInfoService._select_status_candidates(
        "demo",
        [1, 2, 3],
        {"demo": {1, 3}},
        orders,
        11,
    )
    assert result == [2]


def test_select_status_candidates_skips_orders_older_than_lookback():
    """Проверяет, что задания старше настроенного окна не опрашиваются."""
    orders = pd.DataFrame(
        {
            "id": [1, 2],
            "account": ["demo", "demo"],
            "createdAt_msk": pd.to_datetime(
                ["2026-09-22T12:00:00+03:00", "2026-09-01T12:00:00+03:00"]
            ),
        }
    )
    result = AssemblyInfoService._select_status_candidates(
        "demo", [1, 2], {"demo": set()}, orders, 11
    )
    assert result == [1]
