from datetime import date

import pandas as pd

from src_oop.jobs.autopilot_daily.repository import AutopilotDailyRepository


class CapturingDatabase:
    """Сохраняет вызов запроса для проверки источников дневной цены."""

    query: str | None = None
    params: dict[str, date] | None = None

    @classmethod
    def read_sql_to_dataframe(
        cls,
        query: object,
        params: dict[str, date] | None = None,
    ) -> pd.DataFrame:
        cls.query = str(query)
        cls.params = params
        return pd.DataFrame()


def test_historical_price_uses_prices_data_when_primary_price_is_missing_or_zero():
    """Историческая цена резервируется из prices_data по дате и артикулу."""
    repository = AutopilotDailyRepository(database_cls=CapturingDatabase)

    repository.fetch_current_metrics(report_date=date(2026, 11, 12))

    assert "NULLIF(metrics.price_with_disc, 0)" in CapturingDatabase.query
    assert "AVG(discounted_price) AS avg_discounted_price" in CapturingDatabase.query
    assert "GROUP BY date, article_id" in CapturingDatabase.query
    assert "daily_prices.article_id = metrics.article_id" in CapturingDatabase.query
    assert CapturingDatabase.params == {"report_date": date(2026, 11, 12)}
