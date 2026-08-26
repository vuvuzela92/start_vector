from __future__ import annotations

from datetime import datetime

import pandas as pd

class Process:
    """Преобразует ответы WMS API в табличный вид для старой и новой выгрузки.

    Бизнес-сценарий:
    старый метод раскладывает историю транзакций по дням для legacy-таблицы, а
    новый агрегирует дневные остатки до одного суммарного значения на товар и дату
    для витрины `public.wms_stock`.
    """

    def __init__(self, data: list):
        """Сохраняет сырой ответ API до этапа нормализации и агрегации.

        Бизнес-сценарий:
        обе выгрузки строятся из массива JSON-объектов WMS, поэтому класс
        принимает сырые данные один раз и затем готовит нужный целевой срез.
        """
        self.data = data

    def process_historical_stocks(self)-> pd.DataFrame:
        """Преобразует legacy-ответ исторических остатков в плоский DataFrame.

        Бизнес-сценарий:
        метод сохраняет прежнюю структуру старой выгрузки, где один `product_id`
        раскладывается в набор строк по датам транзакций для существующей таблицы.
        """
        if self.data is None:
            return pd.DataFrame()
        stock_list = []
        for item in self.data:
            wild = item.get("product_id")
            for transaction in item.get("data"):
                stock_list.append({
                    "wild": wild,
                    "transaction_date": transaction["transaction_date"],
                    "end_of_day_balance": transaction["end_of_day_balance"]
                })

        df = pd.DataFrame(stock_list)
        return df

    def process_daily_balances(self) -> pd.DataFrame:
        """Нормализует ответ `daily-balances` в строки по каждому товару и дню.

        Бизнес-сценарий:
        endpoint уже возвращает агрегированный диапазон дней в `items[].days[]`,
        включая дни без операций. Задача загрузки - развернуть этот диапазон в
        строки `public.wms_stock`, сохранив итоговый `closing_quantity` как
        суммарный остаток товара на конкретную дату.
        """
        if not self.data:
            return pd.DataFrame(
                columns=["balance_date", "product_id", "stock_qty", "loaded_at"]
            )

        rows: list[dict[str, object]] = []
        loaded_at = datetime.now()
        for item in self.data:
            if not isinstance(item, dict):
                continue

            product_id = self._extract_product_id(item)
            for day in self._extract_days(item):
                rows.append(
                    {
                        "balance_date": day.get("date"),
                        "product_id": product_id,
                        "stock_qty": day.get("closing_quantity"),
                        "loaded_at": loaded_at,
                    }
                )

        dataframe = pd.DataFrame(rows)
        if dataframe.empty:
            return pd.DataFrame(
                columns=["balance_date", "product_id", "stock_qty", "loaded_at"]
            )

        dataframe["balance_date"] = pd.to_datetime(
            dataframe["balance_date"],
            errors="coerce",
        ).dt.date
        dataframe["product_id"] = (
            dataframe["product_id"]
            .astype("string")
            .str.strip()
        )
        dataframe["product_id"] = dataframe["product_id"].replace(
            {
                "": pd.NA,
                "None": pd.NA,
                "<NA>": pd.NA,
                "nan": pd.NA,
            }
        )
        dataframe["stock_qty"] = pd.to_numeric(
            dataframe["stock_qty"],
            errors="coerce",
        ).fillna(0)

        grouped_dataframe = dataframe.drop_duplicates(
            subset=["balance_date", "product_id"],
            keep="last",
        ).copy()
        grouped_dataframe["stock_qty"] = grouped_dataframe["stock_qty"].round().astype("Int64")
        return grouped_dataframe

    def _extract_product_id(self, item: dict[str, object]) -> object:
        """Ищет идентификатор товара в основных вариантах полей ответа.

        Бизнес-правило:
        витрина агрегируется именно по `product_id`, поэтому метод приводит к
        общему полю как прямой `product_id`, так и возможные алиасы источника.
        """
        for key in ("product_id", "nm_id", "article_id", "wild"):
            value = item.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    def _extract_stock_qty(self, item: dict[str, object]) -> object:
        """Оставлен для совместимости и запасного разбора альтернативных ответов.

        Бизнес-правило:
        основной контракт `daily-balances` использует `days[].closing_quantity`,
        но helper сохраняется как безопасный запасной путь, если тестовый ответ
        сервиса придет в упрощенном плоском виде.
        """
        for key in (
            "stock_qty",
            "balance",
            "quantity",
            "qty",
            "end_of_day_balance",
            "available_qty",
        ):
            value = item.get(key)
            if value not in (None, ""):
                return value
        return 0

    def _extract_days(self, item: dict[str, object]) -> list[dict[str, object]]:
        """Возвращает список дней из `items[].days[]` ответа `daily-balances`.

        Бизнес-правило:
        endpoint гарантирует диапазон дней внутри каждого товара, включая
        пустые дни. Именно этот список является источником строк для
        `public.wms_stock` и должен разбираться явно, а не через плоский маппинг.
        """
        days = item.get("days")
        if isinstance(days, list):
            return [day for day in days if isinstance(day, dict)]
        return []
