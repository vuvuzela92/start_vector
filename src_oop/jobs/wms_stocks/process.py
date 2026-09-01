from __future__ import annotations

from datetime import datetime

import pandas as pd

from src_oop.jobs.wms_stocks.config import WMS_STOCK_ZONES


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

    def process_daily_balances(self, quantity_column_name: str = "stock_qty") -> pd.DataFrame:
        """Нормализует ответ `daily-balances` в строки по каждому товару и дню.

        Бизнес-сценарий:
        endpoint уже возвращает агрегированный диапазон дней в `items[].days[]`,
        включая дни без операций. Задача загрузки - развернуть этот диапазон в
        строки `public.wms_stock`, сохранив исходный `opening_quantity` как
        суммарный остаток товара на начало конкретной даты. Один и тот же разбор
        используется и для общего остатка, и для отдельных складских зон.
        """
        if not self.data:
            return pd.DataFrame(
                columns=["balance_date", "product_id", quantity_column_name, "loaded_at"]
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
                        quantity_column_name: day.get("opening_quantity"),
                        "loaded_at": loaded_at,
                    }
                )

        dataframe = pd.DataFrame(rows)
        if dataframe.empty:
            return pd.DataFrame(
                columns=["balance_date", "product_id", quantity_column_name, "loaded_at"]
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
        dataframe[quantity_column_name] = pd.to_numeric(
            dataframe[quantity_column_name],
            errors="coerce",
        ).fillna(0)

        grouped_dataframe = dataframe.drop_duplicates(
            subset=["balance_date", "product_id"],
            keep="last",
        ).copy()
        grouped_dataframe[quantity_column_name] = (
            grouped_dataframe[quantity_column_name].round().astype("Int64")
        )
        return grouped_dataframe

    def merge_daily_balance_frames(
        self,
        frames_by_quantity: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Объединяет общий остаток и зональные срезы в одну витрину `public.wms_stock`.

        Бизнес-сценарий:
        бизнесу нужен один ряд на `(balance_date, product_id)`, где одновременно
        виден общий остаток по складу и остатки по ключевым зонам. Если товар
        присутствует только в части зон, строка не должна теряться при merge.
        """
        key_columns = ["balance_date", "product_id"]
        if not frames_by_quantity:
            return pd.DataFrame(
                columns=self._build_final_columns()
            )

        prepared_frames = [
            dataframe.copy()
            for dataframe in frames_by_quantity.values()
            if dataframe is not None and not dataframe.empty
        ]
        if not prepared_frames:
            return pd.DataFrame(columns=self._build_final_columns())

        merged_dataframe = prepared_frames[0]
        for dataframe in prepared_frames[1:]:
            merged_dataframe = merged_dataframe.merge(
                dataframe,
                on=key_columns,
                how="outer",
                suffixes=("", "_dup"),
            )

            duplicate_loaded_columns = [
                column
                for column in merged_dataframe.columns
                if column.startswith("loaded_at")
            ]
            if len(duplicate_loaded_columns) > 1:
                merged_dataframe["loaded_at"] = merged_dataframe[duplicate_loaded_columns].max(axis=1)
                merged_dataframe = merged_dataframe.drop(columns=duplicate_loaded_columns)

        return self._finalize_merged_daily_balances(merged_dataframe)

    def _finalize_merged_daily_balances(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Приводит объединенную витрину остатков к финальной схеме записи в БД.

        Бизнес-сценарий:
        после merge общий остаток и отдельные зоны могут прийти с разными
        наборами колонок, но репозиторию нужен единый DataFrame с итоговым
        набором полей витрины и одним `loaded_at` для upsert в `public.wms_stock`.
        """
        prepared_dataframe = dataframe.copy()
        loaded_at_columns = [
            column_name
            for column_name in prepared_dataframe.columns
            if column_name.startswith("loaded_at")
        ]
        if loaded_at_columns:
            prepared_dataframe["loaded_at"] = prepared_dataframe[loaded_at_columns].max(axis=1)
            prepared_dataframe = prepared_dataframe.drop(columns=loaded_at_columns)

        for column_name in self._build_quantity_columns():
            if column_name not in prepared_dataframe.columns:
                prepared_dataframe[column_name] = pd.NA
            prepared_dataframe[column_name] = pd.to_numeric(
                prepared_dataframe[column_name],
                errors="coerce",
            ).astype("Int64")

        if "loaded_at" not in prepared_dataframe.columns:
            prepared_dataframe["loaded_at"] = datetime.now()

        return prepared_dataframe.loc[
            :,
            self._build_final_columns(),
        ].copy()

    def _build_quantity_columns(self) -> list[str]:
        """Возвращает список количественных колонок новой витрины WMS.

        Бизнес-сценарий:
        схема `public.wms_stock` может расширяться новыми зонами, поэтому merge
        и финальная подготовка должны опираться на единый список бизнес-колонок,
        а не на разрозненные литералы по коду.
        """
        return ["stock_qty", *(zone.column_name for zone in WMS_STOCK_ZONES)]

    def _build_final_columns(self) -> list[str]:
        """Возвращает итоговый порядок колонок для записи витрины `public.wms_stock`.

        Бизнес-сценарий:
        фиксированный порядок полей упрощает диагностику, сравнение выгрузок и
        безопасную запись в PostgreSQL через общий batch upsert проекта.
        """
        return ["balance_date", "product_id", *self._build_quantity_columns(), "loaded_at"]

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
        основной контракт `daily-balances` использует `days[].opening_quantity`,
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
