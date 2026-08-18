from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text


@dataclass(frozen=True, slots=True)
class SalesAnalyzeSheetConfig:
    """
    Конфигурация Google Sheets для выгрузки аналитики продаж по складам.

    Бизнес-назначение:
    хранит точное имя таблицы и вкладки, куда задача публикует агрегированную
    аналитику по продажам с наших складов, чтобы эти параметры не дублировались
    в коде запуска и не расходились между окружениями.
    """

    table_title: str
    sheet_title: str


SALES_ANALYZE_SHEET_CONFIG = SalesAnalyzeSheetConfig(
    table_title="Новый товар",
    sheet_title="Аналитика складов",
)


SALES_ANALYZE_QUERY = text(
    """
   --- Анализ продаж по нашим складам ---
    SELECT 
        sum(w.seller_price) AS revenue,
        ROUND(avg(w.seller_price), 2) AS price,
        sum(w.seller_price)/avg(w.seller_price) AS qnt,
        w.nm_id,
        a.local_vendor_code,
        w.warehouse_name, 
        date(w.created_at) AS order_date,
        w.account
    FROM wb_order_feed w
    LEFT JOIN article a 
        ON a.nm_id = w.nm_id 
    WHERE w.warehouse_type = 'seller'
    GROUP BY 
        w.nm_id,
        a.local_vendor_code,
        w.warehouse_name, 
        date(w.created_at),
        w.account
    ;
    """
)
