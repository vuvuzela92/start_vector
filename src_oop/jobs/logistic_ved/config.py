from sqlalchemy import text

# Гугл-Таблицы
# Отгрузка ФБО
shipments_fbo = {
    "title": "Отгрузка ФБО",
    "white_orders_sheet": "Заказы по округам",
}

LOGISTIC_VED_COLUMNS = [
    "Артикул поставщика",
    "nm_id",
    "Округ / область",
    "Количество заказов",
]

LOGISTIC_VED_COLUMN_MAPPING = {
    "local_vendor_code": "Артикул поставщика",
    "nm_id": "nm_id",
    "oblast_okrug_name": "Округ / область",
    "orders_cnt": "Количество заказов",
}

LOGISTIC_VED_CLEAR_RANGE = "A:Z"

LOGISTIC_VED_ORDERS_BY_REGION_QUERY = text(
    """
    SELECT
        a.local_vendor_code,
        a.nm_id,
        o.oblast_okrug_name,
        COUNT(o.is_realization) AS orders_cnt
    FROM orders AS o
    LEFT JOIN article AS a
        ON o.article_id = a.nm_id
    WHERE o.date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY
        a.local_vendor_code,
        a.nm_id,
        o.oblast_okrug_name
    ORDER BY
        orders_cnt DESC,
        a.local_vendor_code,
        a.nm_id,
        o.oblast_okrug_name;
    """
)
