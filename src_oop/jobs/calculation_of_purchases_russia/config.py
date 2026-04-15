query = """
        SELECT sum(f.order_count) AS order_count,
        f.wild,
        f.date
        FROM funnel_daily f
    WHERE f.date >= CURRENT_DATE - INTERVAL '7 days'
    AND f.order_count > 0
    GROUP BY f.wild, f.date
    ORDER BY f.date DESC;
    """

google_table = {"title": "Расчет закупки Россия",
                "calculate_sheet": "Расчет закупки",
                "orders_sheet": "БД_Заказы"
                }