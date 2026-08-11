oblast_okrug_name = None
country_name = None
order_type = None
discount_percent = None
finished_price = None

if oblast_okrug_name is None and country_name != 'Россия':
    oblast_okrug_name == country_name

if order_type == "Клиентский":
    order_type = "b2c"
else:
    order_type = "b2b"

seller_price = finished_price * (1 - discount_percent / 100)

mapping_sales_to_orders_feed = {
    "warehouse_name": "warehouse_name",
    "warehouse_type": "warehouse_type",
    "oblast_okrug_name": "destination_district",
    "region_name": "warehouse_region",
    "article_id": "nm_id",
    "order_type": "sale_type",
    "???": "seller_price",
    "srid": "srid",
    "date_from": "created_at",
    "last_change_date": "updated_at",
}

mapping_orders_to_orders_feed = {
    "warehouse_name": "warehouse_name",
    "warehouse_type": "warehouse_type",
    "oblast_okrug_name": "destination_district",
    "region_name": "warehouse_region",
    "article_id": "nm_id",
    "order_type": "sale_type",
    "???": "seller_price",
    "srid": "srid",
    "date_from": "created_at",
    "last_change_date": "updated_at",
}