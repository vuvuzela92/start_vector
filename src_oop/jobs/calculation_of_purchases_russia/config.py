from sqlalchemy import text

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

query_orders_and_supply = text("""
                WITH orders_to_suppliers AS
                    (SELECT o.document_number,
                        DATE(o.document_created_at) AS doc_date,
                        o.guid,
                        o.supply_date,
                        o.local_vendor_code,
                        o.expected_receipt_date,
                        o.product_name,
                        o.quantity,
                        o.amount_with_vat,
                        o.amount_without_vat,
                        o.supplier_name,
                        o.supplier_code,
                        o.author_of_the_change,
                        o.our_organizations_name,
                        round(COALESCE(o.amount_with_vat / NULLIF(o.quantity, 0), 0), 2) AS item_price,
                        o.planned_cost,
                        CASE
                                WHEN o.currency IS NOT NULL AND o.currency != '643'
                                    THEN o.planned_cost
                                ELSE o.planned_cost
                            END AS price_per_item,
                            row_number() OVER (PARTITION BY o.document_number, o.local_vendor_code ORDER BY o.supply_date DESC) AS supply_num
                    FROM ordered_goods_from_buyers o
                    WHERE o.event_status = 'Проведён'
                        AND o.is_valid = TRUE
                        AND o.product_name not ilike '%УТ%'
                        AND DATE(o.document_created_at) > '2026-01-01'
                    ),
                    acceptance_orders AS
                    (SELECT s.document_number,
                        DATE(s.document_created_at) AS doc_date,
                        s.guid,
                        s.order_guid,
                        s.supply_date,
                        s.local_vendor_code,
                        s.product_name,
                        s.quantity,
                        s.amount_with_vat,
                        s.amount_without_vat,
                        s.supplier_name,
                        s.supplier_code,
                        s.author_of_the_change,
                        s.our_organizations_name,
                        round(COALESCE(s.amount_with_vat / NULLIF(s.quantity, 0), 0), 2) AS item_price,
                        s.planned_cost,
                        CASE
                                WHEN s.currency IS NOT NULL AND s.currency != '643'
                                    THEN s.planned_cost
                                ELSE s.planned_cost
                            END AS price_per_item,
                            row_number() OVER (PARTITION BY s.document_number, s.local_vendor_code ORDER BY s.supply_date DESC) AS supply_num
                    FROM supply_to_sellers_warehouse s
                    WHERE s.event_status = 'Проведён'
                        AND s.is_valid = TRUE
                        AND s.product_name not ilike '%УТ%'
                        AND DATE(s.document_created_at) > '2026-01-01'
                        AND s.guid IS NOT NULL
                    ),
                    returns AS
                    (SELECT r.document_number,
                            r.supply_guid,
                            r.local_vendor_code,
                            r.quantity,
                        row_number() OVER (PARTITION BY r.document_number, r.local_vendor_code ORDER BY r.return_date DESC) AS return_num
                    FROM return_to_supplier r
                    WHERE r.is_valid is TRUE)
                    SELECT
                        CASE
                            WHEN s.quantity = o.quantity THEN 'Поставлен'
                            WHEN s.quantity > o.quantity THEN 'Недопоставка'
                            WHEN s.quantity < o.quantity THEN 'Перегруз'
                            ELSE 'Непоставка'
                        END AS "Статус заказа",
                        o.document_number AS "Номер документа заказа",
                        s.document_number AS "Номер документа прихода",
                        o.guid,
                        o.doc_date AS "Дата создания документа",
                        o.supply_date AS "Дата поставки",
                        o.expected_receipt_date AS "Ожидаемая дата прихода",
                        o.local_vendor_code AS wild,
                        o.product_name AS "Наименование товара",
                        o.quantity AS "Количество",
                        o.amount_with_vat AS "Сумма с НДС",
                        o.amount_without_vat "Сумма без НДС",
                        o.supplier_name AS "Название поставщика",
                        o.supplier_code AS "Код поставщика",
                        o.author_of_the_change AS "Автор изменения",
                        o.our_organizations_name AS "Название нашей организации",
                        o.item_price AS "Цена за единицу",
                        o.planned_cost AS "Плановая себестоимость",
                        o.supply_num AS "Поступление по счету",
                        s.quantity AS "Количество по приходам (сумма всех подвязанных приходов)",
                        s.amount_with_vat AS "Сумма приходов без НДС",
                        s.amount_without_vat AS "Сумма приходов с НДС",
                        r.quantity AS "Количество возвратов"
                    FROM orders_to_suppliers o
                    RIGHT JOIN acceptance_orders s
                        ON o.guid = s.order_guid
                        AND o.local_vendor_code = s.local_vendor_code
                        AND o.supply_num = s.supply_num
                    LEFT JOIN returns r
                        ON s.guid = r.supply_guid
                        AND s.local_vendor_code = r.local_vendor_code
                        AND s.supply_num = r.return_num;
    """)

google_table = {"title": "Расчет закупки Россия",
                "calculate_sheet": "Расчет закупки",
                "orders_sheet": "БД_Заказы",
                "statuses_sheet": "Статичный лист статусы",
                "orders_buyers_sheet": "Заказы_и_поступления"
                }