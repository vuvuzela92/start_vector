"""Настройки и SQL-запросы витрин движений и остатков для карты БД закупщиков."""

from sqlalchemy import text


# Идентификатор используется вместо имени, чтобы переименование документа не ломало выгрузку.
PURCHASERS_DATABASE_SPREADSHEET_ID = "1md1hQgysVfh36KSkiqnLjO8SXVuJl5ZvnNEEvtKSAXQ"
PURCHASERS_DATABASE_TABLE_TITLE = "Карта БД Закупщиков"
SHIPMENTS_SHEET_TITLE = "БД Отгрузки"
STOCKS_SHEET_TITLE = "БД Остатки"

SHIPMENTS_COLUMNS = [
    "id",
    "date",
    "fbs_quantity",
    "supply_quantity",
    "fbo_in_quantity",
    "fbo_out_quantity",
    "defects_in_quantity",
    "defects_out_quantity",
    "resort_quantity",
    "resort_delta",
    "fbs_stocks",
]

SHIPMENTS_DATABASE_QUERY = text(
    """
    WITH dates AS (
    SELECT generate_series(DATE '2026-07-29', CURRENT_DATE, INTERVAL '1 day')::date AS dt
    ),
    products_list AS (
        SELECT unnest(ARRAY[
            'wild163', 'wild359', 'wild188901', 'wild188601', 'wild1890',
            'wild1893', 'wild1895', 'wild1896', 'wild1886', 'wild1889',
            'wild1891', 'wild1894', 'wild1884', 'wild1969', 'wild1970',
            'wild1971', 'wild1972', 'wild1973', 'wild1974', 'wild1975',
            'wild1976', 'wild1977', 'wild1978', 'wild1979', 'wild1980',
            'wild1981', 'wild1982', 'wild1983', 'wild2082', 'wild2083',
            'wild2093', 'wild2094', 'wild2123', 'wild2132', 'wild1892'
        ]::varchar[]) AS product_id
    ),
    fbs_agg AS (
        SELECT
            s.product_id,
            (s.shipment_date AT TIME ZONE 'Europe/Moscow')::date AS dt,
            SUM(s.quantity) AS fbs_quantity
        FROM wms.fbs_shipment_items s
        WHERE s.status = 'success'
        AND s.shipment_date >= DATE '2026-07-29'
        AND s.shipment_date < CURRENT_DATE + INTERVAL '1 day'
        AND s.product_id IN (SELECT product_id FROM products_list)
        GROUP BY
            s.product_id,
            (s.shipment_date AT TIME ZONE 'Europe/Moscow')::date
    ),
    supply_agg AS (
        SELECT
            sw.local_vendor_code AS product_id,
            COALESCE(sw.supply_date::date, sw.document_created_at::date) AS dt,
            SUM(sw.quantity) AS supply_quantity
        FROM supply_to_sellers_warehouse sw
        WHERE COALESCE(sw.supply_date::date, sw.document_created_at::date) >= DATE '2026-07-29'
        AND COALESCE(sw.supply_date::date, sw.document_created_at::date) <= CURRENT_DATE
        AND sw.local_vendor_code IN (SELECT product_id FROM products_list)
        AND sw.is_valid IS TRUE
        GROUP BY
            sw.local_vendor_code,
            COALESCE(sw.supply_date::date, sw.document_created_at::date)
    ),
    movements_agg AS (
        SELECT
            m.product_id,
            (m.created_at AT TIME ZONE 'Europe/Moscow')::date AS dt,
            SUM(m.quantity) FILTER (
                WHERE m.to_location_id = 36
            ) AS fbo_in_quantity,
            SUM(m.quantity) FILTER (
                WHERE m.from_location_id = 36
            ) AS fbo_out_quantity,
            SUM(m.quantity) FILTER (
                WHERE m.to_location_id = 37
                AND m.source_type IS DISTINCT FROM 're_sorting_operation'
            ) AS defects_in_quantity,
            SUM(m.quantity) FILTER (
                WHERE m.from_location_id = 37
                AND m.source_type IS DISTINCT FROM 're_sorting_operation'
            ) AS defects_out_quantity
        FROM wms.movements m
        WHERE m.created_at >= DATE '2026-07-29'
        AND m.created_at < CURRENT_DATE + INTERVAL '1 day'
        AND m.product_id IN (SELECT product_id FROM products_list)
        AND (
            m.to_location_id IN (36, 37)
            OR m.from_location_id IN (36, 37)
        )
        GROUP BY
            m.product_id,
            (m.created_at AT TIME ZONE 'Europe/Moscow')::date
    ),
    resorts AS (
        SELECT
            rs.product_id,
            (rs.movement_created_at AT TIME ZONE 'Europe/Moscow')::date AS dt,
            SUM(rs.quantity) FILTER (
                WHERE rs.role = 'source_outgoing'
            ) AS resort_quantity,
            SUM(
                CASE
                    WHEN rs.role = 'target_incoming' THEN rs.quantity
                    WHEN rs.role = 'source_outgoing' THEN -rs.quantity
                    ELSE 0
                END
            ) AS resort_delta
        FROM wms.re_sorting_operation_items rs
        WHERE rs.movement_created_at >= DATE '2026-07-29'
        AND rs.movement_created_at < CURRENT_DATE + INTERVAL '1 day'
        AND rs.product_id IN (SELECT product_id FROM products_list)
        GROUP BY
            rs.product_id,
            (rs.movement_created_at AT TIME ZONE 'Europe/Moscow')::date
    ),
    wms_stocks AS (
        SELECT
            ws.balance_date AS dt,
            ws.product_id,
            ws.fbs
        FROM wms_stock ws
        WHERE ws.balance_date >= DATE '2026-07-29'
        AND ws.balance_date <= CURRENT_DATE
        AND ws.product_id IN (SELECT product_id FROM products_list)
    )
    SELECT
        pl.product_id AS id,
        d.dt AS date,
        COALESCE(fa.fbs_quantity, 0) AS fbs_quantity,
        COALESCE(sa.supply_quantity, 0) AS supply_quantity,
        COALESCE(ma.fbo_in_quantity, 0) AS fbo_in_quantity,
        COALESCE(ma.fbo_out_quantity, 0) AS fbo_out_quantity,
        COALESCE(ma.defects_in_quantity, 0) AS defects_in_quantity,
        COALESCE(ma.defects_out_quantity, 0) AS defects_out_quantity,
        COALESCE(rs.resort_quantity, 0) AS resort_quantity,
        COALESCE(rs.resort_delta, 0) AS resort_delta,
        COALESCE(ws.fbs, 0) AS fbs_stocks
    FROM products_list pl
    CROSS JOIN dates d
    LEFT JOIN fbs_agg fa
        ON fa.product_id = pl.product_id
    AND fa.dt = d.dt
    LEFT JOIN supply_agg sa
        ON sa.product_id = pl.product_id
    AND sa.dt = d.dt
    LEFT JOIN movements_agg ma
        ON ma.product_id = pl.product_id
    AND ma.dt = d.dt
    LEFT JOIN resorts rs
        ON rs.product_id = pl.product_id
    AND rs.dt = d.dt
    LEFT JOIN wms_stocks ws
        ON ws.product_id = pl.product_id
    AND ws.dt = d.dt
    ORDER BY
        pl.product_id,
        d.dt;
        """
)

STOCKS_COLUMNS = [
    "balance_date",
    "product_id",
    "stock_qty",
    "fbs",
    "receiving",
    "packing",
    "shortage",
    "fbo",
    "defects",
    "storage",
]

STOCKS_DATABASE_QUERY = text(
    """
    WITH products_list AS (
        SELECT unnest(ARRAY[
            'wild163','wild359','wild188901','wild188601',
            'wild1890','wild1893','wild1895','wild1896', 'wild1886',
            'wild1894','wild1884','wild1969','wild1970', 'wild1891',
            'wild1971','wild1972','wild1973','wild1974', 'wild1889',
            'wild1975','wild1976','wild1977','wild1978',
            'wild1979','wild1980','wild1981','wild1982',
            'wild1983','wild2082','wild2083','wild2093',
            'wild2094','wild2123','wild2132', 'wild1892'
        ]) AS product_id
    )
    SELECT
        ws.balance_date,
        ws.product_id,
        ws.stock_qty,
        ws.fbs,
        ws.receiving,
        ws.packing,
        ws.shortage,
        ws.fbo,
        ws.defects,
        ws."storage"
    FROM wms_stock AS ws
    JOIN products_list AS p
        ON p.product_id = ws.product_id
    WHERE ws.balance_date > '2026-08-31'::date;
    """
)
