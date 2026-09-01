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
    "fbo_count",
    "returns_in_count",
    "returns_out_count",
    "resort_count",
    "fbs_stocks",
]

SHIPMENTS_DATABASE_QUERY = text(
    """
    WITH dates AS (
        SELECT generate_series(
            '2026-07-29'::date,
            CURRENT_DATE,
            '1 day'::interval
        )::date AS dt
    ),
    products_list AS (
        SELECT unnest(ARRAY[
            'wild163', 'wild359', 'wild188901', 'wild188601',
            'wild1890', 'wild1893', 'wild1895', 'wild1896',
            'wild1886', 'wild1889', 'wild1891', 'wild1894',
            'wild1884', 'wild1969', 'wild1970', 'wild1971',
            'wild1972', 'wild1973', 'wild1974', 'wild1975',
            'wild1976', 'wild1977', 'wild1978', 'wild1979',
            'wild1980', 'wild1981', 'wild1982', 'wild1983',
            'wild2082', 'wild2083', 'wild2093', 'wild2094',
            'wild2123', 'wild2132', 'wild1892'
        ]) AS product_id
    ),
    fbs_agg AS (
        SELECT
            s.product_id,
            s.shipment_date::date AS dt,
            SUM(s.quantity) AS fbs_quantity
        FROM wms.fbs_shipment_items AS s
        WHERE s.status = 'success'
          AND s.shipment_date >= '2026-07-29'::date
          AND s.product_id IN (SELECT product_id FROM products_list)
        GROUP BY s.product_id, s.shipment_date::date
    ),
    supply_agg AS (
        SELECT
            sw.local_vendor_code AS product_id,
            sw.document_created_at::date AS dt,
            SUM(sw.quantity) AS supply_quantity
        FROM supply_to_sellers_warehouse AS sw
        WHERE sw.document_created_at >= '2026-07-29'::date
          AND sw.local_vendor_code IN (SELECT product_id FROM products_list)
          AND sw.is_valid IS TRUE
        GROUP BY sw.local_vendor_code, sw.document_created_at::date
    ),
    fbo_move AS (
        SELECT
            m.product_id,
            m.created_at::date AS dt,
            SUM(m.quantity) AS fbo_count
        FROM wms.movements AS m
        WHERE m.to_location_id = 36
          AND m.created_at >= '2026-07-29'::date
          AND m.product_id IN (SELECT product_id FROM products_list)
        GROUP BY m.product_id, m.created_at::date
    ),
    returns_in AS (
        SELECT
            m.product_id,
            m.created_at::date AS dt,
            SUM(m.quantity) AS returns_in_count
        FROM wms.movements AS m
        WHERE m.to_location_id = 37
          AND m.created_at >= '2026-07-29'::date
          AND m.product_id IN (SELECT product_id FROM products_list)
        GROUP BY m.product_id, m.created_at::date
    ),
    returns_out AS (
        SELECT
            m.product_id,
            m.created_at::date AS dt,
            SUM(m.quantity) AS returns_out_count
        FROM wms.movements AS m
        WHERE m.from_location_id = 37
          AND m.created_at >= '2026-07-29'::date
          AND m.product_id IN (SELECT product_id FROM products_list)
        GROUP BY m.product_id, m.created_at::date
    ),
    resorts AS (
        SELECT
            rs.product_id,
            rs.movement_created_at::date AS dt,
            SUM(
                CASE
                    WHEN rs.role = 'target_incoming' THEN rs.quantity
                    WHEN rs.role = 'source_outgoing' THEN -rs.quantity
                    ELSE 0
                END
            ) AS resort_count
        FROM wms.re_sorting_operation_items AS rs
        WHERE rs.movement_created_at >= '2026-07-29'::date
          AND rs.product_id IN (SELECT product_id FROM products_list)
        GROUP BY rs.product_id, rs.movement_created_at::date
    ),
    wms_stocks AS (
        SELECT
            ws.balance_date,
            ws.product_id,
            ws.fbs
        FROM wms_stock AS ws
        WHERE ws.balance_date >= '2026-07-29'::date
          AND ws.product_id IN (SELECT product_id FROM products_list)
    )
    SELECT
        pl.product_id AS id,
        d.dt AS date,
        COALESCE(fa.fbs_quantity, 0) AS fbs_quantity,
        COALESCE(sa.supply_quantity, 0) AS supply_quantity,
        COALESCE(fm.fbo_count, 0) AS fbo_count,
        COALESCE(ri.returns_in_count, 0) AS returns_in_count,
        COALESCE(ro.returns_out_count, 0) AS returns_out_count,
        COALESCE(rs.resort_count, 0) AS resort_count,
        COALESCE(ws.fbs, 0) AS fbs_stocks
    FROM products_list AS pl
    CROSS JOIN dates AS d
    LEFT JOIN fbs_agg AS fa
        ON fa.product_id = pl.product_id AND fa.dt = d.dt
    LEFT JOIN supply_agg AS sa
        ON sa.product_id = pl.product_id AND sa.dt = d.dt
    LEFT JOIN fbo_move AS fm
        ON fm.product_id = pl.product_id AND fm.dt = d.dt
    LEFT JOIN returns_in AS ri
        ON ri.product_id = pl.product_id AND ri.dt = d.dt
    LEFT JOIN returns_out AS ro
        ON ro.product_id = pl.product_id AND ro.dt = d.dt
    LEFT JOIN resorts AS rs
        ON rs.product_id = pl.product_id AND rs.dt = d.dt
    LEFT JOIN wms_stocks AS ws
        ON ws.product_id = pl.product_id AND ws.balance_date = d.dt
    ORDER BY pl.product_id, d.dt;
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
            'wild163', 'wild359', 'wild188901', 'wild188601',
            'wild1890', 'wild1893', 'wild1895', 'wild1896',
            'wild1886', 'wild1894', 'wild1884', 'wild1969',
            'wild1970', 'wild1971', 'wild1972', 'wild1973',
            'wild1974', 'wild1975', 'wild1976', 'wild1977',
            'wild1978', 'wild1979', 'wild1980', 'wild1981',
            'wild1982', 'wild1983', 'wild2082', 'wild2083',
            'wild2093', 'wild2094', 'wild2123', 'wild2132', 'wild1892'
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
