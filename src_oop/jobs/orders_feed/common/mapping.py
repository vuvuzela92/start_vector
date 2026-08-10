"""Сопоставление полей WB Order Feed с колонками PostgreSQL."""

from __future__ import annotations

# Явный маппинг сохраняет контракт между camelCase WB API и snake_case витрины.
ORDER_FEED_COLUMN_MAP: dict[str, str] = {
    "nmId": "nm_id",
    "chrtId": "chrt_id",
    "srid": "srid",
    "createdAt": "created_at",
    "updatedAt": "updated_at",
    "status": "status",
    "cancelType": "cancel_type",
    "warehouseName": "warehouse_name",
    "warehouseRegion": "warehouse_region",
    "isMp": "is_mp",
    "destinationCity": "destination_city",
    "destinationDistrict": "destination_district",
    "sellerPrice": "seller_price",
    "isB2b": "is_b2b",
}
