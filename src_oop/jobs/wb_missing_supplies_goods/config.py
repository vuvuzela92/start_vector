"""Настройки загрузки товаров поставок WB."""

from __future__ import annotations

from datetime import timedelta

SUPPLIES_URL = "https://supplies-api.wildberries.ru/api/v1/supplies"
SUPPLY_GOODS_URL_TEMPLATE = (
    "https://supplies-api.wildberries.ru/api/v1/supplies/{supply_id}/goods"
)
SUPPLY_DETAIL_URL_TEMPLATE = (
    "https://supplies-api.wildberries.ru/api/v1/supplies/{supply_id}"
)
SUPPLY_PAGE_SIZE = 1000
GOODS_PAGE_SIZE = 1000
LOOKBACK_PERIOD = timedelta(days=30)
REQUEST_TIMEOUT_SECONDS = 40
MAX_RETRIES = 5
RETRY_BASE_DELAY_SECONDS = 2
MAX_CONCURRENT_ACCOUNTS = 2
REQUEST_DELAY_SECONDS = 2.1

GOODS_COLUMN_MAPPING: dict[str, str] = {
    "ID": "id",
    "barcode": "barcode",
    "vendorCode": "vendor_code",
    "nmID": "nm_id",
    "needKiz": "need_kiz",
    "tnved": "tnved",
    "techSize": "tech_size",
    "color": "color",
    "supplierBoxAmount": "supplier_box_amount",
    "quantity": "quantity",
    "readyForSaleQuantity": "ready_for_sale_quantity",
    "unloadingQuantity": "unloading_quantity",
    "acceptedQuantity": "accepted_quantity",
}

SUPPLY_COLUMN_MAPPING: dict[str, str] = {
    "ID": "id",
    "phone": "phone",
    "statusID": "status_id",
    "boxTypeID": "box_type_id",
    "createDate": "create_date",
    "supplyDate": "supply_date",
    "factDate": "fact_date",
    "updatedDate": "updated_date",
    "warehouseID": "warehouse_id",
    "warehouseName": "warehouse_name",
    "actualWarehouseID": "actual_warehouse_id",
    "actualWarehouseName": "actual_warehouse_name",
    "transitWarehouseID": "transit_warehouse_id",
    "transitWarehouseName": "transit_warehouse_name",
    "acceptanceCost": "acceptance_cost",
    "paidAcceptanceCoefficient": "paid_acceptance_coefficient",
    "rejectReason": "reject_reason",
    "supplierAssignName": "supplier_assign_name",
    "storageCoef": "storage_coef",
    "deliveryCoef": "delivery_coef",
    "quantity": "quantity",
    "readyForSaleQuantity": "ready_for_sale_quantity",
    "acceptedQuantity": "accepted_quantity",
    "unloadingQuantity": "unloading_quantity",
    "depersonalizedQuantity": "depersonalized_quantity",
}
