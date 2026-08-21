autopilot_gs = {
    "title": "Панель управления продажами Вектор",
    "ic_sheet": "ИУ_ИНФО",
    "hourly_sheet": "Автопилот",
}

unit_gs = {
    "title": "UNIT 2.0 (tested)",
    "unit_sheet": "MAIN (tested)",
}

FUNNEL_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
WB_PRICES_URL = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
WB_CARD_DETAIL_URL = "https://card.wb.ru/cards/v4/detail"
COMETA_AUTOPILOTS_URL = "https://api.e-comet.io/v1/autopilots"

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
RETRY_BASE_SLEEP_SECONDS = 10
RETRY_MAX_SLEEP_SECONDS = 60
MAX_CONCURRENT_ACCOUNTS = 4
MAX_CONCURRENT_PUBLIC_CARD_REQUESTS = 8
WB_PRICE_PAGE_LIMIT = 1000

# Временно отключает онлайн-парсинг цен, СПП, рейтинга и запись spp_history через WB public card.
ENABLE_WB_ONLINE_PRICE_PARSING = False

# Коэффициент сохранен из legacy: расходы Cometa budget_spent_today умножаются на 1.1.
COMETA_SPEND_MULTIPLIER = 1.1

AUTOPILOT_VALUES_FIRST_ROW = 4
AUTOPILOT_DATE_COLUMN_OFFSET = 7
AUTOPILOT_STATUS_CELL = "A2"
AUTOPILOT_PERCENT_COLUMNS_RANGE = "BE:BF"
AUTOPILOT_PERCENT_COLUMNS_FORMAT = {
    "numberFormat": {
        "type": "PERCENT",
        "pattern": "0.00%",
    },
}
AUTOPILOT_DAILY_PERCENT_COLUMNS_RANGES = ("BE:BF", "BX:BX")

UNIT_ARTICLE_COLUMN_NAME = "Артикул"
UNIT_EXPECTED_REMAINS_HEADER = "Свободный остаток\n(сервис)"
UNIT_MARGIN_COLUMN_NAME = "Мар"

# Часть метрик является текущим снимком, а не дневным рядом, поэтому пишется в базовую колонку без смещения.
METRICS_WITHOUT_DATE_OFFSET = {"unit_free_stock"}

# Метрики ПУ пишутся в фиксированные колонки. Значение смещается на AUTOPILOT_DATE_COLUMN_OFFSET.
METRIC_TO_BASE_COLUMN: dict[str, str] = {
    "orders_sum_rub": "AX",
    "orders_count": "BI",
    "adv_spend": "BQ",
    "full_price": "CD",
    "spp": "CW",
    "total_quantity": "DN",
    "profit_by_cond_orders": "DW",
    "views": "EW",
    "clicks": "FF",
    "ctr": "FN",
    "to_cart_convers": "FV",
    "to_orders_convers": "GD",
    "add_to_cart_count": "GL",
    "open_card_count": "GT",
    "cpo": "HB",
    "cpc": "HJ",
    "rating": "HR",
    "cpm": "HZ",
    "promo_status": "DF",
    "net_profit_after_ad": "EE",
    "advertising_cost_share": "EN",
    "organic": "II",
    "unit_free_stock": "DU",
    "discounted_price": "CK",
}
