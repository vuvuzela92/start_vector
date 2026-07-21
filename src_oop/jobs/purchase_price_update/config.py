from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text


@dataclass(frozen=True, slots=True)
class GoogleSheetConfig:
    """
    Конфигурация одного листа Google Sheets.

    Назначение:
    хранит минимальный набор параметров, который нужен задаче для чтения
    или записи конкретного листа без размазывания этих настроек по коду.

    Параметры:
    `table_title` — имя Google-таблицы.
    `sheet_title` — имя листа внутри таблицы.
    `header_row_index` — индекс строки с заголовками в терминах Python, начиная с нуля.
    `data_row_index` — индекс первой строки с данными в терминах Python, начиная с нуля.

    Особенности:
    объект неизменяемый, чтобы конфигурацию нельзя было случайно поменять
    во время выполнения задачи.
    """

    table_title: str
    sheet_title: str
    header_row_index: int
    data_row_index: int

# Конфигурация основного рабочего листа, где хранится текущая закупочная цена
# и откуда берется строка для точечного обновления по артикулу `wild`.
UNIT_SHEET_CONFIG = GoogleSheetConfig(
    table_title="UNIT 2.0 (tested)",
    sheet_title="Сопост",
    header_row_index=0,
    data_row_index=1,
)

# Конфигурация отдельного листа-истории, куда задача добавляет отчет
# по всем найденным изменениям закупочной цены.
REPORT_SHEET_CONFIG = GoogleSheetConfig(
    table_title="Новый товар",
    sheet_title="UNIT: Изменение закупочной цены",
    header_row_index=0,
    data_row_index=1,
)

# Запрос закупочной стоимости
PURCHASE_PRICE_UPDATE_QUERY = text(
    """
    WITH latest_purchase_price AS (
        SELECT DISTINCT ON (local_vendor_code)
            supply_date,
            guid,
            document_number,
            local_vendor_code,
            product_name,
            amount_with_vat,
            quantity,
            ROUND(amount_with_vat / quantity, 2) AS latest_price_per_item,
            currency,
            planned_cost,
            supplier_name
        FROM supply_to_sellers_warehouse
        WHERE is_valid = TRUE
          AND local_vendor_code LIKE 'wild%'
          AND supplier_name != 'РВБ ООО'
          AND quantity != 0
          AND supply_date >= CURRENT_DATE - (:days_count * INTERVAL '1 day')
          AND supply_date < CURRENT_DATE
        ORDER BY local_vendor_code, supply_date DESC
    )
    SELECT
        lpp.supply_date,
        lpp.guid,
        lpp.document_number,
        lpp.local_vendor_code,
        lpp.product_name,
        lpp.amount_with_vat,
        lpp.quantity,
        lpp.latest_price_per_item,
        CASE
            WHEN
                (
                    lpp.currency IS NOT NULL
                    AND lpp.currency != '643'
                )
                OR lpp.supplier_name ILIKE '%ZILOL"%'
            THEN lpp.planned_cost
            ELSE lpp.latest_price_per_item
        END AS price_per_item,
        lpp.currency,
        lpp.planned_cost,
        CASE
            WHEN
                (
                    (lpp.currency IS NOT NULL AND lpp.currency != '643')
                    OR lpp.supplier_name ILIKE '%ZILOL%'
                )
                AND (lpp.planned_cost IS NULL OR lpp.planned_cost = 0)
            THEN 'ALARM: planned_cost missing'
            ELSE NULL
        END AS alarm_flag
    FROM latest_purchase_price lpp
    ORDER BY lpp.local_vendor_code
    """
)

# Корень проекта нужен, чтобы строить стабильные относительные пути
# к локальным служебным файлам без зависимости от текущей рабочей директории.
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Локальный CSV-файл со snapshot изменившихся строк.
# Нужен для быстрой диагностики после боевого запуска и для разбора инцидентов.
LOCAL_REPORT_PATH = PROJECT_ROOT / "logs" / "changed_purchase_prices.csv"

# Названия колонок вынесены в конфиг, чтобы не разносить строковые литералы по коду.
# Колонка с закупочной ценой, которую задача сравнивает и обновляет в `Сопост`.
PURCHASE_PRICE_COLUMN = "Стоимость в закупке (руб.)"

# Главный business key для сопоставления строки из БД со строкой в Google Sheets.
ARTICLE_COLUMN = "wild"

# Флаг в UNIT, который запрещает автоматически менять закупочную цену для SKU.
NEVER_CHANGE_PRICE_COLUMN = "Неизменяемая цена"

# Минимальный набор колонок, который обязан прийти из SQL-запроса.
# Если чего-то не хватает, задачу безопаснее остановить, чем продолжать расчет.
REQUIRED_DB_COLUMNS = (
    "supply_date",
    "guid",
    "document_number",
    "local_vendor_code",
    "product_name",
    "quantity",
    "price_per_item",
)

# Минимальный набор колонок, который обязан существовать в листе `Сопост`.
# Эти поля нужны для сопоставления, чтения текущей цены и проверки блокировки цены.
REQUIRED_UNIT_COLUMNS = (
    ARTICLE_COLUMN,
    PURCHASE_PRICE_COLUMN,
    NEVER_CHANGE_PRICE_COLUMN,
)

# Фиксированный порядок колонок в отчете об изменении закупочной цены.
# Используется и для локального CSV, и для записи в отдельный лист истории.
REPORT_COLUMNS = (
    "product_name",
    "local_vendor_code",
    "guid",
    "document_number",
    "quantity",
    "unit_price",
    "price_per_item",
    "price_diff_rub",
    "supply_date",
    "insert_date",
)

# Порог, после которого изменение цены считается подозрительным и попадает в warning-лог.
# Значение 0.25 означает 25% отклонения относительно текущей цены в UNIT.
SUSPICIOUS_DIFF_THRESHOLD = 0.25

# Окно выборки из БД по умолчанию.
# Сейчас задача смотрит изменения за последние 2 дня, как и в legacy-сценарии.
DEFAULT_LOOKBACK_DAYS = 2

# Размер одного пакета записи в Google Sheets.
# Нужен, чтобы не отправлять слишком много обновлений одним большим запросом.
DEFAULT_BATCH_SIZE = 200

# Параметры ограниченного retry для операций записи в Google Sheets.
# Повторяем только временные ошибки и только ограниченное число раз.
# Сколько максимум попыток делать для одной операции записи в Google Sheets.
GOOGLE_WRITE_RETRY_ATTEMPTS = 3

# Пауза между повторными попытками записи в Google Sheets в секундах.
GOOGLE_WRITE_RETRY_DELAY_SECONDS = 2

# Список HTTP-статусов, которые считаем временными и безопасными для повторной попытки.
GOOGLE_WRITE_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
