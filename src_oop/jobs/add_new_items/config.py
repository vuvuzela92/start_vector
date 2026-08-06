from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias


ColumnAlias: TypeAlias = str | tuple[str, ...]


NEW_ITEM_STATUS_TO_PROCESS = "добавить"


@dataclass(frozen=True, slots=True)
class WorksheetConfig:
    """Настройки подключения к конкретной вкладке Google Sheets."""

    table_title: str
    sheet_title: str
    header_row: int


@dataclass(frozen=True, slots=True)
class InputColumns:
    """Названия обязательных колонок входной таблицы."""

    supplier_name: ColumnAlias = ("Поставщик", "Поставщик Рынок Ксяоми или?")
    sku: ColumnAlias = ("Артикул цифр", "Артикул")
    client: ColumnAlias = "Магазин"
    supplier_code_duplicates: ColumnAlias = ("Артикул", "Артикул поставщика", "Артикул цифр")
    status: ColumnAlias = "Статус"
    item_name: ColumnAlias = ("Наименование", "Наименование товара")
    category: ColumnAlias = ("предмет", "Предмет")
    supplier_code_unique: ColumnAlias = "wild"
    purchase_price: ColumnAlias = ("Стоимость в закупке (руб.)", "Закупочная цена")
    manager: ColumnAlias = "Ответственный менеджер"
    added_to_unit_main: ColumnAlias = "Добавлено в MAIN (tested)"
    added_to_autopilot: ColumnAlias = "Добавлено в Автопилот"
    added_to_products: ColumnAlias = "Добавлено в products"


@dataclass(frozen=True, slots=True)
class AddNewItemsSheetsConfig:
    """Конфиг всех таблиц, участвующих в процессе добавления товара."""

    new_items: WorksheetConfig
    sopost: WorksheetConfig
    unit_main: WorksheetConfig
    autopilot: WorksheetConfig
    competitors: WorksheetConfig


SHEETS = AddNewItemsSheetsConfig(
    new_items=WorksheetConfig(
        table_title="Новый товар",
        sheet_title="Для юнит",
        header_row=1,
    ),
    sopost=WorksheetConfig(
        table_title="UNIT 2.0 (tested)",
        sheet_title="Сопост",
        header_row=1,
    ),
    unit_main=WorksheetConfig(
        table_title="UNIT 2.0 (tested)",
        sheet_title="MAIN (tested)",
        header_row=1,
    ),
    autopilot=WorksheetConfig(
        table_title="Панель управления продажами Вектор",
        sheet_title="Автопилот",
        header_row=3,
    ),
    competitors=WorksheetConfig(
        table_title="UNIT 2.0 (tested)",
        sheet_title="Конкуренты",
        header_row=1,
    ),
)


INPUT_COLUMNS = InputColumns()

# Колонки для вставки в Сопост идут подряд от "предмет" до "Добавляем".
SOPOST_INSERT_HEADERS = (
    "предмет",
    "Наименование",
    "wild",
    "Артикул продавца",
    "Стоимость в закупке (руб.)",
    "Добавляем",
)

# В MAIN (tested) значения вставляются подряд от артикула до wild.
UNIT_MAIN_INSERT_HEADERS = (
    "Артикул",
    "ЛК",
    "wild",
)

# В Автопилоте legacy-логика пишет первые четыре колонки строки:
# артикул, категория, клиент, wild.
AUTOPILOT_INSERT_INDEXES = (0, 1, 2, 3)

COMPETITORS_WILD_HEADER = "Укажи вилд"
SOPOST_WILD_HEADER = "wild"
UNIT_MAIN_SKU_HEADER = "Артикул"
AUTOPILOT_SKU_HEADER = "Артикул"

# Значение 100 соответствует ручной пометке "добавляем товар" в Сопост.
SOPOST_ADD_FLAG = 100

STATUS_YES = "да"
STATUS_NO = "нет"
