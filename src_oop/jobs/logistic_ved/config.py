"""Конфигурация процессов синхронизации логистики ВЭД.

Бизнес-логика:
- ``CHINA_COLS`` определяет, какие поля закупщиков считаются управляемыми
  для прямой передачи в работу логистам;
- ``CHINA_FILTER_COLS`` задает поля, по которым отбираются строки для прямой
  передачи из таблицы закупщиков в таблицу логистов;
- ``LOGISTIC_TO_CHINA_SYNC_COLS`` определяет, какие поля логисты возвращают
  обратно закупщикам после начала операционной работы по поставке.

Техническая логика:
- словарь ``delivery_calculation_china`` хранит название исходной Google Таблицы
  и листа, из которого читаются данные закупщиков;
- словарь ``ved_logistics_2026`` хранит название целевой Google Таблицы и листа,
  где логисты ведут свою часть процесса.
"""

from sqlalchemy import text

# Колонки, которые прямая синхронизация читает из таблицы закупщиков
# и обновляет в таблице логистов по ключу ORDER_LINE_ID.
CHINA_COLS = [
    "wild",
    "Модель",
    "Номер Трака",
    "КОМПАНИЯ",
    "Фабрика",
    "Поставщик",
    "Статус",
    "Кол-во к заказу",
    "Кол-во коробок",
    "объем партии, м3",
    "вес партии, кг",
    "Сумма заказа, RMB",
    "ORDER_LINE_ID",
]

# Колонки в таблице Заказы белые ТЕСТ, по которым действует фильтр бизнес-готовности.
# Сейчас в работу логистов попадают только строки:
# - с непустым номером трака;
# - со статусом "товар готов к вывозу".
CHINA_FILTER_COLS = ["Номер Трака", "Статус"]

# Колонки, которые логисты заполняют в ОТЧЁТ_2.0 и затем возвращают закупщикам
# в таблицу Заказы белые ТЕСТ во время обратной синхронизации.
LOGISTIC_TO_CHINA_SYNC_COLS = [
    "ФАКТИЧЕСКАЯ ДАТА ОТГРУЗКИ ОТ НАС",
    "Номер ТС",
    "План прибытия на склад",
    "ФАКТИЧЕСКАЯ ДАТА ПРИБЫТИЯ НА СКЛАД ОТ НАС",
]


# Исходная таблица закупщиков.
delivery_calculation_china = {
    "title": "Расчет поставки Китай_по обороту",
    # Стабильный идентификатор таблицы закупщиков. Используется как защита
    # от ручного переименования документа без изменения бизнес-сценария.
    "spreadsheet_id": "1fXiijP8vMYv8vEFN1BnnTcqioCT_P2t1tgogi_ATh_8",
    "white_orders_sheet": "Заказы белые ТЕСТ",
}

# Целевая таблица логистов.
ved_logistics_2026 = {
    "title": "Логистика ВЭД 2026",
    "report_sheet": "ОТЧЁТ_2.0",
}

# SQL-запрос для автоматической сверки приемки с данными из PostgreSQL.
# Из таблицы приходов забираются только валидные строки с заполненными
# номером автомобиля, номером трака и wild. Документы, начинающиеся с "К",
# исключаются, потому что по ним не нужно вести количественный учет.
SUPPLY_ACCEPTANCE_STATUS_QUERY = text(
    """
    SELECT
           btrim(s.transport_number) AS transport_number,
           btrim(s.truck_number) AS truck_number,
           btrim(s.local_vendor_code) AS local_vendor_code,
           s.quantity
    FROM supply_to_sellers_warehouse s
    WHERE s.is_valid IS TRUE
      AND s.truck_number IS NOT NULL
      AND btrim(s.truck_number) <> ''
      AND s.transport_number IS NOT NULL
      AND btrim(s.transport_number) <> ''
      AND s.local_vendor_code IS NOT NULL
      AND btrim(s.local_vendor_code) <> ''
      AND s.document_number IS NOT NULL
      AND NOT starts_with(s.document_number, 'К');
    """
)

# SQL-запрос для заполнения фактической даты прибытия на склад по номеру трака.
# Если по одному траку в БД встретится несколько разных дат, окончательное
# решение принимает код: такие случаи не заполняются автоматически, чтобы не
# подставить в ОТЧЁТ_2.0 неверную дату.
TRUCK_ARRIVAL_DATE_QUERY = text(
    """
    SELECT
           DISTINCT btrim(s.truck_number) AS truck_number,
           DATE(s.supply_date) AS supply_date
    FROM supply_to_sellers_warehouse s
    WHERE s.truck_number IS NOT NULL
      AND btrim(s.truck_number) <> ''
      AND s.is_valid IS TRUE;
    """
)
