"""Конфигурация запросов и целевых листов расчета закупки России.

Модуль не выполняет запросы и не обращается к Google Sheets самостоятельно.
Он хранит SQL-витрины, названия целевых таблиц и правила сопоставления полей,
которые используются job расчета закупки России. Такое разделение позволяет
изменять структуру выгрузки отдельно от orchestration-кода.
"""

from dataclasses import dataclass

from sqlalchemy import text

# Сводка заказов за последние семь дней для блока воронки продаж.
# Запрос оставлен строкой, потому что используется legacy-участком модуля.
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

# Витрина сопоставления заказов поставщикам, приходов и возвратов.
# Результат используется листом «Заказы_и_поступления».
# В выборку попадают только проведенные и корректные документы без служебных
# товаров «УТ», созданные после начала текущего периода учета.
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

# Сумма штрафов по основаниям брака, невыполненного заказа и подмены.
# Вторая часть расчета использует эти значения для корректировки виртуального
# остатка по внутреннему артикулу поставщика.
query_penalties_and_virtual_stock = text("""
    SELECT
        sum(f.penalty) AS sum,
        a.local_vendor_code
    FROM daily_fin_reports_full f
    LEFT JOIN article a
        USING(nm_id)
    WHERE f.create_dt BETWEEN CURRENT_DATE - INTERVAL '7 days'
      AND CURRENT_DATE
      AND f.penalty != 0
      AND f.bonus_type_name ILIKE ANY (
          ARRAY[
              '%брак%',
              '%невыполненный%',
              '%подмена%'
          ]
      )
    GROUP BY a.local_vendor_code;
""")

# Приходы из 1С для листа «Приходы_1С».
# Фильтр is_valid исключает неактуальные документы, а дата обновления задает
# минимальный исторический период, который поддерживается в выгрузке.
# transport_number и truck_number добавляются в конец legacy-порядка колонок.
query_supplies_1c = text("""
    SELECT
        id,
        guid,
        document_number,
        document_created_at,
        supply_date,
        local_vendor_code,
        product_name,
        event_status,
        quantity,
        amount_with_vat,
        amount_without_vat,
        supplier_name,
        supplier_code,
        update_document_datetime,
        author_of_the_change,
        our_organizations_name,
        is_valid,
        amount_with_vat / NULLIF(quantity, 0) AS price_per_item,
        planned_cost,
        transport_number,
        truck_number
    FROM public.supply_to_sellers_warehouse
    WHERE is_valid = TRUE
      AND update_document_datetime >= DATE '2025-05-01';
""")

# Заказы поставщикам из 1С для листа «Заказы_поставщиков_1С».
# Приходные операции присоединяются по идентификатору заказа; LEFT JOIN
# сохраняет заказы, для которых приход еще не создан.
query_orders_1c = text("""
    SELECT
        main.id,
        main.guid,
        main.document_number,
        main.document_created_at::date AS document_created_at,
        main.supply_date::date AS supply_date,
        main.local_vendor_code,
        main.product_name,
        main.event_status,
        main.quantity,
        main.amount_with_vat,
        main.amount_without_vat,
        main.supplier_name,
        main.supplier_code,
        main.update_document_datetime::date AS update_document_datetime,
        main.author_of_the_change,
        main.our_organizations_name,
        main.warehouse_id,
        main.is_valid,
        main.in_acceptance,
        main.created_at::date AS created_at,
        main.is_printed_barcode,
        main.acceptance_completed,
        main.expected_receipt_date::date AS expected_receipt_date,
        main.actual_quantity,
        main.currency,
        main.unit_price,
        main.last_purchase_price,
        main.last_purchase_supplier,
        main.payment_indicator,
        main.payment_document_number,
        main.shipment_date::date AS shipment_date,
        main.receipt_transaction_number,
        main.cancelled_due_to,
        main.comment,
        main.planned_cost,
        receipts.reciept_number,
        receipts.reciept_date::date AS receipt_date,
        receipts.reciept_quantity,
        receipts.reciept_amount
    FROM ordered_goods_from_buyers AS main
    LEFT JOIN receipts_for_ordered_goods_from_buyers AS receipts
        ON main.id = receipts.ordered_goods_from_buyers_id
    WHERE main.is_valid = TRUE
      AND main.update_document_datetime >= DATE '2025-05-01';
""")

# Поставки WB для листа «БД_поставки».
# Источником являются только последние два месяца. Запись выбирается по паре
# поставка-артикул, а наиболее свежая версия определяется по updated_date и
# created_at. Артикул продавца всегда берется из article.local_vendor_code,
# даже если wsg.vendor_code пустой.
query_wb_supplies = text("""
    SELECT DISTINCT ON (wsg.id, wsg.vendor_code)
        wsg.id AS "Номер поставки",
        ws.supply_date AS "Плановая дата поставки",
        ws.fact_date AS "Фактическая дата поставки",
        ws.status_id AS "Статус",
        wsg.quantity AS "Добавлено в поставку",
        wsg.unloading_quantity AS "Раскладывается",
        wsg.accepted_quantity AS "Принято, шт",
        wsg.ready_for_sale_quantity AS "Поступило в продажу",
        a.local_vendor_code AS "Артикул продавца",
        wsg.nm_id AS "Артикул WB",
        wsg.supplier_box_amount AS "Указано в упаковке, шт",
        a.account
    FROM wb_supplies_goods wsg
    LEFT JOIN wb_supplies ws
        ON wsg.id = ws.id
    LEFT JOIN article a
        USING(nm_id)
    WHERE ws.create_date >= NOW() - INTERVAL '2 months'
    ORDER BY wsg.id, wsg.vendor_code, ws.updated_date DESC, wsg.created_at DESC;
""")

# Названия Google Sheets и листов, используемых расчетом закупки России.
# Имена листов являются частью внешнего контракта и должны совпадать с Google
# Sheets; изменение этих значений может направить данные в другой лист.
google_table = {
    "title": "Расчет закупки Россия",
    "calculate_sheet": "Расчет закупки",
    "orders_sheet": "БД_Заказы",
    "statuses_sheet": "Статичный лист статусы",
    "orders_buyers_sheet": "Заказы_и_поступления",
    "orders_1c_sheet": "Заказы_поставщиков_1С",
    "supplies_1c_sheet": "Приходы_1С",
    "wb_supplies_sheet": "БД_поставки",
}


@dataclass(frozen=True, slots=True)
class Supplies1CTargetSheet:
    """Описывает целевой лист для публикации выгрузки `Приходы_1С`.

    Бизнес-сценарий:
    одна и та же витрина приходов 1С теперь нужна в нескольких Google-таблицах,
    поэтому конфигурация хранит и человекочитаемое имя таблицы, и стабильный
    `spreadsheet_id`, чтобы job писала в правильный документ даже при
    неуникальных названиях.
    """

    table_title: str
    spreadsheet_id: str | None
    sheet_title: str


# Дополнительные реквизиты целевых Google Sheets для публикации приходов 1С.
# Для основной таблицы используется поиск по названию, для китайской таблицы
# задан стабильный spreadsheet_id, чтобы исключить неоднозначность названий.
supplies_1c_target_sheets = (
    Supplies1CTargetSheet(
        table_title="Расчет закупки Россия",
        spreadsheet_id=None,
        sheet_title="Приходы_1С",
    ),
    Supplies1CTargetSheet(
        table_title="Расчет поставки Китай_по обороту",
        spreadsheet_id="1fXiijP8vMYv8vEFN1BnnTcqioCT_P2t1tgogi_ATh_8",
        sheet_title="Приходы_1С",
    ),
)

# Настройки листа UNIT 2.0, который используется другими расчетами модуля.
unit_table = {
    "title": "UNIT 2.0 (tested)",
    "sheet_unit": "MAIN (tested)",
}

# Заголовки расчетных колонок, в которые основной job передает дополнительные
# показатели после объединения данных из PostgreSQL.
penalties_column_name = "Штраф (по основанию брак, невыполненный заказ и подмена)"
virtual_stock_column_name = "Кол-во товара по виртуальным остаткам"

# Соответствие технических колонок БД и пользовательских заголовков листа
# «Приходы_1С». Ключи должны совпадать с именами полей query_supplies_1c.
supply_1c_columns_rename = {
    "document_number": "Номер документа",
    "document_created_at": "Дата создания документа",
    "supply_date": "Дата поставки",
    "local_vendor_code": "Внутренний код поставщика",
    "product_name": "Наименование товара",
    "event_status": "Статус события",
    "quantity": "Количество",
    "amount_with_vat": "Сумма с НДС",
    "amount_without_vat": "Сумма без НДС",
    "supplier_name": "Название поставщика",
    "supplier_code": "Код поставщика",
    "update_document_datetime": "Дата обновления документа",
    "author_of_the_change": "Автор изменения",
    "our_organizations_name": "Название нашей организации",
    "is_valid": "Корректность документа",
    "price_per_item": "Цена за единицу",
    "planned_cost": "Плановая себестоимость",
    "transport_number": "Номер машины",
    "truck_number": "Номер трака",
}

# Итоговый порядок колонок для legacy-совместимой выгрузки листа «Приходы_1С».
# Новые транспортные реквизиты намеренно находятся в конце, чтобы не сместить
# существующие колонки и не сломать формулы Google Sheets.
supply_1c_output_columns = [
    "Номер документа",
    "Дата создания документа",
    "Дата поставки",
    "Внутренний код поставщика",
    "Наименование товара",
    "Статус события",
    "Количество",
    "Сумма с НДС",
    "Сумма без НДС",
    "Название поставщика",
    "Код поставщика",
    "Дата обновления документа",
    "Автор изменения",
    "Название нашей организации",
    "Корректность документа",
    "Цена за единицу",
    "Плановая себестоимость",
    "Номер машины",
    "Номер трака",
]

# Соответствие полей заказов 1С и заголовков legacy-листа
# «Заказы_поставщиков_1С». Порядок публикации формируется отдельно в job.
orders_1c_columns_rename = {
    "document_number": "Номер документа",
    "document_created_at": "Дата создания документа",
    "supply_date": "Дата поставки",
    "local_vendor_code": "Внутренний код поставщика",
    "product_name": "Наименование товара",
    "event_status": "Статус события",
    "quantity": "Количество",
    "amount_with_vat": "Сумма с НДС",
    "amount_without_vat": "Сумма без НДС",
    "supplier_name": "Название поставщика",
    "supplier_code": "Код поставщика",
    "update_document_datetime": "Дата обновления документа",
    "author_of_the_change": "Автор изменения",
    "our_organizations_name": "Название нашей организации",
    "warehouse_id": "ID склада",
    "is_valid": "Корректность документа",
    "in_acceptance": "В приёмке",
    "created_at": "Дата создания записи",
    "is_printed_barcode": "Штрихкод напечатан",
    "acceptance_completed": "Приёмка завершена",
    "expected_receipt_date": "Ожидаемая дата поступления",
    "actual_quantity": "Фактическое количество",
    "currency": "Валюта",
    "unit_price": "Цена за единицу",
    "last_purchase_price": "Цена последней закупки",
    "last_purchase_supplier": "Поставщик последней закупки",
    "payment_indicator": "Признак/способ оплаты",
    "payment_document_number": "Номер платёжного документа",
    "shipment_date": "Дата отгрузки",
    "receipt_transaction_number": "Номер приходной операции",
    "cancelled_due_to": "Причина аннулирования",
    "comment": "Комментарий",
    "planned_cost": "Плановая себестоимость",
    "reciept_number": "Номер чека",
    "receipt_date": "Дата чека",
    "reciept_quantity": "Чек кол-во",
    "reciept_amount": "Чек стоимость",
}

# Значения статусов WB переводятся в пользовательские названия для листа
# «БД_поставки». Неизвестный статус не должен молча считаться принятым.
wb_supply_statuses = {
    1: "Не запланировано",
    2: "Запланировано",
    3: "Отгрузка разрешена",
    4: "Идёт приёмка",
    5: "Принято",
    6: "Отгружено на воротах",
}
