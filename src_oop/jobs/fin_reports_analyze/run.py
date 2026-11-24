import json
import logging
import math
from datetime import date, datetime
from itertools import zip_longest
from pathlib import Path
from zoneinfo import ZoneInfo

import gspread
from gspread_dataframe import set_with_dataframe
from gspread.utils import rowcol_to_a1
from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.fin_reports_analyze.repository import FinReportsAnalyze
from src_oop.storage.google_sheets.google_sheets import fin_rep_analyze

from src_oop.jobs.fin_reports_analyze.queries import (
    query_cash_flow_writeoffs,
    query_deductions_by_month,
    query_in_transit_purchase_prices,
    query_monthly_report,
    query_stock_analyze,
    query_wb_stock_financial_report,
)

logger = logging.getLogger(__name__)
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _get_google_service_account_email() -> str:
    """Возвращает адрес сервисного аккаунта для инструкции по доступу к отчёту.

    Бизнес-сценарий: если Google Sheets запрещает открыть финансовую витрину,
    оператору нужно добавить именно аккаунт из локальных учётных данных job.
    Читается и возвращается только безопасное поле `client_email`; приватный
    ключ и другие чувствительные данные не выводятся.
    """
    credentials_path = Path(__file__).resolve().parents[3] / "creds" / "creds.json"
    try:
        with credentials_path.open(encoding="utf-8") as credentials_file:
            credentials = json.load(credentials_file)
    except (OSError, json.JSONDecodeError):
        logger.warning(
            "Не удалось определить сервисный аккаунт из creds/creds.json; "
            "проверьте локальные учётные данные Google Sheets."
        )
        return "из файла creds/creds.json"

    if not isinstance(credentials, dict):
        return "из файла creds/creds.json"
    return credentials.get("client_email") or "из файла creds/creds.json"


def _open_financial_report_sheet(
    table_title: str,
    sheet_title: str,
    spreadsheet_id: str,
    required_access: str,
) -> GoogleTabs:
    """Открывает лист отчёта и объясняет, как исправить отказ Google 403.

    Бизнес-сценарий: финансовая выгрузка читает остатки из UNIT и записывает
    суммы в финмодель. При отсутствии доступа оператору нужны документ,
    сервисный аккаунт и требуемая роль, чтобы исправить доступ без поиска
    причины в traceback.
    """
    try:
        return GoogleTabs(
            table_title=table_title,
            sheet_title=sheet_title,
            spreadsheet_id=spreadsheet_id,
        )
    except PermissionError as error:
        google_error = error.__cause__
        response = getattr(google_error, "response", None)
        if getattr(response, "status_code", None) != 403:
            raise

        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
        access_message = (
            f"Нет доступа к Google Таблице «{table_title}», лист «{sheet_title}». "
            f"Добавьте сервисный аккаунт {_get_google_service_account_email()} "
            f"в таблицу {spreadsheet_url} с правами {required_access}."
        )
        logger.error(access_message)
        raise PermissionError(access_message) from None


def _read_table_rows_by_headers(
    worksheet: gspread.Worksheet,
    required_headers: tuple[str, ...],
) -> list[dict[str, object]]:
    """Читает столбцы таблицы по заголовкам без жёсткой привязки к колонкам.

    Бизнес-сценарий: листы поставок и справочники могут менять расположение
    полей и число строк. Заголовки берутся из последней закреплённой строки,
    а на листах без закрепления — из первой строки. Данные читаются только
    по нужным столбцам до текущей границы листа.
    """
    header_row = worksheet.frozen_row_count or 1

    header_values = worksheet.row_values(header_row)
    header_columns = {
        str(value).strip(): column_number
        for column_number, value in enumerate(header_values, start=1)
        if str(value).strip() in required_headers
    }
    missing_headers = set(required_headers) - header_columns.keys()
    if missing_headers:
        raise ValueError(
            f"На листе «{worksheet.title}» не найдены обязательные поля: "
            f"{', '.join(sorted(missing_headers))}."
        )

    first_data_row = header_row + 1
    if first_data_row > worksheet.row_count:
        return []

    ranges = [
        f"{rowcol_to_a1(first_data_row, header_columns[header])}:"
        f"{rowcol_to_a1(worksheet.row_count, header_columns[header])}"
        for header in required_headers
    ]
    column_data = worksheet.batch_get(
        ranges,
        value_render_option="UNFORMATTED_VALUE",
    )
    columns = [
        [row[0] if row else None for row in values]
        for values in column_data
    ]
    return [
        dict(zip(required_headers, row, strict=False))
        for row in zip_longest(*columns, fillvalue=None)
    ]


def _parse_finite_number(value: object) -> float | None:
    """Преобразует значение источника в число для финансового расчёта.

    Бизнес-правило: пустые, ошибочные и бесконечные значения цены считаются
    отсутствующими, чтобы такие товары можно было исключить из обеих сумм.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        parsed_value = float(value)
    except (TypeError, ValueError):
        return None
    return parsed_value if math.isfinite(parsed_value) else None


def _get_wms_stock_totals(
    worksheet: gspread.Worksheet,
) -> tuple[float, float, float]:
    """Читает количество и две стоимости остатков склада ВВТ из WMS.

    Бизнес-правило: на листе «WMS остатки» поля определяются по заголовкам,
    а значения берутся из строки SUBTOTAL над ними. Поэтому все три итога
    учитывают одинаковый фильтр листа; фиксированные адреса колонок и длина
    списка товаров не используются. Пустой или некорректный итог прерывает
    обновление финансовой витрины.
    """
    header_row = worksheet.frozen_row_count
    if header_row < 2:
        raise ValueError(
            "На листе «WMS остатки» не удалось определить заголовки "
            "и предшествующую им строку итогов."
        )
    required_headers = (
        "Остаток",
        "Стоимость остатков ФБС по закупочной стоимости",
        "Стоимость остатков ФБС по цене для клиента",
    )
    header_values = worksheet.row_values(header_row)
    header_columns = {
        str(value).strip(): column_number
        for column_number, value in enumerate(header_values, start=1)
        if str(value).strip() in required_headers
    }
    missing_headers = set(required_headers) - header_columns.keys()
    if missing_headers:
        raise ValueError(
            "На листе «WMS остатки» не найдены поля: "
            f"{', '.join(sorted(missing_headers))}."
        )
    total_cells = [
        rowcol_to_a1(header_row - 1, header_columns[header])
        for header in required_headers
    ]
    raw_totals = worksheet.batch_get(
        total_cells,
        value_render_option="UNFORMATTED_VALUE",
    )
    totals: list[float] = []
    for header, values in zip(required_headers, raw_totals, strict=True):
        value = values[0][0] if values and values[0] else None
        total = _parse_finite_number(value)
        if total is None:
            raise ValueError(
                f"На листе «WMS остатки» нет числового итога для «{header}»."
            )
        totals.append(total)
    logger.info(
        "Получены количество и стоимости остатков ВВТ из WMS | cells=%s",
        ",".join(total_cells),
    )
    return totals[0], totals[1], totals[2]


def _get_wms_client_prices_by_wild(
    worksheet: gspread.Worksheet,
    required_wilds: set[str],
) -> dict[str, float | None]:
    """Сопоставляет wild из остатков WB с клиентской ценой за единицу в WMS.

    Бизнес-сценарий: когда в ценовом снимке WB нет положительной цены,
    финансовый отчёт берёт цену за штуку из поля «Цена для клиента» листа
    «WMS остатки». Сопоставление выполняется по коду товара `wild`, а не по
    общей стоимости запасов ВВТ.
    """
    rows = _read_table_rows_by_headers(
        worksheet,
        ("Товар", "Цена для клиента"),
    )
    price_by_wild: dict[str, float | None] = {}
    for row in rows:
        wild = str(row.get("Товар") or "").strip()
        if not wild or wild not in required_wilds:
            continue

        price = _parse_finite_number(row.get("Цена для клиента"))
        if wild in price_by_wild and price_by_wild[wild] != price:
            raise ValueError(
                f"На листе «WMS остатки» найдено несколько разных "
                f"клиентских цен для {wild}."
            )
        price_by_wild[wild] = price
    return price_by_wild


def _get_order_goods_totals(
    unit_spreadsheet: gspread.Spreadsheet,
    statuses: set[str],
    scenario_name: str,
    snapshot_date: date,
) -> tuple[float, float, float]:
    """Считает цену поставщика, себестоимость и цену клиента товара в пути.

    Бизнес-правила: включаются только строки листа заказов с указанными
    статусами. Цена поставщика считается по «Сумма заказа, RMB»,
    умноженной на «Курс юаня», кроме ZILOL FAYZ INVEST, где сумма уже указана
    в рублях. Себестоимость равна количеству по `wild`, умноженному на среднюю
    `purchase_price` из PostgreSQL `cost_price` на дату снимка WB; отсутствие
    такой цены даёт нулевой вклад и предупреждение, как для остатков WB.
    Закупочная цена из «Сопост» или плана используется только для резервной
    оценки цены клиента. Она выбирается построчно, чтобы сохранить разные
    плановые цены одного `wild`. Если цена клиента из WMS отсутствует или не
    положительна, она рассчитывается как закупочная цена × 2. Строки без цены
    для такой оценки исключаются только из суммы по цене клиента.
    """
    order_spreadsheet_id = "1fXiijP8vMYv8vEFN1BnnTcqioCT_P2t1tgogi_ATh_8"
    order_worksheet = _open_financial_report_sheet(
        table_title="Расчет поставки Китай_по обороту",
        sheet_title="Заказы белые ТЕСТ",
        spreadsheet_id=order_spreadsheet_id,
        required_access="читателя",
    ).sheet_title
    order_rows = _read_table_rows_by_headers(
        order_worksheet,
        (
            "wild",
            "Статус",
            "Кол-во к заказу",
            "Себестоимость 1 шт. в руб ПЛАН",
            "Поставщик",
            "Сумма заказа, RMB",
            "Курс юаня",
        ),
    )
    first_data_row = order_worksheet.frozen_row_count + 1
    order_lines_by_wild: dict[str, list[tuple[float, float | None, int]]] = {}
    matching_order_row_count = 0
    supplier_price_total = 0.0
    normalized_statuses = {status.strip().casefold() for status in statuses}
    for row_offset, row in enumerate(order_rows):
        status = str(row.get("Статус") or "").strip().casefold()
        if status not in normalized_statuses:
            continue

        matching_order_row_count += 1
        wild = str(row.get("wild") or "").strip()
        quantity = _parse_finite_number(row.get("Кол-во к заказу"))
        source_row = first_data_row + row_offset
        if not wild:
            raise ValueError(
                f"В строке {source_row} листа «Заказы белые ТЕСТ» "
                f"для сценария «{scenario_name}» указан статус без wild."
            )
        if quantity is None or quantity < 0:
            raise ValueError(
                f"В строке {source_row} листа «Заказы белые ТЕСТ» "
                f"для {wild} указано некорректное количество к заказу "
                f"в сценарии «{scenario_name}»."
            )
        planned_purchase_cost = _parse_finite_number(
            row.get("Себестоимость 1 шт. в руб ПЛАН")
        )
        supplier = str(row.get("Поставщик") or "").strip().casefold()
        order_amount = _parse_finite_number(row.get("Сумма заказа, RMB"))
        if order_amount is None:
            raise ValueError(
                f"В строке {source_row} листа «Заказы белые ТЕСТ» "
                f"для {wild} не указана «Сумма заказа, RMB» "
                f"в сценарии «{scenario_name}»."
            )
        if supplier.startswith("zilol"):
            # Для ZILOL сумма заказа уже указана в рублях.
            supplier_price_total += order_amount
        else:
            currency_rate = _parse_finite_number(row.get("Курс юаня"))
            if currency_rate is None or currency_rate <= 0:
                raise ValueError(
                    f"В строке {source_row} листа «Заказы белые ТЕСТ» "
                    f"для {wild} не указан положительный курс юаня "
                    f"в сценарии «{scenario_name}»."
                )
            supplier_price_total += order_amount * currency_rate
        order_lines_by_wild.setdefault(wild, []).append(
            (quantity, planned_purchase_cost, source_row)
        )

    if not matching_order_row_count:
        raise ValueError(
            f"На листе «Заказы белые ТЕСТ» не найдено строк для сценария "
            f"«{scenario_name}» со статусами: {', '.join(sorted(statuses))}."
        )

    purchase_prices = Database.read_sql_to_dataframe(
        text(query_in_transit_purchase_prices),
        params={
            "snapshot_date": snapshot_date,
            "wilds": list(order_lines_by_wild),
        },
    )
    purchase_price_by_wild = {
        str(row.local_vendor_code).strip(): _parse_finite_number(
            row.average_purchase_price
        )
        for row in purchase_prices.itertuples(index=False)
    }
    purchase_cost_total = 0.0
    missing_purchase_price_wilds: set[str] = set()
    for wild, order_lines in order_lines_by_wild.items():
        purchase_price = purchase_price_by_wild.get(wild) or 0.0
        if not purchase_price and any(quantity for quantity, _, _ in order_lines):
            missing_purchase_price_wilds.add(wild)
        purchase_cost_total += sum(
            quantity * purchase_price for quantity, _, _ in order_lines
        )
    if missing_purchase_price_wilds:
        logger.warning(
            "Для части товара в пути нет себестоимости в cost_price на дату "
            "снимка WB; эти wild учтены с нулевой себестоимостью, расчёт "
            "продолжается | wild_count=%s | wild=%s",
            len(missing_purchase_price_wilds),
            ", ".join(sorted(missing_purchase_price_wilds)),
        )

    match_rows = _read_table_rows_by_headers(
        unit_spreadsheet.worksheet("Сопост"),
        ("wild", "Стоимость в закупке (руб.)"),
    )
    client_price_rows = _read_table_rows_by_headers(
        unit_spreadsheet.worksheet("WMS остатки"),
        ("Товар", "Цена для клиента"),
    )

    def build_price_map(
        rows: list[dict[str, object]],
        wild_header: str,
        price_header: str,
        source_name: str,
        required_wilds: set[str],
    ) -> dict[str, float | None]:
        """Строит однозначное сопоставление артикула и цены из справочника.

        В сценарии поставок помощник защищает финансовую сумму от случайного
        выбора одной из противоречащих друг другу строк с одинаковым wild.
        """
        price_by_wild: dict[str, float | None] = {}
        for source_row in rows:
            wild = str(source_row.get(wild_header) or "").strip()
            if not wild or wild not in required_wilds:
                continue
            price = _parse_finite_number(source_row.get(price_header))
            if wild in price_by_wild and price_by_wild[wild] != price:
                raise ValueError(
                    f"В источнике «{source_name}» найдено несколько разных "
                    f"значений цены для {wild}."
                )
            price_by_wild[wild] = price
        return price_by_wild

    purchase_cost_by_wild = build_price_map(
        match_rows,
        wild_header="wild",
        price_header="Стоимость в закупке (руб.)",
        source_name="Сопост",
        required_wilds=set(order_lines_by_wild),
    )
    client_price_by_wild = build_price_map(
        client_price_rows,
        wild_header="Товар",
        price_header="Цена для клиента",
        source_name="WMS остатки",
        required_wilds=set(order_lines_by_wild),
    )

    client_price_total = 0.0
    excluded_wilds: list[str] = []
    excluded_order_lines: list[str] = []
    fallback_purchase_cost_wilds: list[str] = []
    estimated_client_price_wilds: list[str] = []
    included_wild_count = 0
    for wild, order_lines in order_lines_by_wild.items():
        purchase_cost = purchase_cost_by_wild.get(wild)
        client_price = client_price_by_wild.get(wild)

        if purchase_cost is not None and purchase_cost > 0:
            priced_order_lines = [
                (quantity, purchase_cost, source_row)
                for quantity, _, source_row in order_lines
            ]
            used_plan_cost = False
        else:
            used_plan_cost = True
            priced_order_lines = []
            for quantity, planned_cost, source_row in order_lines:
                if planned_cost is None or planned_cost <= 0:
                    excluded_order_lines.append(f"{wild} (строка {source_row})")
                    continue
                priced_order_lines.append((quantity, planned_cost, source_row))

        if not priced_order_lines:
            excluded_wilds.append(wild)
            continue
        if used_plan_cost:
            fallback_purchase_cost_wilds.append(wild)

        included_wild_count += 1
        for quantity, line_purchase_cost, _ in priced_order_lines:
            if client_price is None or client_price <= 0:
                line_client_price = line_purchase_cost * 2
                estimated_client_price_wilds.append(wild)
            else:
                line_client_price = client_price
            client_price_total += quantity * line_client_price

    if not included_wild_count:
        raise ValueError(
            f"Не удалось рассчитать цену клиента для сценария «{scenario_name}»: "
            "нет положительной цены в WMS и закупочной цены в «Сопост» "
            "или плане; запись финансового отчёта прервана."
        )
    if excluded_wilds:
        logger.warning(
            "Товары сценария «%s» без закупочной стоимости в «Сопост» и "
            "плановых заказах исключены из суммы по цене клиента; суммы заказа "
            "по цене поставщика учтены, расчёт продолжается | "
            "wild_count=%s | wild=%s",
            scenario_name,
            len(excluded_wilds),
            ", ".join(sorted(excluded_wilds)),
        )
    if excluded_order_lines:
        logger.warning(
            "Строки сценария «%s» без закупочной стоимости в «Сопост» и "
            "плановых заказах исключены из суммы по цене клиента; суммы заказа "
            "по цене поставщика учтены, расчёт продолжается | "
            "line_count=%s | wild_rows=%s",
            scenario_name,
            len(excluded_order_lines),
            ", ".join(sorted(excluded_order_lines)),
        )
    if fallback_purchase_cost_wilds:
        logger.info(
            "Для оценки цены клиента в сценарии «%s» использована плановая "
            "закупочная стоимость из строк без цены в «Сопост» | "
            "wild_count=%s | wild=%s",
            scenario_name,
            len(fallback_purchase_cost_wilds),
            ", ".join(sorted(fallback_purchase_cost_wilds)),
        )
    if estimated_client_price_wilds:
        estimated_client_price_wilds = sorted(set(estimated_client_price_wilds))
        logger.warning(
            "Для товаров сценария «%s» без положительной цены клиента применена "
            "цена по правилу закупочная стоимость × 2; расчёт продолжается | "
            "wild_count=%s | wild=%s",
            scenario_name,
            len(estimated_client_price_wilds),
            ", ".join(sorted(estimated_client_price_wilds)),
        )

    logger.info(
        "Рассчитана стоимость %s | order_rows=%s | client_priced_wilds=%s | "
        "supplier_price=%.2f | purchase_cost=%.2f | client_price=%.2f",
        scenario_name,
        matching_order_row_count,
        included_wild_count,
        supplier_price_total,
        purchase_cost_total,
        client_price_total,
    )
    return supplier_price_total, purchase_cost_total, client_price_total


def _get_supplier_debt_by_order_status(order_status: str) -> float:
    """Считает неоплаченную задолженность за товар по статусу заказа.

    Бизнес-правило: по заказам с заданным статусом суммируются только те части
    аванса, баланса 1 и баланса 2, статус оплаты которых не равен «оплачено».
    Сумма в RMB умножается на «Курс юаня», кроме поставщика ZILOL FAYZ INVEST,
    для которого исходная сумма переносится без валютного пересчёта.
    """
    order_spreadsheet_id = "1fXiijP8vMYv8vEFN1BnnTcqioCT_P2t1tgogi_ATh_8"
    worksheet = _open_financial_report_sheet(
        table_title="Расчет поставки Китай_по обороту",
        sheet_title="Заказы белые ТЕСТ",
        spreadsheet_id=order_spreadsheet_id,
        required_access="читателя",
    ).sheet_title
    payment_fields = (
        ("Оплата аванса", "Сумма аванса, RMB 🤖"),
        ("Оплата Баланса1", "Сумма баланса, RMB 🤖"),
        ("Оплата баланса", "Сумма баланса, RMB 2 🤖"),
    )
    required_headers = (
        "Статус",
        "Поставщик",
        "Курс юаня",
        *(header for pair in payment_fields for header in pair),
    )
    rows = _read_table_rows_by_headers(worksheet, required_headers)
    first_data_row = worksheet.frozen_row_count + 1
    matching_order_rows = 0
    unpaid_components = 0
    zilol_rows = 0
    converted_rows = 0
    debt_total = 0.0
    normalized_order_status = order_status.strip().casefold()

    for row_offset, row in enumerate(rows):
        if str(row.get("Статус") or "").strip().casefold() != normalized_order_status:
            continue

        matching_order_rows += 1
        unpaid_amount = 0.0
        for payment_status_header, amount_header in payment_fields:
            payment_status = str(row.get(payment_status_header) or "").strip().casefold()
            if payment_status == "оплачено":
                continue

            amount = _parse_finite_number(row.get(amount_header))
            if amount is None:
                source_row = first_data_row + row_offset
                raise ValueError(
                    f"В строке {source_row} листа «Заказы белые ТЕСТ» "
                    f"со статусом «{order_status}» "
                    f"не указана сумма для неоплаченного поля «{amount_header}»."
                )
            unpaid_amount += amount
            unpaid_components += 1

        if unpaid_amount == 0:
            continue

        supplier = str(row.get("Поставщик") or "").strip().casefold()
        if supplier.startswith("zilol"):
            # Для ZILOL исходная сумма уже учитывается без пересчёта по курсу юаня.
            debt_total += unpaid_amount
            zilol_rows += 1
            continue

        currency_rate = _parse_finite_number(row.get("Курс юаня"))
        if currency_rate is None or currency_rate <= 0:
            source_row = first_data_row + row_offset
            raise ValueError(
                f"В строке {source_row} листа «Заказы белые ТЕСТ» "
                f"со статусом «{order_status}» "
                "не указан положительный курс юаня для неоплаченной задолженности."
            )
        debt_total += unpaid_amount * currency_rate
        converted_rows += 1

    logger.info(
        "Рассчитана задолженность за товар по статусу «%s» | order_rows=%s | "
        "unpaid_components=%s | zilol_rows_without_conversion=%s | "
        "converted_rows=%s",
        order_status,
        matching_order_rows,
        unpaid_components,
        zilol_rows,
        converted_rows,
    )
    return debt_total


def _get_payment_stage_totals(required_stages: set[str]) -> dict[str, float]:
    """Суммирует неоплаченные суммы по нужным этапам платёжного календаря.

    Бизнес-правило: для каждой статьи управленческого отчёта берутся строки
    листа «Аналитика_платежей» с точным названием этапа и суммируется поле
    «Не_оплачено» без повторного пересчёта заказов. Заголовки этого листа
    находятся в первой строке. Временные ошибки чтения Google Sheets
    повторяются; пустая или некорректная сумма нужного этапа прерывает расчёт.
    """
    payment_calendar_spreadsheet_id = "1lL9LSIBE5IxXIrx-R5aBu-DUib6wHuK2TU9fUpS9xOs"
    connection = _open_financial_report_sheet(
        table_title="Платежный календарь",
        sheet_title="Аналитика_платежей",
        spreadsheet_id=payment_calendar_spreadsheet_id,
        required_access="читателя",
    )
    worksheet = connection.sheet_title
    rows = connection.execute_with_retry(
        "чтение неоплаченных этапов платёжного календаря",
        _read_table_rows_by_headers,
        worksheet,
        ("Этап платежа", "Не_оплачено"),
    )
    stage_totals = {stage: 0.0 for stage in required_stages}
    stage_counts = {stage: 0 for stage in required_stages}

    for row_offset, row in enumerate(rows):
        stage = str(row.get("Этап платежа") or "").strip()
        if stage not in required_stages:
            continue

        unpaid_amount = _parse_finite_number(row.get("Не_оплачено"))
        if unpaid_amount is None:
            source_row = (worksheet.frozen_row_count or 1) + 1 + row_offset
            raise ValueError(
                f"В строке {source_row} листа «Аналитика_платежей» "
                f"для этапа «{stage}» не указана сумма поля «Не_оплачено»."
            )
        stage_totals[stage] += unpaid_amount
        stage_counts[stage] += 1

    missing_stages = required_stages - {
        stage for stage, count in stage_counts.items() if count
    }
    if missing_stages:
        raise ValueError(
            "На листе «Аналитика_платежей» не найдены этапы платежа: "
            f"{', '.join(sorted(missing_stages))}."
        )
    logger.info(
        "Рассчитаны неоплаченные суммы по этапам платёжного календаря | "
        "stage_count=%s | source_rows=%s",
        len(stage_totals),
        sum(stage_counts.values()),
    )
    return stage_totals


def update_weekly_profit_report():
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_weekly_profit_report()
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("weekly_rep")
    # Создаем соединение с гугл-таблицей
    google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
    # Вставляем данные в гугл-таблицу
    google_connect.set_df_to_google(df)
    print("Данные вставлены в гугл таблицу")

def update_outcomes_detalize(table_name: str = "Анализ_фин_отчетов_Вектор", sheet_name: str = "отчет_по_неделям"):
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_outcomes_detalize()
    
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("outcomes_detalize")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        google_connect.set_df_to_google(df)
        print("Данные вставлены в гугл таблицу")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {table_name}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except StopIteration:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}") 

def update_fin_deductions_mv(table_name: str = "Анализ_фин_отчетов_Вектор", sheet_name: str = "отчет_по_неделям"):
    """Функция для вставки в таблицу Анализ_фин_отчетов_Вектор данных о еженедельных удержаниях"""
    # Создаем движок для подключения к БД
    engine = Database.get_engine()
    # Получаем датафрейм из БД
    df = FinReportsAnalyze(engine).get_fin_deductions_mv()

    
    # Определяем таблицу и лист для вставки данных
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("deducations_detalize")

    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
        # Вставляем данные в гугл-таблицу
        google_connect.set_df_to_google(df)
        print("Данные вставлены в гугл таблицу")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {table_name}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except StopIteration:
        print(f"Не найден лист {sheet_name} в таблице {table_name}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}") 


def update_daily_fin_reports_deductions_agg() -> None:
    """Выгружает помесячную детализацию удержаний WB в управленческий финансовый отчёт.

    Job получает агрегированные удержания по их группе и месяцу из PostgreSQL,
    заменяет отсутствующие значения нулями и полностью обновляет предназначенный
    для этой детализации лист Google Sheets. Нули сохраняют корректность итогов
    и сводных формул в отчёте при неполных данных источника.
    """
    fin_rep = FinReportsAnalyze()
    dataframe = fin_rep.get_daily_fin_reports_deductions_agg().fillna(0)
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("export_daily_fin_reports_deductions_agg")

    logger.info(
        "Начата выгрузка помесячной детализации удержаний в Google Sheets | rows=%s",
        len(dataframe.index),
    )
    google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
    google_connect.set_df_to_google(dataframe)
    logger.info("Выгрузка помесячной детализации удержаний в Google Sheets завершена")


def update_deductions_by_month():
    """ Обновление данных о ежемесячных удержаниях в таблице Анализ_фин_отчетов_Вектор, лист отчет_по_месяцам"""
    analyze = FinReportsAnalyze()
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("deductions_by_month")
    analyze.set_processed_df_to_google(query_deductions_by_month, table_name=table_name, sheet_name=sheet_name)

def update_cash_flow_writeoffs():
    """ Выгрузка детализированных данных по затратам из 1С Анализ_фин_отчетов_Вектор, лист расходы_по_банку"""
    fin_rep = FinReportsAnalyze()
    df = fin_rep.get_update_cash_flow_writeoffs()
    table_name = fin_rep_analyze.get("title")
    sheet_name: str = fin_rep_analyze.get("query_cash_flow_writeoffs")
    # Создаем соединение с гугл-таблицей
    google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
    # Вставляем данные в гугл-таблицу
    google_connect.set_df_to_google(df)

def update_monthly_report():
    fin_rep = FinReportsAnalyze()
    df = fin_rep.get_monthly_profit_report()
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("monthly_rep")
    # Создаем соединение с гугл-таблицей
    google_connect = GoogleTabs(table_title=table_name, sheet_title=sheet_name)
    # Вставляем данные в гугл-таблицу
    google_connect.set_df_to_google(df)


def update_fin_reports_art_analyze() -> int:
    """Обновляет PostgreSQL-витрину и выгружает её в таблицу финансового анализа.

    Сначала создаёт или пересчитывает `public.fin_reports_art_analyze` с учётом
    поздних корректировок отчётов WB, затем полностью обновляет вкладку
    «по_отчетно» в Google-таблице по её стабильному идентификатору.
    """
    analyze = FinReportsAnalyze()
    refreshed_rows = analyze.refresh_fin_reports_art_analyze()
    dataframe = analyze.get_fin_reports_art_analyze()
    google_connect = GoogleTabs(
        table_title=fin_rep_analyze.get("title"),
        sheet_title=fin_rep_analyze.get("art_report_sheet"),
        spreadsheet_id=fin_rep_analyze.get("art_report_spreadsheet_id"),
    )
    google_connect.set_df_to_google(dataframe)
    logger.info(
        "Витрина фин. отчётов выгружена в Google Sheets | sheet=%s | rows=%s",
        fin_rep_analyze.get("art_report_sheet"),
        len(dataframe),
    )
    return refreshed_rows

def update_stock_analyze():
    analyze = FinReportsAnalyze()
    table_name = fin_rep_analyze.get("title")
    sheet_name = fin_rep_analyze.get("stock_analyze")
    analyze.set_processed_df_to_google(query_stock_analyze, table_name=table_name, sheet_name=sheet_name)


def update_wb_fin_model_report() -> None:
    """Обновляет остатки ВВТ, три категории WB и финансовую витрину.

    Бизнес-сценарий: читает количество, себестоимость и цену продаж остатков
    склада ВВТ из итогов WMS с повтором при временной ошибке Google Sheets.
    Затем рассчитывает цену поставщика товара в пути до склада ВВТ
    по сумме заказа с пересчётом в рубли, кроме ZILOL FAYZ INVEST. Его
    себестоимость считает по тем же ценам `cost_price` и дате снимка, что и
    остатки WB; цену клиента берёт из WMS, а при её
    отсутствии оценивает по себестоимости из «Сопост» или плану, затем
    распределяет последний доступный снимок WB по остаткам всех складов,
    пути к клиенту и пути от клиента, включая «Склад WB РФ».
    Для каждой категории количество и себестоимость берутся из PostgreSQL;
    клиентская цена — из PostgreSQL или, если она
    отсутствует или не положительна, из WMS по wild. Без обеих клиентских
    цен товар учитывается с нулевой клиентской стоимостью и предупреждением.
    Чтение клиентских цен WMS повторяется при временной ошибке сети.
    Также считает неоплаченную задолженность по прибывшим заказам, заказам в
    пути и этапам платёжного календаря. Брокерская статья объединяет два этапа,
    а общая таможенная задолженность — три этапа КРЕДО. Затем обновляет
    целевые ячейки витрины: цену поставщика, себестоимость и цену клиента
    товара в пути пишет в соответствующие колонки, а задолженности — в поля
    «по себестоимости». Итог активов учитывает себестоимость товара в пути.
    Итог задолженности исключает справочную общую строку таможни, чтобы её
    этапы не учитывались дважды.
    """
    dataframe = Database.read_sql_to_dataframe(query_wb_stock_financial_report)
    if dataframe.empty:
        raise ValueError("Запрос остатков WB не вернул итоговые суммы.")

    required_wb_columns = {
        "local_vendor_code",
        "quantity",
        "in_way_to_client",
        "in_way_from_client",
        "average_purchase_price",
        "discounted_price",
    }
    missing_wb_columns = required_wb_columns - set(dataframe.columns)
    if missing_wb_columns:
        raise ValueError(
            "Запрос остатков WB не вернул обязательные поля для расчёта: "
            f"{', '.join(sorted(missing_wb_columns))}."
        )

    unit_spreadsheet_id = "1bvZLSNb-CdWzuyBScL6C6x2fw--U1F8BD9PY4krElc4"
    unit_connection = _open_financial_report_sheet(
        table_title="UNIT 2.0 (tested)",
        sheet_title="WMS остатки",
        spreadsheet_id=unit_spreadsheet_id,
        required_access="читателя",
    )
    (
        warehouse_quantity,
        warehouse_purchase_cost,
        warehouse_client_price,
    ) = unit_connection.execute_with_retry(
        "чтение итогов остатков склада ВВТ из WMS",
        _get_wms_stock_totals,
        unit_connection.sheet_title,
    )
    wms_client_prices = unit_connection.execute_with_retry(
        "чтение клиентских цен WMS для трёх категорий WB",
        _get_wms_client_prices_by_wild,
        unit_connection.sheet_title,
        required_wilds={
            str(wild).strip()
            for wild in dataframe["local_vendor_code"].dropna()
            if str(wild).strip()
        },
    )
    # Каждая колонка количества WB соответствует отдельной строке витрины.
    wb_quantity_columns = {
        "Остатки на ВБ (все склады)": "quantity",
        "Остатки на ВБ (в пути до клиента)": "in_way_to_client",
        "Остатки на ВБ (в пути от клиента)": "in_way_from_client",
    }
    wb_quantity_totals = {label: 0.0 for label in wb_quantity_columns}
    wb_cost_totals = {label: 0.0 for label in wb_quantity_columns}
    wb_client_totals = {label: 0.0 for label in wb_quantity_columns}
    fallback_price_wilds: set[str] = set()
    unavailable_client_price_wilds: set[str] = set()
    for values in (
        dataframe[
            [
                "local_vendor_code",
                *wb_quantity_columns.values(),
                "average_purchase_price",
                "discounted_price",
            ]
        ].itertuples(index=False, name=None)
    ):
        wild = str(values[0] or "").strip()
        purchase_price = _parse_finite_number(values[-2]) or 0.0
        wb_price = _parse_finite_number(values[-1])
        wms_price = wms_client_prices.get(wild)
        client_price = wb_price if wb_price is not None and wb_price > 0 else wms_price
        for (label, _), quantity_value in zip(
            wb_quantity_columns.items(), values[1:-2], strict=True
        ):
            quantity = _parse_finite_number(quantity_value) or 0.0
            wb_quantity_totals[label] += quantity
            wb_cost_totals[label] += quantity * purchase_price
            if client_price is not None and client_price > 0:
                wb_client_totals[label] += quantity * client_price
                if wb_price is None or wb_price <= 0:
                    fallback_price_wilds.add(wild)
            elif quantity:
                unavailable_client_price_wilds.add(wild or "(пустой wild)")

    if fallback_price_wilds:
        logger.info(
            "Для WB применена цена за штуку из WMS там, где цена WB "
            "отсутствует или не положительна | wild_count=%s",
            len(fallback_price_wilds),
        )
    if unavailable_client_price_wilds:
        logger.warning(
            "Для части остатков WB нет положительной цены ни в prices_data, "
            "ни в WMS; эти wild учтены с нулевой ценой, расчёт продолжается | "
            "wild_count=%s | wild=%s",
            len(unavailable_client_price_wilds),
            ", ".join(sorted(unavailable_client_price_wilds)),
        )
    logger.info(
        "Рассчитаны три категории остатков WB по всем складам | "
        "stock_qty=%.0f | to_client_qty=%.0f | from_client_qty=%.0f",
        *wb_quantity_totals.values(),
    )
    (
        in_transit_supplier_price,
        in_transit_purchase_cost,
        in_transit_client_price,
    ) = _get_order_goods_totals(
        unit_connection.table,
        statuses={"в пути"},
        scenario_name="товара в пути до склада ВВТ",
        snapshot_date=dataframe["snapshot_date"].iloc[0],
    )
    arrived_supplier_debt = _get_supplier_debt_by_order_status("прибыло")
    in_transit_supplier_debt = _get_supplier_debt_by_order_status("в пути")
    # Название строки отчёта сопоставляется с одним или двумя этапами источника.
    payment_report_stages = {
        "Таможенные платежи КРЕДО": ("Таможенные платежи КРЕДО",),
        "Международная перевозка": ("Международная перевозка",),
        "Российская доставка": ("Российская доставка",),
        "Брокерское оформление (Брокерское оформление+Брокерское оформление КРЕДО)": (
            "Брокерское оформление",
            "Брокерское оформление КРЕДО",
        ),
        "Страхование груза": ("Страхование груза",),
        "Простой": ("Простой",),
        "Хранение": ("Хранение",),
        "Агентское вознаграждение КРЕДО": (
            "Агентское вознаграждение КРЕДО",
        ),
    }
    payment_stage_totals = _get_payment_stage_totals({
        stage
        for stages in payment_report_stages.values()
        for stage in stages
    })
    customs_debt = sum(
        payment_stage_totals[stage]
        for stage in (
            "Брокерское оформление КРЕДО",
            "Агентское вознаграждение КРЕДО",
            "Таможенные платежи КРЕДО",
        )
    )
    spreadsheet_id = "1Cqr7pb0RKglLgQAc00o8bj5jiL14G1qOD_dSAWrpD2Y"
    worksheet_name = "Фин отчет управл"
    logger.info(
        "Получены категории остатков WB для управленческого отчёта | "
        "snapshot=%s",
        dataframe["snapshot_date"].iloc[0],
    )

    google_connect = _open_financial_report_sheet(
        table_title="Модель фин отчетов",
        sheet_title=worksheet_name,
        spreadsheet_id=spreadsheet_id,
        required_access="редактора",
    )
    worksheet = google_connect.sheet_title
    warehouse_label = "Остатки на складе ВВТ Вектор"
    in_transit_label = "Товар в пути до склада ВВТ Вектор по цене поставщика"
    arrived_debt_label = "Задолженность перед поставщиками за прибывший товар*"
    in_transit_debt_label = "Задолженность перед поставщиками за товар в пути*"
    customs_debt_label = "Задолженность перед таможней"
    overdraft_label = "Овердрафт"
    total_debt_label = "Всего задолженность"
    total_assets_label = "Всего активы"
    warehouse_label_cell = worksheet.find(warehouse_label)
    in_transit_label_cell = worksheet.find(in_transit_label)
    arrived_debt_label_cell = worksheet.find(arrived_debt_label)
    in_transit_debt_label_cell = worksheet.find(in_transit_debt_label)
    customs_debt_label_cell = worksheet.find(customs_debt_label)
    overdraft_label_cell = worksheet.find(overdraft_label)
    total_debt_label_cell = worksheet.find(total_debt_label)
    wb_label_cells = {
        label: worksheet.find(label) for label in wb_quantity_columns
    }
    total_assets_label_cell = worksheet.find(total_assets_label)
    supplier_price_cell = worksheet.find("по цене поставщика")
    quantity_cell = worksheet.find("количество")
    cost_cell = worksheet.find("по себестоимости")
    sales_cell = worksheet.find("по цене продаж")
    if (
        warehouse_label_cell is None
        or in_transit_label_cell is None
        or arrived_debt_label_cell is None
        or in_transit_debt_label_cell is None
        or customs_debt_label_cell is None
        or overdraft_label_cell is None
        or total_debt_label_cell is None
        or any(cell is None for cell in wb_label_cells.values())
        or total_assets_label_cell is None
        or supplier_price_cell is None
        or quantity_cell is None
        or cost_cell is None
        or sales_cell is None
    ):
        raise ValueError(
            "На листе управленческого отчёта не найдена строка остатков ВВТ "
            "или одна из трёх строк остатков WB, строка товара в пути, "
            "задолженности, итога активов "
            "или один из целевых заголовков."
        )
    supplier_in_transit_address = rowcol_to_a1(
        in_transit_label_cell.row, supplier_price_cell.col
    )
    cost_in_transit_address = rowcol_to_a1(
        in_transit_label_cell.row, cost_cell.col
    )
    assets_total_address = rowcol_to_a1(
        total_assets_label_cell.row, cost_cell.col
    )
    assets_total_cell = google_connect.execute_with_retry(
        "чтение формулы итога активов управленческого отчёта",
        worksheet.acell,
        assets_total_address,
        value_render_option="FORMULA",
    )
    assets_total_formula = assets_total_cell.value or ""
    asset_terms = assets_total_formula.removeprefix("=").split("+")
    if not assets_total_formula.startswith("="):
        raise ValueError("В строке «Всего активы» не найдена формула себестоимости.")
    if supplier_in_transit_address in asset_terms:
        asset_terms[asset_terms.index(supplier_in_transit_address)] = (
            cost_in_transit_address
        )
    elif cost_in_transit_address not in asset_terms:
        raise ValueError(
            "Формула «Всего активы» не содержит ссылку на товар в пути "
            "в колонке по себестоимости или по цене поставщика."
        )
    for cell in wb_label_cells.values():
        wb_cost_address = rowcol_to_a1(cell.row, cost_cell.col)
        if wb_cost_address not in asset_terms:
            asset_terms.append(wb_cost_address)
    warehouse_cost_address = rowcol_to_a1(
        warehouse_label_cell.row, cost_cell.col
    )
    if warehouse_cost_address not in asset_terms:
        asset_terms.append(warehouse_cost_address)
    assets_total_formula = "=" + "+".join(asset_terms)
    sales_total_address = rowcol_to_a1(
        total_assets_label_cell.row, sales_cell.col
    )
    sales_total_cell = google_connect.execute_with_retry(
        "чтение формулы итога активов по цене продаж",
        worksheet.acell,
        sales_total_address,
        value_render_option="FORMULA",
    )
    sales_total_formula = sales_total_cell.value or ""
    if not sales_total_formula.startswith("="):
        raise ValueError("В строке «Всего активы» не найдена формула цены продаж.")
    sales_terms = sales_total_formula.removeprefix("=").split("+")
    for cell in wb_label_cells.values():
        wb_sales_address = rowcol_to_a1(cell.row, sales_cell.col)
        if wb_sales_address not in sales_terms:
            sales_terms.append(wb_sales_address)
    warehouse_sales_address = rowcol_to_a1(
        warehouse_label_cell.row, sales_cell.col
    )
    if warehouse_sales_address not in sales_terms:
        sales_terms.append(warehouse_sales_address)
    sales_total_formula = "=" + "+".join(sales_terms)
    report_labels = google_connect.execute_with_retry(
        "чтение названий статей управленческого отчёта",
        worksheet.col_values,
        in_transit_label_cell.col,
    )
    payment_label_rows = {
        str(value).strip(): row_number
        for row_number, value in enumerate(report_labels, start=1)
        if str(value).strip() in payment_report_stages
    }
    missing_payment_labels = set(payment_report_stages) - payment_label_rows.keys()
    if missing_payment_labels:
        raise ValueError(
            "На листе управленческого отчёта не найдены строки этапов платежа: "
            f"{', '.join(sorted(missing_payment_labels))}."
        )

    target_ranges = [
        {
            "range": (
                f"{rowcol_to_a1(cell.row, cost_cell.col)}:"
                f"{rowcol_to_a1(cell.row, sales_cell.col)}"
            ),
            "values": [[wb_cost_totals[label], wb_client_totals[label]]],
        }
        for label, cell in wb_label_cells.items()
    ]
    target_ranges.extend(
        {
            "range": rowcol_to_a1(cell.row, quantity_cell.col),
            "values": [[wb_quantity_totals[label]]],
        }
        for label, cell in wb_label_cells.items()
    )
    target_ranges.extend([
        {
            "range": rowcol_to_a1(warehouse_label_cell.row, quantity_cell.col),
            "values": [[warehouse_quantity]],
        },
        {
            "range": f"{warehouse_cost_address}:{warehouse_sales_address}",
            "values": [[warehouse_purchase_cost, warehouse_client_price]],
        },
    ])
    target_ranges.extend([
        {
            "range": supplier_in_transit_address,
            "values": [[in_transit_supplier_price]],
        },
        {
            "range": cost_in_transit_address,
            "values": [[in_transit_purchase_cost]],
        },
        {
            "range": rowcol_to_a1(in_transit_label_cell.row, sales_cell.col),
            "values": [[in_transit_client_price]],
        },
        {
            "range": assets_total_address,
            "values": [[assets_total_formula]],
        },
        {
            "range": sales_total_address,
            "values": [[sales_total_formula]],
        },
    ])
    target_ranges.append(
        {
            "range": rowcol_to_a1(arrived_debt_label_cell.row, cost_cell.col),
            "values": [[arrived_supplier_debt]],
        }
    )
    target_ranges.append(
        {
            "range": rowcol_to_a1(in_transit_debt_label_cell.row, cost_cell.col),
            "values": [[in_transit_supplier_debt]],
        }
    )
    target_ranges.append(
        {
            "range": rowcol_to_a1(customs_debt_label_cell.row, cost_cell.col),
            "values": [[customs_debt]],
        }
    )
    for label, stages in payment_report_stages.items():
        target_ranges.append({
            "range": rowcol_to_a1(payment_label_rows[label], cost_cell.col),
            "values": [[sum(payment_stage_totals[stage] for stage in stages)]],
        })
    first_debt_cell = rowcol_to_a1(arrived_debt_label_cell.row, cost_cell.col)
    last_debt_cell = rowcol_to_a1(overdraft_label_cell.row, cost_cell.col)
    customs_debt_cell = rowcol_to_a1(customs_debt_label_cell.row, cost_cell.col)
    target_ranges.append({
        "range": rowcol_to_a1(total_debt_label_cell.row, cost_cell.col),
        "values": [[
            f"=SUM({first_debt_cell}:{last_debt_cell})-{customs_debt_cell}"
        ]],
    })
    execution_timestamp = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S")
    target_ranges.append(
        {
            "range": "A1",
            "values": [[
                "*данные на текущую дату\n"
                f"Дата и время исполнения скрипта: {execution_timestamp} МСК"
            ]],
        }
    )
    google_connect._execute_google_write_with_retry(
        operation_name=(
            "обновление остатков ВВТ, трёх категорий WB и задолженности"
        ),
        func=worksheet.batch_update,
        data=target_ranges,
        value_input_option="USER_ENTERED",
    )
    logger.info(
        "Финансовые показатели записаны в управленческий отчёт | "
        "warehouse_row=%s | wb_rows=%s | in_transit_row=%s | arrived_debt_row=%s | "
        "in_transit_debt_row=%s | customs_debt_row=%s",
        warehouse_label_cell.row,
        ",".join(str(cell.row) for cell in wb_label_cells.values()),
        in_transit_label_cell.row,
        arrived_debt_label_cell.row,
        in_transit_debt_label_cell.row,
        customs_debt_label_cell.row,
    )

# python -m src_oop.jobs.fin_reports_analyze.run
# if __name__ == "__main__":
#     update_stock_analyze()
