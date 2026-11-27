from src_oop.jobs.conditional_calculations.processor import ProcessConditionalCalculation
from src_oop.jobs.conditional_calculations.repository import ConditionalCalculationsRepository
from src_oop.jobs.conditional_calculations.tables_scheme import conditional_calculations
import logging
from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from gspread_dataframe import set_with_dataframe
import gspread
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)


def conditional_calculation_to_db_run():
    """Получает данные по Условному расчету и добавляет их в БД.

    Бизнес-сценарий: пересчитывает дневные показатели по аккаунтам, дополняет
    их финансовыми полями WB, доводит схему таблицы до актуальной версии и
    записывает результат в `conditions_calculation` по ключу `date + account`.
    """
    repo = ConditionalCalculationsRepository()
    df = ProcessConditionalCalculation(repo).process_df()

    if df.empty:
        logger.warning("Нет данных для записи")
        return
    
    scheme = conditional_calculations.get("columns")
    table = conditional_calculations.get("title")
    keys = conditional_calculations.get("unique_keys")

    repo.ensure_conditions_calculation_columns()
    Database.sync_data_to_postgres(
        table_name=table,
        data=df,
        schema_definition=scheme,
        unique_keys=keys
    )


def update_conditional_calculations_to_gs(
    table_name: str = "Условный расчет",
    sheet_name: str = "Справочная информация",
):
    """Выгружает сохраненный Условный расчет из БД в Google Sheets.

    Бизнес-сценарий: публикует в справочный лист те же данные, которые были
    записаны в `conditions_calculation`, включая штрафы по `date_from`, итог к
    оплате, кредитные перечисления и фактический эквайринг из финансового
    отчёта. Колонка эквайринга добавляется перед `updated_at`.
    """
    df = ConditionalCalculationsRepository().get_conditional_calculations()

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


def update_penalties_to_gs(
    table_name: str = "Условный расчет",
    sheet_name: str = "Штрафы",
) -> None:
    """Публикует детализацию штрафов WB на вкладку условного расчёта.

    Бизнес-сценарий: загружает штрафные операции за последние 14 недель,
    добавляет сведения о товаре и статусах сборочного задания, переводит
    ключевые статусы на русский язык и полностью обновляет вкладку «Штрафы».
    Если за период штрафов нет, текущая вкладка сохраняется без очистки.
    """
    repo = ConditionalCalculationsRepository()
    df = repo.get_penalties_for_google_sheets()
    if df is None or df.empty:
        logger.warning(
            "Выгрузка штрафов пропущена: за последние 14 недель строк со штрафами нет."
        )
        return

    status_translations = {
        "supplier_status": {
            "new": "Новое сборочное задание",
            "confirm": "На сборке",
            "complete": "В доставке",
            "cancel": "Отменено продавцом",
        },
        "wb_status": {
            "waiting": "сборочное задание в работе",
            "sorted": "сборочное задание отсортировано",
            "sold": "заказ получен покупателем",
            "canceled": "отмена сборочного задания",
            "canceled_by_client": "покупатель отменил заказ при получении",
            "declined_by_client": (
                "покупатель отменил заказ. Отмена доступна покупателю в первый час "
                "с момента заказа, если заказ не переведён на сборку"
            ),
            "defect": "отмена заказа по причине брака",
            "ready_for_pickup": (
                "сборочное задание прибыло на пункт выдачи заказов (ПВЗ)"
            ),
        },
    }
    for column_name, translations in status_translations.items():
        if column_name in df.columns:
            df[column_name] = df[column_name].replace(translations)

    df = df.rename(
        columns={
            "date_from": "Дата отчета",
            "sale_dt": "Дата продажи",
            "penalty": "Штраф",
            "count_items": "Количество",
            "bonus_type_name": "Вид штрафа",
            "nm_id": "SKU",
            "subject_name": "Предмет",
            "account": "ЛК",
            "local_vendor_code": "wild",
            "shk_id": "Стикер",
            "assembly_id": "Сборочное задание",
            "supplier_status": "Статус СЗ у продавца",
            "wb_status": "Cтатус СЗ в системе WB",
            "supply_id": "Номер поставки",
        }
    )
    for column_name in ("Дата продажи", "Дата отчета", "order_date"):
        if column_name in df.columns:
            df[column_name] = df[column_name].astype(str)
    df = df.sort_values(
        ["Дата отчета", "Штраф"],
        ascending=[False, False],
    )

    try:
        GoogleTabs(table_title=table_name, sheet_title=sheet_name).set_df_to_google(df)
        logger.info(
            "Детализация штрафов опубликована в Google Sheets | строк=%s | вкладка=%s",
            len(df.index),
            sheet_name,
        )
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error("Таблица Google Sheets «%s» не найдена.", table_name)
        raise
    except gspread.exceptions.WorksheetNotFound:
        logger.error(
            "Вкладка Google Sheets «%s» в таблице «%s» не найдена.",
            sheet_name,
            table_name,
        )
        raise
