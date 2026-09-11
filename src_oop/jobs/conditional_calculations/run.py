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
    оплате и кредитные перечисления.
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
