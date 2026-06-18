import asyncio

from src.core.utils_sql import get_db_engine, sync_data_to_postgres
from src.modules.WB.docs.schemas import (
    redeem_notification_dict,
    weekly_implementation_report_dict,
)


def _load_docs_processing_functions():
    """
    Лениво загружает функции обработки документов.

    Это нужно, чтобы задачи, не связанные с PDF-документами, не зависели от пакета pdfplumber
    уже на этапе импорта общего реестра задач. Если запускается именно PDF-задача и пакет
    не установлен, выбрасываем понятную ошибку с подсказкой по установке.
    """
    try:
        from src.modules.WB.docs.processing import (
            process_xlsx_redeem_file,
            processed_pdf_week_reps,
            processed_zip_docs,
        )
    except ModuleNotFoundError as error:
        if error.name == "pdfplumber":
            raise ModuleNotFoundError(
                "Для обработки PDF-документов требуется пакет pdfplumber. "
                "Установите его командой: pip install pdfplumber"
            ) from error
        raise

    return processed_zip_docs, processed_pdf_week_reps, process_xlsx_redeem_file


# python -m src.modules.WB.docs.tasks
async def get_bukh_docs():
    processed_zip_docs, processed_pdf_week_reps, process_xlsx_redeem_file = (
        _load_docs_processing_functions()
    )

    # Получаем обработанные zip файлы
    processed_zip_files = await processed_zip_docs()
    # Достаём из них файлы в pdf и xlsx форматах
    pdf_files = [
        file_data
        for file_data in processed_zip_files
        if file_data["path"].endswith(".pdf")
    ]  # Для еженедельного отчета реализации
    xlsx_files = [
        file_data
        for file_data in processed_zip_files
        if file_data["path"].endswith(".xlsx")
    ]  # Для уведомления о выкупе

    # Запрашиваем и обрабатываем еженедельные отчеты
    week_reps_data = processed_pdf_week_reps(pdf_files)

    engine = get_db_engine()

    table_weekly_rep = list(weekly_implementation_report_dict.keys())[0]
    unique_keys = ["doc_num", "в„–", "account"]
    schema_definition = weekly_implementation_report_dict[table_weekly_rep]

    sync_data_to_postgres(
        engine,
        table_weekly_rep,
        week_reps_data.to_dict("records"),
        schema_definition,
        unique_keys,
    )

    # Достаём все уведомления о выкупе
    redeem_file_data = process_xlsx_redeem_file(xlsx_files)

    table_notif = list(redeem_notification_dict.keys())[0]
    unique_keys = ["doc_name", "в„–", "account"]
    schema_definition = redeem_notification_dict[table_notif]

    sync_data_to_postgres(
        engine,
        table_notif,
        redeem_file_data.to_dict("records"),
        schema_definition,
        unique_keys,
    )
