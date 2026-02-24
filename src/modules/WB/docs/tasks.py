import asyncio
from src.modules.WB.docs.processing import processed_zip_docs, processed_pdf_week_reps, process_xlsx_redeem_file
from src.core.utils_sql import get_db_engine, sync_data_to_postgres
from src.modules.WB.docs.schemas import weekly_implementation_report_dict, redeem_notification_dict

# python -m src.modules.WB.docs.tasks
async def get_bukh_docs():
    # Получаем обработанные zip файлы
    processed_zip_files = await(processed_zip_docs())
    # Достаем из них файлы в pdf и xlsx форматах
    pdf_files = [f for f in processed_zip_files if f['path'].endswith('.pdf')] # Для еженедельного отчета реализации
    xlsx_files = [f for f in processed_zip_files if f['path'].endswith('.xlsx')] # Для уведомления о выкупе

    # Запрашиваем и обрабатываем еженедельные отчеты
    week_reps_data = processed_pdf_week_reps(pdf_files)

    engine = get_db_engine()


    table_weekly_rep = list(weekly_implementation_report_dict.keys())[0]
    unique_keys = ['doc_num', '№', 'account']
    schema_definition = weekly_implementation_report_dict[table_weekly_rep]

    sync_data_to_postgres(engine, table_weekly_rep, week_reps_data.to_dict('records'), schema_definition, unique_keys)

    # Достаем все Уведомления о выкупе
    redeem_file_data = process_xlsx_redeem_file(xlsx_files)

    table_notif = list(redeem_notification_dict.keys())[0]
    unique_keys = ['doc_name', '№', 'account']
    schema_definition = redeem_notification_dict[table_notif]

    sync_data_to_postgres(engine, table_notif, redeem_file_data.to_dict('records'), schema_definition, unique_keys)

