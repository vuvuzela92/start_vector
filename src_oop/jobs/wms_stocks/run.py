from src_oop.jobs.wms_stocks.api_client import WMSStockService
from src_oop.jobs.wms_stocks.process import Process
from src_oop.core.database import Database
from src_oop.jobs.wms_stocks.tables_scheme import historical_stocks_fbs_service_table

from datetime import datetime, timedelta

async def wms_stocks_run(date_from: str = None, date_to: str = None):
    if date_from is None:
        date_from = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")
    if date_to is None:
        date_to = datetime.now().strftime("%Y-%m-%d")    
    data = await WMSStockService().fetch_external_stocks(date_from, date_to)
    df = Process(data).process_historical_stocks()
    table_name = historical_stocks_fbs_service_table.get("title")
    scheme_definition = historical_stocks_fbs_service_table.get("columns")
    unique_keys = historical_stocks_fbs_service_table.get("key_columns")
    Database.sync_data_to_postgres(table_name, df, scheme_definition, unique_keys)
