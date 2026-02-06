import asyncio
from src.core.utils_general import load_api_tokens
from src.core.utils_sql import get_db_engine, sync_data_to_postgres
from src.modules.WB.advert.api import fetch_advert_info
from src.modules.WB.advert.processing import extract_campaign_info
from src.modules.WB.advert.schemas import advert_campaigns_info_dict

# python -m src.modules.WB.advert.tasks
def advert_info():
    # === Получение данных по рекламным кампаниям ===
    tokens = load_api_tokens()
    advert_info_data = asyncio.run(fetch_advert_info(tokens))

    # Преобразуем данные из API в плоскую структуру
    data = extract_campaign_info(advert_info_data)

    # Установка соединения с базой данных
    engine = get_db_engine()

    table_name = list(advert_campaigns_info_dict.keys())[0]
    unique_keys = ['campaign_id']  # Уникальность по ID кампании
    schema_definition = advert_campaigns_info_dict.get('advert_campaigns_info')

    sync_data_to_postgres(engine, table_name, data, schema_definition, unique_keys)