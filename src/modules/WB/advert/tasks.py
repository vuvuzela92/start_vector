# Импорт внешних библиотек
import asyncio
from datetime import datetime, timedelta
import pandas as pd
# Импорт внутренних модулей и функций
from src.core.utils_general import load_api_tokens
from src.core.utils_sql import get_db_engine, sync_data_to_postgres
from src.modules.WB.advert.api import fetch_advert_info, fetch_advert_spend_info
from src.modules.WB.advert.processing import extract_campaign_info, process_advert_spend_info
from src.modules.WB.advert.schemas import advert_campaigns_info_dict, advert_spend_info_dict


# === Получение данных о рекламных кампаниях
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

# === Получение данных о рекламных затратах

async def advert_spend_info(days_count=28):
    """ Обрабатывает данные собранные за несколько дней. По умолчанию за 28 дней."""
    # Список для хранения данных
    all_data = []
    # Проходим циклом по каждому дню
    for day in range(1, days_count+1):
        date_from = date_to = (datetime.now()-timedelta(days=day)).strftime("%Y-%m-%d")
        data = await fetch_advert_spend_info(load_api_tokens(), date_from, date_to)
        try:
            df_data = process_advert_spend_info(data, date_from)   
            all_data.append(df_data)
        except Exception:
            all_data.append(pd.DataFrame())

    df_all_data = pd.concat(all_data)
    return df_all_data

def advert_spend():
    """Получение данных по затратам"""
    df = asyncio.run(advert_spend_info())
    # Присваиваем имена колонок в snake_case
    df = df.rename(columns={
        'updTime': 'upd_time',
        'campName': 'camp_name',
        'paymentType': 'payment_type',
        'updNum': 'upd_num',
        'updSum': 'upd_sum',
        'advertId': 'advert_id',
        'advertType': 'advert_type',
        'advertStatus': 'advert_status'
                            })
    # Преобразуем датафрейм в словарь для вставкив БД
    data = df.to_dict(orient='records')
    # Установка соединения с базой данных
    engine = get_db_engine()

    table_name = list(advert_spend_info_dict.keys())[0]
    unique_keys = ['advert_id', 'upd_num', 'upd_time']  # Уникальность по ID кампании
    # В ключе указываем имя таблицы в БД
    schema_definition = advert_spend_info_dict.get(table_name)

    sync_data_to_postgres(engine, table_name, data, schema_definition, unique_keys)