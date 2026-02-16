# Импорт внешних библиотек
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import text
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

def advert_spend():
    """Получение данных по затратам"""
    spend_info = asyncio.run(fetch_advert_spend_info(load_api_tokens()))
    df = process_advert_spend_info(spend_info)
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

    # Поскольку ВБ обновляетя данные по затратам с некоторой задержкой, удаляем данные за последние 28 дней перед добавлением новых
    # Удаление данных из таблицы перед добавлением новых
    with engine.connect() as connection:
            sql_stmt = text(f"DELETE FROM {table_name} WHERE date >= CURRENT_DATE - INTERVAL '28 days'")
            
            print(f"🧹 Очистка данных в {table_name} за последние 28 дней...")
            connection.execute(sql_stmt)
            connection.commit() # Обязательно фиксируем изменения
            
    unique_keys = ['advert_id', 'upd_num', 'upd_time']  # Уникальность по ID кампании
    # В ключе указываем имя таблицы в БД
    schema_definition = advert_spend_info_dict.get(table_name)        
    # Синхронизация данных в базу данных    
    sync_data_to_postgres(engine, table_name, data, schema_definition, unique_keys)