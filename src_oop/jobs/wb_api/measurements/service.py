# Импорт внешних библиотек
import aiohttp
import asyncio
import logging
# Импорт внутренних модулей
from src_oop.jobs.wb_api.measurements.config import table
from src_oop.core.utils_general import load_api_tokens
from src_oop.core.database import Database
from src_oop.jobs.wb_api.measurements.measurements import Measurements
from src_oop.jobs.wb_api.measurements.process import process_measurements_data
from src_oop.core.scraper import HTTPClient
from src_oop.jobs.wb_api.measurements.config import table

logger = logging.getLogger(__name__)

async def fetch_measurements_task():
    """Async функция для получения данных о замерах и сохранения их в БД"""
    tokens = load_api_tokens()
    db = Database()
    # Буфер для накопления данных перед записью в БД
    buffer = []
    # Лимит для пакетной записи в БД
    BUFFER_LIMIT = 10000
    # Параметры таблицы из конфига
    table_name = table['title']
    schema_definition = table['schema_definition']
    unique_keys = table['unique_keys']
    # Асинхронная сессия для HTTP-запросов
    async with aiohttp.ClientSession() as session:
        measurements = Measurements()
        # Перебираем все аккаунты и собираем данные
        for account, token in tokens.items():
            logger.info(f"🚀 Начинаем сбор: {account}")
            client = HTTPClient(session=session, api_key=token, account=account, timeout=60.0)

            # Перебираем страницы API по 1000 записей
            async for batch in measurements.get_measurements(account, client):
                # 1. Обрабатываем текущую порцию
                processed_batch = process_measurements_data(batch)
                
                # 2. Добавляем в общий буфер
                buffer.extend(processed_batch)
                logger.debug(f"Буфер наполнен: {len(buffer)}/{BUFFER_LIMIT}")

                # 3. Если накопили 10к+, сохраняем
                if len(buffer) >= BUFFER_LIMIT:
                    logger.info(f"💾 Буфер полон ({len(buffer)}). Сохраняю в БД...")
                    # 1. Зачистка дубликатов: оставляем последнюю встреченную запись для каждой пары
                    unique_data = {(d['nm_id']): d for d in buffer }.values()
                    clean_buffer = list(unique_data)
                    
                    logger.info(f"После очистки осталось {len(clean_buffer)} записей")
                    
                    # 2. Сохраняем уже чистые данные
                    db.sync_data_to_postgres(table_name, clean_buffer, schema_definition, unique_keys)
                    buffer.clear() # Очищаем после записи

            logger.info(f"✅ Сбор для {account} завершен.")

        # 4. сохраняем остатки после завершения всех циклов
        if buffer:
            logger.info(f"💾 Сохраняю финальный остаток: {len(buffer)} записей")
            # 1. Зачистка дубликатов: оставляем последнюю встреченную запись для каждой пары
            unique_data = {(d['nm_id']): d for d in buffer }.values()
            clean_buffer = list(unique_data)
            
            logger.info(f"После очистки осталось {len(clean_buffer)} записей")
            # 2. Сохраняем уже чистые данные
            db.sync_data_to_postgres(table_name, clean_buffer, schema_definition, unique_keys)
            buffer.clear()

def get_measurements():
    """Главная функция для запуска сбора данных о замерах"""
    db = Database()
    # Получаем дату последнего замера из БД, чтобы не запрашивать данные, которые уже есть
    table_name = table['title']
    query = f"""SELECT * FROM {table_name} ORDER BY date DESC"""
    df = db.read_sql_to_dataframe(query)
    return df