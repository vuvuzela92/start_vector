# Импорт внешних библиотек
import logging
import pandas as pd
import aiohttp
import asyncio
from datetime import datetime, timedelta
import requests
import json
import time
from datetime import datetime, timedelta
from typing import AsyncGenerator, List, Dict, Any
# Импорт внутренних модулей
from src_oop.core.scraper import HTTPClient
from src_oop.core.database import Database
from src_oop.core.utils_general import load_api_tokens
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.wb_api.measurements.config import google_table

logger = logging.getLogger(__name__)

class Measurements:
    def __init__(self, date_from: str = None, date_to: str = None):
        self.url = "https://seller-analytics-api.wildberries.ru/api/analytics/v1/warehouse-measurements"

        self.delay = 60.1  # Согласно лимиту WB: 1 запрос в минуту
        self.retries = 5

        # === Настройки гугл-таблицы ===
        self.google_table = google_table["title"]
        self.google_sheet = google_table["sheet_name"]
        self.google_connect = GoogleTabs(self.google_table, self.google_sheet)


    async def get_measurements(self, account: str, client: Any, date_from: str = None, date_to: str = None) -> AsyncGenerator[List[Dict], None]:
        """Генератор для получения данных о замерах"""
        # WB ожидает формат ISO 8601: YYYY-MM-DDTHH:MM:SSZ
        if date_from is None:
           date_from = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
           date_to = (datetime.now()).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            date_from = date_from
            date_to = date_to
        offset = 0
        limit = 1000

        while True:
            params = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "limit": limit,
                "offset": offset
            }

            # Делаем запрос через твой HTTPClient
            response = await client.get(self.url, params=params, delay=self.delay, retries=self.retries)

            # Если данных нет, выходим из цикла
            if response is None:
                logger.error(f"{account} Не удалось получить данные (None)")
                break

            # Согласно документации структура: {"data": {"reports": [...], "total": 100}}
            data_body = response.get("data", {})
            reports = data_body.get("reports", [])
            total = data_body.get("total", 0)
            # Добавляем информацию об аккаунте в каждый замер
            for rep in reports:
                rep["account"] = account 
            # Если отчетов нет, значит мы достигли конца данных
            if not reports:
                logger.info(f"Для ЛК {account} отчетов больше нет.")
                break

            yield reports  # Отдаем пачку данных наружу
            
            offset += len(reports)
            logger.info(f"По ЛК {account} загружено {offset} из {total}")

            # Если мы загрузили всё, что было указано в total, выходим
            if offset >= total:
                break

    

    def set_data(self, df):
        # Вставляем данные в таблицу Годовой план закупа 2026 в лист БД Заказы
        self.google_connect.set_df_to_google(df)