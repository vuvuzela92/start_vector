import logging
import pandas as pd
import aiohttp
import asyncio
from datetime import datetime, timedelta
import requests
import json
import time

from src_oop.core.scraper import HTTPClient
from src_oop.core.utils_general import load_api_tokens, load_sima_land_tokens
from src_oop.core.database import Database
from src_oop.jobs.sima_land.config import sima_land_items_table

class SimaLandClient:
    def __init__(self, token=None, db_client=None):
        if token:
            self.token = token
        else:
            self.token = load_sima_land_tokens()['ВЕКТОР']
        self.headers = {
                "x-api-key": self.token
            }
        self.db = Database()

    def _download_all_categories_to_json(self):
        url = "https://www.sima-land.ru/api/v3/category/"
        headers = {"x-api-key": self.token}
        
        all_categories = []
        page = 1
        per_page = 50 # Оптимально для API
        
        print("🚀 Начинаю полную выгрузку категорий. Это может занять пару минут...")
        
        while True:
            params = {
                "page": page,
                "per-page": per_page,
                "is_active": 1 # Берем только те, где есть товары
            }
            
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                items = data.get('items', [])
                if not items:
                    break
                
                # Сохраняем только самое важное, чтобы файл не весил 100Мб
                for item in items:
                    all_categories.append({
                        "id": item['id'],
                        "name": item['name'],
                        "level": item['level'],
                        "path": item['path'] # Поможет понять вложенность
                    })
                
                print(f"✅ Загружено страниц: {page}", end="\r")
                
                # Проверка на последнюю страницу
                if page >= data.get('_meta', {}).get('pageCount', 0):
                    break
                    
                page += 1
                time.sleep(0.05) # Минимальная задержка
                
            except Exception as e:
                print(f"\n❌ Ошибка на странице {page}: {e}")
                break

        # Сохраняем в файл
        with open("sima_categories.json", "w", encoding="utf-8") as f:
            json.dump(all_categories, f, ensure_ascii=False, indent=4)
        
        print(f"\n\nГотово! Всего собрано категорий: {len(all_categories)}")
        print("Данные сохранены в файл: sima_categories.json")

    def get_sim_land_items(self, category_id):
        url = "https://www.sima-land.ru/api/v3/item/"
        all_items = []
        last_id = 0  
        
        params = {
            "category_id": category_id,
            "has_balance": 1,
            "is_disabled": 0,
            "is_deleted": 0,
            "has_photo": 1,
            "per-page": 50,
            "sort": "id"  
        }

        print(f"🚀 Начинаю выгрузку товаров для категории {category_id}...")

        while True:
            # Используем альтернативную пагинацию
            params["id-greater-than"] = last_id
            
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                items = data.get('items', [])
                if not items:
                    break     

                for item in items:
                    clean_item = {
                        "sid": item.get('sid'),
                        "name": item.get('name'),
                        "balance": item.get('balance', 0),
                        "price": item.get('price', 0),
                        "price_max": item.get('price_max', 0),
                        "boxtype_id": item.get('boxtype_id'),
                        "box_depth": item.get('box_depth', 0),
                        "box_width": item.get('box_width', 0),
                        "box_height": item.get('box_height', 0),
                        "width": item.get('width', 0),
                        "height": item.get('height', 0),    
                        "per_package": item.get('per_package', 0),
                        "photo_url": item.get('photoUrl', ''),
                    }
                    all_items.append(clean_item)
                
                # Обновляем last_id значением ID последнего товара в текущей пачке
                last_id = items[-1]['id']

                print(f"✅ Загружено товаров: {len(all_items)} (последний ID: {last_id})", end="\r")
                
                # Если пришло меньше, чем мы просили (per-page), значит товары кончились
                if len(items) < params["per-page"]:
                    break
                    
                time.sleep(0.1)
                
            except Exception as e:
                print(f"\n❌ Ошибка: {e}")
                break

        print(f"\nВыгрузка завершена. Итого: {len(all_items)} шт.")
        return all_items
    

    async def get_items_generator(self, category_id):
        """Асинхронный генератор: отдает товары по мере поступления страниц"""
        url = "https://www.sima-land.ru/api/v3/item/"
        last_id = 0
        params = {
            "category_id": category_id,
            "has_balance": 1,
            "per-page": 50,
            "sort": "id"
        }
  
        while True:
            params["id-greater-than"] = last_id
            
            response = await self.client.request("GET", url, params=params) 
            data = await response.json()
            items = data.get('items', [])
            
            if not items:
                break

            batch_to_yield = []
            for item in items:
                clean_item = {
                    "sid": item.get('sid'),
                    "name": item.get('name'),
                    "balance": item.get('balance', 0),
                    "price": item.get('price', 0),
                    "price_max": item.get('price_max', 0),
                    "boxtype_id": item.get('boxtype_id'),
                    "box_depth": item.get('box_depth', 0),
                    "box_width": item.get('box_width', 0),
                    "box_height": item.get('box_height', 0),
                    "width": item.get('width', 0),
                    "height": item.get('height', 0),    
                    "per_package": item.get('per_package', 0),
                    "photo_url": item.get('photoUrl', ''),
                }
                batch_to_yield.append(clean_item)
            
            # Отдаем целую страницу товаров наружу
            yield batch_to_yield
            
            last_id = items[-1]['id']
            if len(items) < params["per-page"]:
                break
            
            # Вместо time.sleep используем неблокирующую паузу
            await asyncio.sleep(0.1)

    async def process_and_save(self, category_ids):
        """Метод для запуска процесса: получает данные и сразу пишет в БД"""
        for cat_id in category_ids:
            batch_for_db = []
            
            async for items_page in self.get_items_generator(cat_id):
                batch_for_db.extend(items_page)
                
                # Если накопили достаточно для вставки в БД (например, 500 записей)
                if len(batch_for_db) >= 500:
                    await self.db.sync_data_to_postgres(batch_for_db) # Метод вставки в БД
                    batch_for_db = [] # Очищаем память!
            
            # Дописываем остатки после выхода из цикла категории
            if batch_for_db:
                await self.db.sync_data_to_postgres(batch_for_db)