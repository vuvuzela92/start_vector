import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from src_oop.core.scraper import HTTPClient

logger = logging.getLogger(__name__)

class WMSStockService:
    def __init__(self):
        self

    async def get_historical_stocks(
    self,
    session: aiohttp.ClientSession,
    date_from: str = None,
    date_to: str = None,
    warehouse_id: int = 1,
    page_size: int = 5000,
    api_key: Optional[str] = None,  # Если API требуется ключ
    ) -> List[Dict]:
        """Получение исторических остатков с пагинацией (Async version)"""
        
        # 1. Обработка дат
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")
        if date_to is None:
            date_to = datetime.now().strftime("%Y-%m-%d")

        # 2. URL без пробелов
        url = "https://api-routing.star-vector.ru/api/warehouse_and_balances/get_historical_stocks"
        
        # 3. Создаем клиент
        client = HTTPClient(
            session=session,
            api_key=api_key,
            timeout=30.0 
        )
        
        all_res = []
        page_num = 1
        
        while True:
            payload = {
                "date_from": date_from,
                "date_to": date_to,
                "warehouse_id": warehouse_id,
                "page_size": page_size,
                "page_num": page_num
            }
            
            # 4. Асинхронный POST-запрос через твой класс
            data = await client.post(url, json=payload, delay=1.0, retries=3)
            
            logger.debug(f"📄 Страница {page_num}: отправлен запрос")
            
            if data is None:
                logger.error(f"❌ Страница {page_num}: не получены данные")
                break
            
            # 5. Обработка ответа
            if isinstance(data, list):
                all_res.extend(data)
                logger.info(f"✅ Страница {page_num}: получено {len(data)} записей")
                print(f"✅ Страница {page_num}: получено {len(data)} записей")
                
                # 🔴 УСЛОВИЕ ВЫХОДА: если данных меньше размера страницы
                if len(data) == 0:
                    break
            else:
                logger.warning(f"⚠️ Неожиданный формат ответа: {type(data)}")
                break
            
            # 6. Следующая страница
            page_num += 1
            
            # 7. Защита от бесконечного цикла
            if page_num > 100:
                logger.warning("⚠️ Достигнут лимит страниц (100)")
                break
        
        logger.info(f"🏁 Всего собрано {len(all_res)} записей")
        return all_res
    

    async def fetch_external_stocks(self, date_from: str=None, date_to: str=None):
        async with aiohttp.ClientSession() as session:
            return await self.get_historical_stocks(
                session=session,
                date_from=date_from,
                date_to=date_to
        )