from src_oop.core.scraper import HTTPClient
from src_oop.core.utils_general import load_api_tokens
import logging
import aiohttp
import asyncio

logger = logging.getLogger(__name__)

# Сбор данных о рекламных кампаниях на ВБ
class Promotions:
    async def get_promotions(session: aiohttp.ClientSession, start_date_time: str = None, end_date_time: str = None, all_promo: bool = False, account: str = None, api_key: str = None)->list:
        """Функция для получения данных об акциях на ВБ"""
        # Обработка дат
        if start_date_time is None:
            start_date_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        if end_date_time is None:
            end_date_time = (datetime.now() + timedelta(days=28)).strftime("%Y-%m-%dT%H:%M:%SZ")
        # URL
        url = "https://dp-calendar-api.wildberries.ru/api/v1/calendar/promotions"
        
        # Создаем клиент для соединения
        client = HTTPClient(
            session=session,
            api_key=api_key,
            timeout=30
        )

        all_data = []

        # для отслеживания не пустых ответов со стороны ВБ
        limit = 1000
        offset = 0

        while True:
            params = {
            "startDateTime": start_date_time,
            "endDateTime": end_date_time,
            "allPromo": str(all_promo).lower(),
            "limit": limit,
            "offset": offset,
            }

            # Отправляем асинхронный запрос
            data = await client.get(url, params=params, delay = 0.6)
            logger.info(f"Запрошена информация по акциям для ЛК {account}")

            # Если данные пустые выходим из цикла
            if data is None:
                logger.error(f"Данные для ЛК {account} не получены")
                break

            # Безопасное извлечение данных
            # Структура ответа WB обычно: {"data": {"promotions": [...]}}
            inner_data = data.get("data")
            if not inner_data:
                break
                
            promo_list = inner_data.get("promotions", [])
            
            if not promo_list:
                logger.info(f"Больше акций не найдено для {account}")
                break

            for promo in promo_list:
                promo["account"] = account
                all_data.append(promo) # Используем append для добавления словаря целиком

            logger.info(f"[{account}] Получено {len(promo_list)} записей (offset: {offset})")

            # 4. Проверяем, нужно ли идти на следующую страницу
            if len(promo_list) < limit:
                break
                
            offset += len(promo_list)

        logger.info(f"🏁 Всего собрано {len(all_data)} записей для {account}")
        return all_data


    async def fetch_get_promotions(tokens: dict):
        """Функция для получения данных обо всех акциях на ВБ по доступным ЛК"""
        # Асинхронно обрабатываем все аккаунты и токены
        async with aiohttp.ClientSession() as session:
                tasks = [
                    get_promotions(
                        session=session, 
                        account=account, 
                        api_key=token
                    ) for account, token in tokens.items()
                ]
                results = await asyncio.gather(*tasks)
                flat_results = [item for sublist in results for item in sublist]
                return flat_results 