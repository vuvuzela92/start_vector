import asyncio
import aiohttp
from datetime import datetime, timedelta


async def get_orders(account: str, token: str, date_from: str, api_token: str, session: aiohttp.ClientSession):
    """ Получаем данные по отчету orders с ВБ"""
    url = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'
    params = {
        'dateFrom': date_from,
        'flag': 1
    }
    headers = {
        "Authorization": api_token
    }
    try:
        async with session.get(url, headers=headers, params=params, timeout=10) as res:
            # 1. Пытаемся распарсить JSON, если это возможно
            content_type = res.headers.get('Content-Type', '')
            data = await res.json() if 'application/json' in content_type else None

            if res.status == 200:
                for d in data:
                    d['account'] = account # Добавляем информацию об аккаунте в данные
                print(f"✅ [{account}] Данные получены")
                return data

            # 2. Безопасно достаем описание ошибки
            error_detail = data.get('detail') if data else await res.text()
            
            # 3. Обработка ошибок без дублирования кода
            if res.status == 401:
                print(f"🔑 [{account}] Ошибка 401: Неверный токен. ({error_detail})")
            elif res.status == 429:
                print(f"⏳ [{account}] Ошибка 429: Лимит запросов! ({error_detail})")
            elif res.status == 400:
                print(f"❓ [{account}] Ошибка 400: Плохой запрос. ({error_detail})")
            else:
                print(f"❌ [{account}] Ошибка {res.status}: {error_detail}")
            
            return None
            
    except Exception as e:
        print(f"💥 [{account}] Непредвиденная ошибка: {e}")
        return None    


async def fetch_advert_info(tokens: dict):
    """Асинхронная функция для получения информации о рекламных кампаниях для всех аккаунтов и токенов."""
    # Асинхронно обрабатываем все аккаунты и токены
    async with aiohttp.ClientSession() as session:
            date_from = (datetime.now()-timedelta(days=0)).strftime('%Y-%m-%d')
            # Создаем задачи для каждого аккаунта и токена
            tasks = [get_orders(account, token, date_from, token, session) for account, token in tokens.items()]
            # Ожидаем завершения всех задач и собираем результаты
            results = await asyncio.gather(*tasks)
            return results