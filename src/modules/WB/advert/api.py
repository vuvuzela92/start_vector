import asyncio
import aiohttp


async def get_advert_info_wb(account, token, session):
    """Асинхронная функция для получения информации о рекламных кампаниях c ВБ."""
    url = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
    headers = {"Authorization": token}
    params = {"statuses": 9}
    delay = 60
    attempts = 3
    for attempt in attempts:
        try:
            async with session.get(url, headers=headers, params=params, timeout=10) as response:
                # 1. Сразу пытаемся распарсить JSON, если это возможно
                content_type = response.headers.get('Content-Type', '')
                data = await response.json() if 'application/json' in content_type else None
                
                if response.status == 200:
                    data['account'] = account  # Добавляем информацию об аккаунте в данные
                    print(f"✅ [{account}] Данные получены")
                    return data
                
                # 2. Безопасно достаем описание ошибки
                error_detail = data.get('detail') if data else await response.text()
                
                # 3. Обработка ошибок без дублирования кода
                if response.status == 401:
                    print(f"🔑 [{account}] Ошибка 401: Неверный токен. ({error_detail})")
                elif response.status == 429:
                    print(f"⏳ [{account}] Ошибка 429: Лимит запросов! ({error_detail})")
                    await asyncio.sleep(delay)
                    attempt +=1
                    continue                    
                elif response.status == 400:
                    print(f"❓ [{account}] Ошибка 400: Плохой запрос. ({error_detail})")
                    await asyncio.sleep(delay)
                    attempt +=1
                    continue
                else:
                    print(f"❌ [{account}] Ошибка {response.status}: {error_detail}")
                    
                return None
                
        except Exception as e:
            print(f"💥 [{account}] Непредвиденная ошибка: {e}")
            return None
    
async def fetch_advert_info(tokens: dict):
    """Асинхронная функция для получения информации о рекламных кампаниях для всех аккаунтов и токенов."""
    # Асинхронно обрабатываем все аккаунты и токены
    async with aiohttp.ClientSession() as session:
            # Создаем задачи для каждого аккаунта и токена
            tasks = [get_advert_info_wb(account, token, session) for account, token in tokens.items()]
            # Ожидаем завершения всех задач и собираем результаты
            results = await asyncio.gather(*tasks)
            return results