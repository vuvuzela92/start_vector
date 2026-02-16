import asyncio
import aiohttp
from datetime import datetime, timedelta

# === Информация о рекламных кампаниях ===
async def get_advert_info_wb(account, token, session):
    """Асинхронная функция для получения информации о рекламных кампаниях c ВБ."""
    url = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
    headers = {"Authorization": token}
    params = {"statuses": 9}
    delay = 60
    attempts = 3
    for attempt in range(attempts+1):
        try:
            async with session.get(url, headers=headers, params=params, timeout=15) as response:
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
                    continue                    
                elif response.status == 400:
                    print(f"❓ [{account}] Ошибка 400: Плохой запрос. ({error_detail})")
                    await asyncio.sleep(delay)
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
    
# === Информация о рекламных затратах ===
async def get_advert_spend(account: str, date_from: str, date_to: str, api_token: str, session: aiohttp.ClientSession):
    """Получает данные по рекламным затратам за указанный период"""
    url = "https://advert-api.wildberries.ru/adv/v1/upd"
    headers = {
        "Authorization": api_token
    }
    params = {
        "from": date_from,
        "to": date_to
        }
    # Интервал для запросов на ВБ
    delay = 1
    # Количество попыток в случае ошибки
    attempts = 3
    for attempt in range(attempts+1):
        try:
                async with session.get(url, headers=headers, params=params, timeout=15) as res:
                    # 1. Пытаемся распарсить JSON, если это возможно
                    content_type = res.headers.get('Content-Type', '')
                    data = await res.json() if 'application/json' in content_type else None

                    if res.status == 200:
                        for d in data:
                            d['account'] = account.upper() # Добавляем информацию об аккаунте в данные
                        print(f"✅ [{account}] Данные получены за {date_from}:{date_to}")
                        return data             

                    # 2. Безопасно достаем описание ошибки
                    error_detail = data.get('detail') if data else await res.text()
                    
                    # 3. Обработка ошибок без дублирования кода
                    if res.status == 401:
                        print(f"🔑 [{account}] Ошибка 401: Неверный токен. ({error_detail})")
                    elif res.status == 429:
                        print(f"⏳ [{account}] Ошибка 429: Лимит запросов! ({error_detail})")
                        await asyncio.sleep(delay)
                        continue
                    elif res.status == 400:
                        print(f"❓ [{account}] Ошибка 400: Плохой запрос. ({error_detail})")
                        await asyncio.sleep(delay)
                    elif res.status == 503:
                        print(f"❓ [{account}] Ошибка 503: Сервис недоступен. ({error_detail})")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        print(f"❌ [{account}] Ошибка {res.status}: {error_detail}")
                    
                    return None
                    
        except aiohttp.ClientError as e:
                print(f"💥 [{account}] Ошибка сессии: {e}")
                return None
        except asyncio.TimeoutError as e:
                print(f"💥 [{account}] Таймаут: {e}")
                return None 
        except Exception as e:
                print(f"💥 [{account}] Непредвиденная ошибка: {e}")
                return None     


async def fetch_advert_spend_info(tokens: dict, date_from = (datetime.now()-timedelta(days=28)).strftime('%Y-%m-%d'), date_to = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')):
    """Асинхронная функция для получения информации о рекламных кампаниях для всех аккаунтов и токенов."""
    # Асинхронно обрабатываем все аккаунты и токены
    async with aiohttp.ClientSession() as session:
            # Создаем задачи для каждого аккаунта и токена
            tasks = [get_advert_spend(account, date_from, date_to, token,session) for account, token in tokens.items()]
            # tasks = [get_advert_spend(account, '2025-12-31', '2026-01-01', token,session) for account, token in tokens.items()]            
            # Ожидаем завершения всех задач и собираем результаты
            results = await asyncio.gather(*tasks)
            return results   