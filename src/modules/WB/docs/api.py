# Для получения данных из раздела Документы в ЛК ВБ
import aiohttp
import asyncio
from datetime import datetime, timedelta
from src.core.utils_general import load_api_tokens



async def get_doc_list_wb(
    account: str,
    token: str,
    session: aiohttp.ClientSession,
    date_from=datetime.now() - timedelta(days=28),
    date_to=datetime.now() - timedelta(days=1),
    limit=50
):
    """Возвращает список всех документов за период для двух категорий."""
    doc_names = ('weekly-implementation-report', 'redeem-notification')
    all_documents = []
    url = "https://documents-api.wildberries.ru/api/v1/documents/list"
    headers = {"Authorization": token}
    delay = 10
    attempts = 5

    for category in doc_names:
        offset = 0
        while True:
            params = {
                "beginTime": date_from.strftime("%Y-%m-%d"),
                "endTime": date_to.strftime("%Y-%m-%d"),
                "category": category,
                "limit": limit,
                "offset": offset
            }

            for attempt in range(attempts + 1):
                try:
                    async with session.get(url, headers=headers, params=params, timeout=60) as res:
                        if res.status == 200:
                            data = await res.json()
                            documents = data.get('data', {}).get('documents', [])
                            if not documents:
                                break  # Выход из while, документов больше нет
                            for doc in documents:
                                doc['account'] = account
                                doc['doc_type'] = category  # добавляем метку категории
                            all_documents.extend(documents)
                            offset += limit  # переходим к следующей странице
                            break  # успех, выходим из цикла попыток
                        else:
                            error_detail = await res.text() if res.content_type != 'application/json' else (await res.json()).get('detail', '')
                            if res.status in (429, 503):
                                print(f"⏳ [{account}] Ошибка {res.status}: {error_detail}. Повтор через {delay} сек.")
                                await asyncio.sleep(delay)
                                delay += attempt
                                continue  # повторяем попытку с тем же offset
                            elif res.status == 401:
                                print(f"🔑 [{account}] Ошибка 401: Неверный токен. ({error_detail})")
                                break  # нет смысла повторять
                            elif res.status == 400:
                                print(f"❓ [{account}] Ошибка 400: Плохой запрос. ({error_detail})")
                                break
                            else:
                                print(f"❌ [{account}] Ошибка {res.status}: {error_detail}")
                                break
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    print(f"💥 [{account}] Ошибка сети: {e}. Попытка {attempt+1}/{attempts+1}")
                    if attempt == attempts:
                        print(f"❌ [{account}] Превышено число попыток для категории {category}, offset={offset}")
                        break
                    await asyncio.sleep(delay)
                except Exception as e:
                    print(f"💥 [{account}] Непредвиденная ошибка: {e}")
                    break

            else:
                # Цикл попыток исчерпан без успеха – прерываем получение этой категории
                print(f"❌ [{account}] Не удалось получить данные для категории {category}")
                break

            # Проверяем, нужно ли продолжать while
            if not documents:  # условие выхода из while
                break

    return all_documents

async def fetch_doc_list_wb(tokens: dict = load_api_tokens()):
    """Функция для получения данных о документах по всем ЛК"""
    # Асинхронно обрабатываем все аккаунты и токены
    async with aiohttp.ClientSession() as session:
        # Создаем задачи для каждого аккаунта и токена
        tasks = [get_doc_list_wb(account, token, session) for account, token in tokens.items()]
        # Ожидаем завершения всех задач и собираем результаты        
        results = await asyncio.gather(*tasks)
        return results

    

async def dowload_all_documents(account: str, api_token: str, session: aiohttp.ClientSession, docs_for_download: dict = None):
    """Функция для скачивания всех документов, переданных в docs_for_download.
    """
    from src.modules.WB.docs.processing import processing_doc_list_wb

    if docs_for_download is None:
        docs_for_download = await(processing_doc_list_wb())

    url = "https://documents-api.wildberries.ru/api/v1/documents/download/all"
    headers = {
        "Authorization": api_token
    }
    params = {'params': []}

    # Выделяем список документов для конкретного аккаунта
    params['params'].extend(docs_for_download.get(account, []))

    # Интервал для запросов на ВБ
    delay = 350
    # Количество попыток в случае ошибки
    attempts = 5
    for _ in range(attempts+1):
        try:
            async with session.post(url, headers=headers, json=params, timeout=15) as res:
                    # 1. Пытаемся распарсить JSON, если это возможно
                    content_type = res.headers.get('Content-Type', '')
                    data = await res.json() if 'application/json' in content_type else None

                    # Статусы при которых инициируем повторный запрос
                    delay_statuses = (429, 400, 503)
                    error_detail = data.get('detail') if data else await res.text()

                    if res.status == 200:
                        data['account'] = account    
                        print(f"✅ [{account}] Данные получены по кабинету {account}")
                        return data
                    elif res.status in delay_statuses:
                        print(f'❓ Ошибка {error_detail}')
                        asyncio.sleep(delay)
                    else:
                         print(f'❌ [{account}] Ошибка {res.status}: {error_detail}')
                    
                    return None
        
        except aiohttp.ClientError as e:
                print(f"💥 [{account}] Ошибка сессии: {e}")
                return None
        except asyncio.TimeoutError as e:
                print(f"💥 [{account}] Таймаут: {e}")
                asyncio.sleep(delay) 
        except Exception as e:
                print(f"💥 [{account}] Непредвиденная ошибка: {e}")
                return None                
        

async def fetch_download_all_documents(tokens: dict = load_api_tokens()):
      """ Асинхронная функция для скачивания запрашиваемых документов со всех доступных аккаунтов"""
      from src.modules.WB.docs.processing import processing_doc_list_wb
      async with aiohttp.ClientSession() as session:
        # Получаем единоразово список документов, доступных для скачивания
        docs_for_download = await(processing_doc_list_wb())
        # Создаем задачи для каждого аккаунта и токена
        tasks = [dowload_all_documents(account, api_token, session, docs_for_download) for account, api_token in tokens.items()]
        # Ожидаем завершения всех задач и собираем результаты
        results = await asyncio.gather(*tasks)
        return results