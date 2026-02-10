# Импортируем библиотеки для работы с системой и путями
# import os
# from  pathlib import Path
# import sys
import pandas as pd

# === Данные рекламных кампаний ===
# Обработка данных
def extract_campaign_info(advert_info_data):
    """
    Преобразует вложенную структуру данных API Wildberries в плоский список словарей.
    Это упрощает дальнейший анализ данных или их экспорт в Excel/Pandas.
    """
    # Инициализируем список для хранения финальной информации о рекламных кампаниях 
    campaign_info = []

    for account_data in advert_info_data:
        # ПРОВЕРКА: Если данные по аккаунту пустые или API вернул ошибку, пропускаем этот аккаунт
        if not account_data or 'adverts' not in account_data:
            continue
            
        # Добавляем информацию об аккаунте (берем из ключа, который мы добавили вручную в get_advert_info_wb)
        account_name = account_data.get('account', 'Unknown')
        
        # Цикл по каждой рекламной кампании (advert) внутри данных текущего аккаунта
        for advert in account_data.get('adverts', []):
            
            # БЕЗОПАСНОЕ ИЗВЛЕЧЕНИЕ ВЛОЖЕННЫХ ОБЪЕКТОВ:
            # Чтобы не писать длинные проверки на каждой строке, выносим вложенные словари в переменные.
            # Используем .get() и пустые словари {} / списки [{}], чтобы код не "падал", если WB не прислал ключи.
            
            # Извлекаем настройки товара (берем первый элемент списка nm_settings, если он существует)
            nm_settings = advert.get('nm_settings', [{}])[0] if advert.get('nm_settings') else {}
            
            # Извлекаем ставки в копейках (вложены в nm_settings)
            bids = nm_settings.get('bids_kopecks', {})
            
            # Извлекаем общие настройки (название, способ оплаты) и зоны показов (placements)
            settings = advert.get('settings', {})
            placements = settings.get('placements', {})
            
            # Формируем итоговый "плоский" словарь для текущей кампании
            campaign_info.append({
                'account': account_name,
                'campaign_id': advert.get('id'),
                'campaign_name': settings.get('name'),
                
                # Тип ставки (например, unified — единая ставка, manual — ручная ставка)
                'bid_type': advert.get('bid_type'),
                
                # ID товара (артикул WB)
                'nm_id': nm_settings.get('nm_id'),
                
                # Ставка в Поиске (извлекаем из bids_kopecks -> search)
                'search_bid': bids.get('search'),
                
                # Ставка в рекомендациях (извлекаем из bids_kopecks -> recommendations)
                'recommendations_bid': bids.get('recommendations'),
                
                # Тип оплаты (например, CPC — оплата за клик, CPM — оплата за 1000 показов)
                'payment_type': settings.get('payment_type'),
                
                # Статус показов в рекомендательных полках (True/False)
                'recommendations': placements.get('recommendations'),
                
                # Статус показов в поиске (True/False)
                'search': placements.get('search'),
                
                # Дата создания кампании (безопасно достаем из вложенного объекта timestamps)
                'created_at_campaign': advert.get('timestamps', {}).get('created')
            })
            
    return campaign_info

# === Данные о затратах по рекламе ===

def process_advert_spend_info(data):
    """ Функция обрабатывает полученные данные по рекламным затратам за один день. Метод АПИ возвращает лишние даты, фунцкия так же производит фильтрацию"""
    if data:
        # Список для хранения данных
        data_list = []
        # Проходим внешним циклом по данным каждого ЛК
        for orders_lk in data:
            # Достаем отдельно данные по каждому заказу и добваляем в общий список
            for orders in orders_lk:
                data_list.append(orders)
        df = pd.DataFrame(data_list)
        df['date'] = pd.to_datetime(df['updTime'],
                                      format='ISO8601'
                                      ).dt.date
        return df
    else:
        return pd.DataFrame() 
