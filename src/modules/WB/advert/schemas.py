# Модуль описывает таблицы в БД
from sqlalchemy import BigInteger, String, Boolean, DateTime, Numeric, Float


# === vector_db ===

# --- Рекламные таблицы ---
# Таблица advert_campaigns_info в БД vector_db, содержит данные о рекламных кампаниях
advert_campaigns_info_dict = {
    "advert_campaigns_info": {
        'campaign_id': BigInteger,
        'campaign_name': String,
        'bid_type': String,
        'nm_id': BigInteger,
        'search_bid': BigInteger,
        'recommendations_bid': BigInteger,
        'payment_type': String,
        'recommendations': Boolean,
        'search': Boolean,
        'created_at_campaign': DateTime,
        'account': String
    }
}

# Таблица advert_spend_info_dict в БД vector_db, содержит данные о затратах по рекламным кампаниям
advert_spend_info_dict = {
    "advert_spend": {
        'upd_time': DateTime(timezone=True),
        'camp_name': String(255),
        'payment_type': String(255),
        'upd_num': BigInteger,
        'upd_sum': Numeric,
        'advert_id': BigInteger,
        'advert_type': BigInteger,
        'advert_status': BigInteger,
        'date': DateTime,
        'account': String
    }
}

# --- Данные из раздела Документы на ВБ --- 
weekly_implementation_report_dict = {
    "weekly_implementation_report": {
        '№': String(255),
        'Наименование': String(255),
        'Документ основание': String(255),
        'Дата': DateTime(),
        '№ документа': Numeric(12,2),
        'Сумма, руб.': Numeric(12,2),
        'в т.ч НДС, руб.': Numeric(12,2),
        'account': String(255)
    }
}
