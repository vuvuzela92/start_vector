# Модуль описывает таблицы в БД
from sqlalchemy import BigInteger, String, Boolean, DateTime, Numeric


# === vector_db ===

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