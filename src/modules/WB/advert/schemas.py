from sqlalchemy import BigInteger, String, Boolean, DateTime

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