# Для описания таблиц в vector_db
from sqlalchemy import BigInteger, String, Boolean, DateTime, Numeric, Float

# Таблица weekly_implementation_report
weekly_implementation_report_dict = {
    "weekly_implementation_report": {
        '№': String(10),
        'title': String(255),
        'supporting_document': String(255),
        'date': DateTime,
        'doc_num': Numeric(12,2),
        'sum_rub': Numeric(12,2),
        'vat_rub': Numeric(12,2),
        'account': String(255)
    }
}


# Таблица redeem_notification
redeem_notification_dict = {
    "redeem_notification": {
        '№': String(255),
        'wild': String(255),
        'subject_name': String(255),
        'quantity': BigInteger,
        'sum_rub_with_vat': Numeric(12,2),
        'vat_rate': String(10),
        'vat_sum_rub': Numeric(12,2),
        'kiz': String(255),
        'doc_name': String(255),
        'account': String(255)
    }
}