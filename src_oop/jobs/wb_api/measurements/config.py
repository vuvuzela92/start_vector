# Модуль описывает таблицы в БД
from sqlalchemy import BigInteger, Date, String, Boolean, DateTime, Numeric, Float

# Таблца для хранения данных о замерах в БД
table = {
    "title": "wb_measurements",
    "schema_definition": {
        "nm_id": BigInteger,
        "subjectName": String,
        "dim_id": String,
        "volume": Numeric(precision=10, scale=2),
        "width": Numeric(precision=10, scale=2),
        "length": Numeric(precision=10, scale=2),
        "height": Numeric(precision=10, scale=2),
        "photo_urls": String,
        "dt": DateTime,
        "account": String,
        "date": Date
    },
    "unique_keys": ["nm_id"]
    }

# Гугл-таблица для выгрузки данных о замерах
google_table = {
    "title": "Отгрузка ФБО",
    "sheet_name": "БД_Замеры_ВБ",}