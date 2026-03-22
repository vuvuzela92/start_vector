from sqlalchemy import Date, String, BigInteger, Numeric, Integer

historical_stocks_fbs_service_table = {
    "title": "historical_stocks_fbs_service",
    "columns" : {
        "transaction_date": Date,
        "end_of_day_balance": Integer,
        "wild": String
    },
    "key_columns": ["transaction_date", "wild"]
}