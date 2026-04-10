from sqlalchemy import String, BigInteger, Numeric

sima_land_items_table = {
    "title": "sima_land_items",
    "columns": {
        'sid': BigInteger,
        'name': String(255),
        'balance': BigInteger,
        'price': Numeric(15, 2),
        'price_max': Numeric(15, 2),
        'boxtype_id': BigInteger,
        'box_depth': Numeric(15, 2),
        'box_width': Numeric(15, 2),
        'box_height': Numeric(15, 2),   
        'width': Numeric(15, 2),
        'height': Numeric(15, 2),   
        'per_package': BigInteger,
        'photo_url': String(255)
    },
    "unique_keys": ["sid"]
    }