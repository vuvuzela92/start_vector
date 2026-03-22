import pandas as pd

class Process:
    def __init__(self, data: list):
        self.data = data

    def process_historical_stocks(self)-> pd.DataFrame:
        if self.data is None:
            return pd.DataFrame()
        stock_list = []
        for item in self.data:
            wild = item.get("product_id")
            for transaction in item.get("data"):
                stock_list.append({
                    "wild": wild,
                    "transaction_date": transaction["transaction_date"],
                    "end_of_day_balance": transaction["end_of_day_balance"]
                })

        df = pd.DataFrame(stock_list)
        return df