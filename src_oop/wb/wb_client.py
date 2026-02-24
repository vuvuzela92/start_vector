import requests

class WildberriesClient():
    def __init__(self, api_key, timeout=30):
        # Общий url адрес
        self.common_url = "https://common-api.wildberries.ru"
        self.news_url = "https://common-api.wildberries.ru/api/communications/v2/news"
        self.api_key = api_key
        self.timeout = timeout

    def get_ping(self):
        """ Получение новостей портала продавцов """
        print(f"Здесь делаю запрос на {self.news_url} для проверки соединения. Использую ключ {self.api_key[:5]}")
        headers = {
            "Authorization": self.api_key
        }
        res = requests.get(url=self.news_url, headers=headers)
        if res.status_code == 200:
            return res.json()
        else:
            return None

# wb_token = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwOTA0djEiLCJ0eXAiOiJKV1QifQ."
# wb_token_test = "4646567890SFGSG"

# wb_client = WildberriesClient(api_key=wb_token)
# wb_client_test = WildberriesClient(api_key=wb_token_test)

# ping = wb_client.get_ping()
# ping_test = wb_client_test.get_ping()