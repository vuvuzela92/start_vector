from wb_client import WildberriesClient, load_api_tokens
import aiohttp
import asyncio
from pathlib import Path
import json

# tokens = load_api_tokens()
# clients = {}
# for account, api_token in tokens.items():
#     clients[account] = WildberriesClient(api_key=api_token, session )

# В main.py
async def fetch_funnel():
    tokens = load_api_tokens()
    
    async with aiohttp.ClientSession() as session:
        clients = {}
        for name, token in tokens.items():
            clients[name] = WildberriesClient(token, session, name)
        # print(clients)
        # Собираем результаты
        results = {}
        tasks = [client.get_funnel() for _, client in clients.items()]
        results = await asyncio.gather(*tasks)
        return results

# 1. Находим папку, где лежит main.py
BASE_DIR = Path(__file__).parent  # D:\Pytnon_scripts\start_vector\src_oop\wb

# 2. Строим путь к папке data (создаём иерархию)
DATA_DIR = BASE_DIR / 'data'
DATA_DIR.mkdir(exist_ok=True)  # ← Создаёт папку, если её нет!

# 3. Строим путь к файлу
file_path = DATA_DIR / 'funnel.json'
unprocessed_data = asyncio.run(fetch_funnel())
# 4. Сохраняем данные
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(unprocessed_data, f, ensure_ascii=False, indent=2)

print(f"✅ Данные сохранены в {file_path}")