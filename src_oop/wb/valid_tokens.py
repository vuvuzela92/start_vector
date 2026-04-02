import aiohttp
import asyncio
from src_oop.core.utils_general import load_api_tokens

async def get_failed_tokens(account_name, token):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        response = await session.get(
        url="https://common-api.wildberries.ru/ping",
        headers={
            "Authorization": f"Bearer {token}"
        }
        )
        if response.status == 200:
            print(f"{account_name}: {response.status}")
        if response.status == 401:
            print(f"{account_name}: {response.status}")
            return None
        return None


async def main():
    tokens = load_api_tokens()
    tasks = [
        asyncio.create_task(get_failed_tokens(account_name, token))
        for account_name, token in tokens.items()
    ]
    await asyncio.gather(*tasks)

# python -m src_oop.wb.valid_tokens
if __name__ == "__main__":
    asyncio.run(main())