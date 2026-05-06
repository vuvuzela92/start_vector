import json

TOKENS_FILE_NAME="creds/tokens.json"
async def get_wb_tokens() -> dict:
    with open(TOKENS_FILE_NAME, "r", encoding='utf-8') as file:
        tokens = json.load(file)
    return {acc.capitalize(): token for acc, token in tokens.items()}