# Импорт необходимых библиотек для работы с операционной системой, путями.
import os
from pathlib import Path
# Импорт библиотеки для работы с JSON.
import json
# Импорт библиотеки для работы с переменными окружения.
from dotenv import load_dotenv
load_dotenv()


# Установка базовой директории и пути к файлу с учетными данными. Используем конструкцию try-except для обработки возможных ошибок при определении пути для notebook.
try:
    BASE_DIR = Path(__file__).resolve().parents[2]
except NameError:
    BASE_DIR = Path.cwd().resolve().parents[1]  

try:
    TOKENS_PATH = BASE_DIR / os.getenv("CREDS_DIR") / os.getenv('TOKENS_FILE')        
except TypeError:
    raise ValueError("Проверьте переменные окружения CREDS_DIR и CREDS_FILE. Они должны быть установлены и указывать на существующий файл с учетными данными.")

# Функция для загрузки API токенов из файла tokens.json
def load_api_tokens(filename = TOKENS_PATH):
    with open(filename, encoding= 'utf-8') as f:
        tokens = json.load(f)
        return tokens