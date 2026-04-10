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
    

def load_sima_land_tokens(filename: str = None) -> dict:
    """
    Загружает токены из JSON файла.
    
    Args:
        filename: путь к файлу с токенами. Если не указан, 
                 используется путь по умолчанию относительно проекта
    
    Returns:
        dict: загруженные токены
    """
    if filename is None:
        # Используем pathlib для кроссплатформенности
        base_path = Path(__file__).parent.parent.parent / 'creds'
        filename = base_path / 'sima_land_tokens.json'
    
    try:
        with open(filename, encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Ошибка: файл {filename} не найден")
        raise
    except json.JSONDecodeError:
        print(f"Ошибка: файл {filename} содержит некорректный JSON")
        raise
    