# start_vector

Программы для обслуживания селлера

## 🚀 Быстрый старт

## 🛠 Архитектура проекта
src/core - файлы для хранения общих функций для гугл-таблиц, таблиц в БД и их описание.
src/modules/COMETA - модуль для работы с сервисом автоматизации рекламы Комета. 
src/GOOGLE_SHEETS - модуль для работы с гугл-таблицами.
src/WB - модуль для работы с API WB.

### 1. Клонирование репозитория
```bash
git clone git@github.com:vuvuzela92/start_vector.git
cd your_project
```

### 2. Настройка окружения
```bash
python -m venv venv
source venv/bin/activate  # Для Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Настройка чувствительных данных
Нужно создать файл tokens.json для хранения
```bash
{
  "ИП_Иванов": "your_api_token_here",
  "ИП_Петров": "another_api_token_here"
}
```