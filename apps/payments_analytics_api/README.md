# Payments Analytics API

Отдельное FastAPI-приложение для запуска обновления аналитики платежей через webhook.

API запускает существующую job командой из `PAYMENTS_ANALYZE_COMMAND` в директории `PAYMENTS_ANALYZE_PROJECT_DIR`.

## Структура

```text
apps/payments_analytics_api/
  app/
    __init__.py
    main.py
    runner.py
    settings.py
  requirements.txt
  Dockerfile
  docker-compose.yml
  .env.example
  README.md
```

## Endpoint-ы

```text
GET  /health
POST /jobs/calculation-of-purchases-china/payments-analyze/run
```

## Подготовка

```bash
cd apps/payments_analytics_api
cp .env.example .env
```

В PowerShell:

```powershell
cd apps\payments_analytics_api
Copy-Item .env.example .env
```

Заполни `.env` минимумом:

```env
GOOGLE_SHEETS_WEBHOOK_TOKEN=change_me
PAYMENTS_ANALYZE_PROJECT_DIR=/app/project
PAYMENTS_ANALYZE_COMMAND=python -c "from src_oop.jobs.calculation_of_purchases_china.orders_white_balance_analytics import OrdersWhiteBalanceAnalyticsService; OrdersWhiteBalanceAnalyticsService().run()"
PAYMENTS_ANALYZE_TIMEOUT_SECONDS=900
API_HOST=0.0.0.0
API_PORT=8000
```

Для локального запуска на Windows вне Docker можно указать:

```env
PAYMENTS_ANALYZE_PROJECT_DIR=C:\Users\123\Desktop\start_vector
```

Если оставить контейнерный путь `/app/project`, приложение попробует автоматически найти корень локального проекта (где есть `main.py` и `src_oop`).

## Docker запуск

```bash
cd apps/payments_analytics_api
docker compose config
docker compose build --no-cache
docker compose up -d
docker compose ps
docker compose logs -f payments-analytics-api
```

`docker-compose.yml` монтирует основной проект в контейнер:

```yaml
volumes:
  - ../../:/app/project
```

## Проверка API

Проверка health:

```bash
curl http://127.0.0.1:8000/health
```

Ожидаемый ответ:

```json
{"status":"ok"}
```

Проверка protected endpoint без токена:

```bash
curl -X POST http://127.0.0.1:8000/jobs/calculation-of-purchases-china/payments-analyze/run
```

Ожидаемо: `401 Unauthorized`.

Проверка protected endpoint с токеном:

```bash
curl -X POST http://127.0.0.1:8000/jobs/calculation-of-purchases-china/payments-analyze/run \
  -H "Authorization: Bearer YOUR_TOKEN"
```

## Диагностика

### Ошибка `IndexError: 3`

Причина:
код пытается вычислить корень проекта через `Path(__file__).resolve().parents[...]`.

Решение:
использовать `PAYMENTS_ANALYZE_PROJECT_DIR=/app/project`.

### Ошибка `ModuleNotFoundError`

Причина:
в контейнере нет зависимости, которая нужна основной job.

Решение:
добавить недостающую зависимость в `requirements.txt` этого приложения
или изменить способ запуска job.

### Ошибка `401`

Причина:
неверный или отсутствующий токен.

Решение:
проверить `GOOGLE_SHEETS_WEBHOOK_TOKEN` в `.env` и заголовок
`Authorization: Bearer ...`.

### Ошибка `500`

Причина:
FastAPI доступен, но упала основная job.

Решение:

```bash
docker compose logs -f payments-analytics-api
docker compose exec payments-analytics-api bash
cd /app/project
python main.py update_orders_white_balance_analytics
```
