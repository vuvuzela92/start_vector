# Payments Analytics API

Минимальное FastAPI-приложение для запуска обновления аналитики платежей.

Приложение не импортирует production-логику напрямую. Оно запускает существующую команду основного проекта через `subprocess` из рабочей директории, заданной переменной `PAYMENTS_ANALYZE_PROJECT_DIR`.

## Endpoint-ы

Проверка API:

```text
GET /health
```

Запуск обновления аналитики:

```text
POST /jobs/calculation-of-purchases-china/payments-analyze/run
```

Protected endpoint принимает токен:

```text
Authorization: Bearer <token>
```

## Настройки

Создай `.env` рядом с этим README:

```powershell
Copy-Item .env.example .env
```

Минимальные переменные:

```env
GOOGLE_SHEETS_WEBHOOK_TOKEN=your_secret_token
PAYMENTS_ANALYZE_PROJECT_DIR=/app/project
PAYMENTS_ANALYZE_COMMAND=python -c "from src_oop.jobs.calculation_of_purchases_china.run import update_orders_white_balance_analytics; update_orders_white_balance_analytics()"
PAYMENTS_ANALYZE_TIMEOUT_SECONDS=900
```

`PAYMENTS_ANALYZE_PROJECT_DIR` - путь к основному проекту внутри контейнера.

`PAYMENTS_ANALYZE_COMMAND` - команда запуска существующей job. По умолчанию используется прямой запуск нужной функции, без импорта полного `tasks_registry`.

## Docker volume

`docker-compose.yml` монтирует корень основного проекта в контейнер:

```yaml
volumes:
  - ../../:/app/project
```

Так как `docker-compose.yml` находится в `apps/payments_analytics_api`, путь `../../` указывает на корень основного проекта.

## Сборка и запуск

Из директории `apps/payments_analytics_api`:

```bash
docker compose down
docker compose build --no-cache
docker compose up -d
```

Проверить логи:

```bash
docker compose logs -f
```

## Проверка health endpoint-а

```bash
curl http://127.0.0.1:8000/health
```

Ожидаемый ответ:

```json
{"status":"ok"}
```

## Проверка запуска job

Внимание: POST-запрос запускает реальное обновление листа `payments_analyze_sheet`.

```powershell
$token = "your_secret_token"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/jobs/calculation-of-purchases-china/payments-analyze/run" `
  -Headers @{ Authorization = "Bearer $token" }
```

## Диагностика IndexError на Path.parents

Если контейнер падает с ошибкой вида:

```text
IndexError: 3
```

и в traceback есть строка:

```python
Path(__file__).resolve().parents[...]
```

значит код ошибочно пытается вычислить путь к основному проекту относительно файла приложения.

В Docker этого делать не нужно. Основной проект должен задаваться явно:

```env
PAYMENTS_ANALYZE_PROJECT_DIR=/app/project
```

А `docker-compose.yml` должен монтировать проект:

```yaml
volumes:
  - ../../:/app/project
```

## Google Apps Script

Apps Script не сможет обратиться к `localhost` или `127.0.0.1`, потому что выполняется на серверах Google.

Для запуска из Google Sheets нужен публичный HTTPS URL:

```text
https://your-domain.example/jobs/calculation-of-purchases-china/payments-analyze/run
```

Пример:

```javascript
function runPaymentsAnalytics() {
  const url = 'https://your-domain.example/jobs/calculation-of-purchases-china/payments-analyze/run';
  const token = PropertiesService
    .getScriptProperties()
    .getProperty('PAYMENTS_ANALYTICS_WEBHOOK_TOKEN');

  const response = UrlFetchApp.fetch(url, {
    method: 'post',
    headers: {
      Authorization: `Bearer ${token}`,
    },
    muteHttpExceptions: true,
  });

  SpreadsheetApp.getUi().alert(response.getContentText());
}
```
