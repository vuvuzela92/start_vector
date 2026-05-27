# Payments Analytics API

Минимальное FastAPI-приложение для запуска обновления аналитики платежей из Google Sheets.

Приложение не содержит расчетную бизнес-логику. Оно вызывает существующий service:

```text
src_oop/jobs/calculation_of_purchases_china/orders_white_balance_analytics.py
```

## Endpoint-ы

Проверка API:

```text
GET /health
```

Запуск обновления аналитики:

```text
POST /jobs/calculation-of-purchases-china/payments-analyze/run
```

Protected endpoint принимает токен в заголовке:

```text
Authorization: Bearer <token>
```

## Состав Docker image

Dockerfile копирует в image только:

- `apps/payments_analytics_api/app`
- `src_oop/core/my_gspread.py`
- `src_oop/core/utils_general.py`
- `src_oop/jobs/calculation_of_purchases_china/config.py`
- `src_oop/jobs/calculation_of_purchases_china/orders_white_balance_analytics.py`

Остальные папки проекта в image не копируются.

## Подготовка .env

Из директории `apps/payments_analytics_api`:

```powershell
Copy-Item .env.example .env
```

Заполни `.env`:

```env
GOOGLE_SHEETS_WEBHOOK_TOKEN=your_secret_token
CREDS_DIR=creds
TOKENS_FILE=tokens.json
```

Токен можно сгенерировать:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Не коммить `.env` и файлы из `creds`.

## Google credentials

В корне проекта должен быть файл:

```text
creds/creds.json
```

`docker-compose.yml` пробрасывает папку `creds` в контейнер только на чтение:

```yaml
volumes:
  - ../../creds:/app/creds:ro
```

## Запуск через Docker Compose

Из директории `apps/payments_analytics_api`:

```bash
docker compose build
docker compose up -d
```

Проверить логи:

```bash
docker compose logs -f payments-analytics-api
```

Остановить:

```bash
docker compose down
```

## Локальный запуск без Docker

Из корня проекта:

```bash
pip install -r apps/payments_analytics_api/requirements.txt
python -m uvicorn apps.payments_analytics_api.app.main:app --host 0.0.0.0 --port 8000
```

Или из директории `apps/payments_analytics_api`:

```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Проверка

Health check:

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8000/health"
```

Запуск job:

```powershell
$token = "your_secret_token"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/jobs/calculation-of-purchases-china/payments-analyze/run" `
  -Headers @{ Authorization = "Bearer $token" }
```

Внимание: POST-запрос запускает реальное обновление листа `payments_analyze_sheet`.

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
