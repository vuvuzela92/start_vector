# Запуск аналитики платежей из Google Sheets

Документ описывает, как связать кнопку в Google Sheets с FastAPI endpoint-ом проекта.

## Причина DNS error

`https://YOUR_DOMAIN/jobs/calculation-of-purchases-china/payments-analyze/run` - это шаблонный URL.

`YOUR_DOMAIN` нужно заменить на реальный публичный HTTPS адрес, например:

```text
https://example.com/jobs/calculation-of-purchases-china/payments-analyze/run
```

Google Apps Script выполняется на серверах Google, а не на локальном компьютере. Поэтому Apps Script не сможет обратиться к:

```text
http://localhost:8000
http://127.0.0.1:8000
http://0.0.0.0:8000
```

Даже если FastAPI успешно запущен локально, для Apps Script endpoint должен быть доступен из интернета по HTTPS.

## Режим A. Локальная проверка FastAPI

Запуск локального сервера:

```bash
python -m uvicorn src_oop.api.app:app --host 0.0.0.0 --port 8000
```

Проверка, что API поднялся:

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8000/health"
```

Проверка защищенного endpoint-а локально:

```powershell
$token = "TOKEN_FROM_ENV"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/jobs/calculation-of-purchases-china/payments-analyze/run" `
  -Headers @{ Authorization = "Bearer $token" }
```

Важно: POST endpoint запускает реальное обновление аналитики и выгрузку на `payments_analyze_sheet`.

## Режим B. Запуск из Google Apps Script

Для запуска из Google Sheets нужен публичный HTTPS URL.

Варианты:

1. Развернуть FastAPI на сервере/VPS/облачной платформе и подключить HTTPS.
2. Для временного теста использовать HTTPS tunnel, например ngrok или cloudflared.

Пример с ngrok:

```bash
python -m uvicorn src_oop.api.app:app --host 0.0.0.0 --port 8000
ngrok http 8000
```

ngrok выдаст публичный адрес вида:

```text
https://abc123.ngrok-free.app
```

Итоговый URL для Apps Script:

```text
https://abc123.ngrok-free.app/jobs/calculation-of-purchases-china/payments-analyze/run
```

## Токен

Токен хранится в `.env` проекта:

```env
GOOGLE_SHEETS_WEBHOOK_TOKEN=your_secret_token
```

Сгенерировать токен можно так:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Токен нельзя коммитить в git и нельзя публиковать.

## Пример Google Apps Script

Лучше хранить токен в Script Properties, а не прямо в коде.

1. Открой Apps Script.
2. Project Settings.
3. Script properties.
4. Добавь свойство:

```text
PAYMENTS_ANALYTICS_WEBHOOK_TOKEN = your_secret_token
```

Код:

```javascript
function runPaymentsAnalytics() {
  const url = 'https://YOUR_PUBLIC_HTTPS_DOMAIN/jobs/calculation-of-purchases-china/payments-analyze/run';
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

`YOUR_PUBLIC_HTTPS_DOMAIN` нужно заменить на реальный публичный HTTPS адрес.

## Кнопка в Google Sheets

1. В Google Sheets добавь рисунок или изображение.
2. Нажми на него правой кнопкой.
3. Выбери `Назначить скрипт`.
4. Укажи имя функции:

```text
runPaymentsAnalytics
```

После клика по кнопке Apps Script отправит POST-запрос в FastAPI, а FastAPI запустит обновление `payments_analyze_sheet`.
