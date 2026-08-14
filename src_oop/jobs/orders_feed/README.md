# WB Order Feed

Сценарий загружает актуальную «Ленту заказов» из WB API и позволяет разово
перенести исторические данные из legacy-таблиц `orders` и `sales`.

Все команды ниже выполняются из каталога `start_vector`:

```bash
cd start_vector
```

## Подготовка окружения

В `.env` должны быть указаны параметры PostgreSQL и путь к файлу токенов WB:

```env
DB_HOST=
DB_PORT=5432
DB_NAME=
DB_USER=
DB_PASSWORD=

CREDS_DIR=creds
CREDS_FILE=wb_tokens.json
```

Пример файла токенов:

```json
{
  "Название кабинета": "WB_API_TOKEN"
}
```

## Создание таблицы

Создать `wb_order_feed`, PostgreSQL enum-типы и индексы без запроса к WB:

```bash
uv run python -m src_oop.jobs.orders_feed.run --create-table-only
```

## Загрузка из WB API

Все кабинеты за доступные последние 31 сутки:

```bash
uv run python -m src_oop.jobs.orders_feed.run
```

Один кабинет за последние 31 сутки:

```bash
uv run python -m src_oop.jobs.orders_feed.run --account "Название кабинета"
```

Один кабинет за ручной период:

```bash
uv run python -m src_oop.jobs.orders_feed.run --account "Название кабинета" --date-from "2026-07-23 12:00" --date-to "2026-07-24 18:30"
```

Период можно задавать в форматах:

```text
2026-07-23
2026-07-23 12:00
2026-07-23T12:00:00+03:00
23.07.2026 12:00
```

Если часовой пояс не указан, используется `Europe/Moscow`.

Одна страница WB сохраняется в PostgreSQL небольшими DB-батчами по 1 000
строк. При временном обрыве соединения текущий батч повторяется автоматически;
уже сохранённые батчи страницы повторно не записываются.

## Legacy backfill

Backfill читает только один батч за раз, преобразует его в Python через
Pydantic и выполняет идемпотентный batch-upsert.

Загрузить историю из `orders`:

```bash
uv run python -m src_oop.jobs.orders_feed.backfill --source orders
```

Загрузить историю из `sales`:

```bash
uv run python -m src_oop.jobs.orders_feed.backfill --source sales
```

Указать размер батча вручную:

```bash
uv run python -m src_oop.jobs.orders_feed.backfill --source orders --batch-size 1000
```

Загрузить только определённый период (обе даты включаются полностью):

```bash
uv run python -m src_oop.jobs.orders_feed.backfill --source orders --date-from "2025-01-01" --date-to "2025-03-31"
```

Период можно совместить с собственным размером батча:

```bash
uv run python -m src_oop.jobs.orders_feed.backfill --source sales --date-from "2025-01-01" --date-to "2025-12-31" --batch-size 1000
```

Можно указать только одну границу. Без `--date-from` и `--date-to` переносится
вся доступная история выбранной таблицы. Фильтрация выполняется по `date_from`.

По умолчанию используется батч из 2 000 исходных строк. Каждый батч
сохраняется отдельной транзакцией, поэтому ранее записанные данные остаются в
таблице при ошибке последующего батча.

Legacy-строки сохраняются с `account=NULL`, `chrt_id=NULL` и источником
`orders` или `sales`. Новые строки API имеют источник `order_feed`.
