# WB Acceptance Acts

## Назначение модуля

Модуль `src_oop/jobs/wb_api/acceptance_acts` реализует новый OOP pipeline
для обработки актов приёма-передачи Wildberries.

Pipeline предназначен для:

- получения списка документов WB;
- скачивания архивов с документами;
- распаковки вложенных Excel-файлов;
- parsing и валидации табличных данных;
- нормализации строк под целевые таблицы PostgreSQL;
- chunk-wise записи в БД через upsert;
- non-fatal refresh materialized view для FBS после успешной записи.

## Поддерживаемые направления

Поддерживаются три режима запуска:

- `fbs`
- `fbo`
- `all`

Для запуска используются entrypoint-функции из
[run.py](C:/Users/123/Desktop/start_vector/src_oop/jobs/wb_api/acceptance_acts/run.py):

- `run_fbs_acceptance_acts(...)`
- `run_fbo_acceptance_acts(...)`
- `run_all_acceptance_acts(...)`

## Что делает модуль

Pipeline выполняет следующие шаги:

1. Получает список документов WB за указанный период.
2. Скачивает архивы по найденным `serviceName`.
3. Извлекает Excel-файлы из архивов.
4. Определяет структуру Excel и парсит данные.
5. Валидирует parse result.
6. Нормализует строки отдельно для FBS и FBO.
7. Пишет данные в PostgreSQL chunk-wise через upsert.
8. Для FBS после успешной записи выполняет
   `REFRESH MATERIALIZED VIEW public.check_act_fbs`.

## Безопасный dry-run

При `dry_run=True` используется `DryRunAcceptanceActsRepository`.

Что это означает:

- API, скачивание архивов, parsing и normalizer работают реально;
- запись в PostgreSQL не выполняется;
- для FBS refresh materialized view не выполняется реально;
- в `JobRunResult.warnings` появляются служебные предупреждения:
  - `dry-run: database write skipped`
  - `dry-run: refresh skipped`

Dry-run нужен как основной безопасный сценарий перед любым real write.

## Запуск за конкретный период

Все entrypoint-функции поддерживают параметры:

- `date_from`
- `date_to`
- `days_back`
- `dry_run`
- `accounts`

Для точного ручного запуска рекомендуется явно задавать:

- `date_from`
- `date_to`
- `days_back=None`
- `accounts=["Вектор"]` или другой один конкретный аккаунт

Пример периода:

- `date_from=date(2026, 6, 9)`
- `date_to=date(2026, 6, 10)`

## Запуск только для одного аккаунта

Ограничение по аккаунту задаётся через параметр:

```python
accounts=["Вектор"]
```

Это позволяет:

- не запускать все аккаунты сразу;
- контролировать объём документов;
- безопасно проверять dry-run и real write на коротком периоде.

## Примеры ручного запуска

### FBS dry-run

```python
import asyncio
from datetime import date

from src_oop.jobs.wb_api.acceptance_acts.run import run_fbs_acceptance_acts


async def main() -> None:
    result = await run_fbs_acceptance_acts(
        date_from=date(2026, 6, 9),
        date_to=date(2026, 6, 10),
        days_back=None,
        dry_run=True,
        accounts=["Вектор"],
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

### FBS real write

```python
import asyncio
from datetime import date

from src_oop.jobs.wb_api.acceptance_acts.run import run_fbs_acceptance_acts


async def main() -> None:
    result = await run_fbs_acceptance_acts(
        date_from=date(2026, 6, 9),
        date_to=date(2026, 6, 10),
        days_back=None,
        dry_run=False,
        accounts=["Вектор"],
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

### FBO dry-run

```python
import asyncio
from datetime import date

from src_oop.jobs.wb_api.acceptance_acts.run import run_fbo_acceptance_acts


async def main() -> None:
    result = await run_fbo_acceptance_acts(
        date_from=date(2026, 6, 9),
        date_to=date(2026, 6, 10),
        days_back=None,
        dry_run=True,
        accounts=["Вектор"],
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

### All dry-run

```python
import asyncio
from datetime import date

from src_oop.jobs.wb_api.acceptance_acts.run import run_all_acceptance_acts


async def main() -> None:
    result = await run_all_acceptance_acts(
        date_from=date(2026, 6, 9),
        date_to=date(2026, 6, 10),
        days_back=None,
        dry_run=True,
        accounts=["Вектор"],
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

## Ограничения и текущие риски

- FBS real write уже проверен на одном аккаунте и коротком периоде.
- FBO runtime write пока не подтверждён в рабочем окружении.
- FBO dry-run и runtime могут требовать окружение с доступом к
  `documents-api.wildberries.ru`.
- `partial` сейчас может возникать из-за fallback Excel parser.
- `partial` при fallback не обязательно означает ошибку бизнес-логики.

## Как интерпретировать JobRunResult

Основные поля:

- `documents_found`:
  сколько документов найдено по списку WB.
- `documents_downloaded`:
  сколько документов реально скачано.
- `excel_files_found`:
  сколько Excel-файлов извлечено из архивов.
- `parsed_success`:
  сколько Excel прошли parser без warnings.
- `parsed_partial`:
  сколько Excel распознаны, но с warnings.
- `parsed_failed`:
  сколько Excel не удалось обработать.
- `normalized_rows`:
  сколько строк normalizer подготовил к записи.
- `written_rows`:
  сколько строк repository считает записанными/upsert-обработанными.
- `warnings`:
  список нефатальных проблем и диагностических сообщений.
- `errors`:
  список фатальных ошибок запуска.

## Безопасность запусков

Перед любым real write:

1. Сначала выполнить dry-run.
2. Ограничить запуск одним аккаунтом.
3. Ограничить запуск коротким периодом.
4. Проверить `errors`.
5. Проверить `normalized_rows`.
6. Только потом запускать `dry_run=False`.

Для FBO real write пока требуется отдельное подтверждение успешного FBO dry-run.

## Что нельзя делать без отдельной проверки

- Запускать `run_all` в write-режиме.
- Запускать все аккаунты без ограничения.
- Писать FBO в БД без успешного dry-run.
- Подключать модуль в `tasks_registry.py` без отдельного решения.
