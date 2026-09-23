# Синхронизация сборочных заданий

Job `assembly_info` переносит сценарий из `warehouse_service/assembly_info.py` в ООП-структуру проекта.

## Сценарий

1. `AssemblyInfoClient` получает сборочные задания и статусы через API Wildberries.
2. `AssemblyInfoService` объединяет ответы, переводит время в `Europe/Moscow`, цены — из копеек в рубли.
3. Из результата исключаются строки без `wb_status`, дубли по `(id, supplier_status, wb_status)` и отменённые поставщиком заказы в статусе `waiting`.
4. `AssemblyInfoRepository` передает данные через `COPY` во временную таблицу и выполняет порционный upsert в `assembly_task_status_model`.
5. При повторном запуске задания с финальными статусами WB исключаются из запроса статусов; новые и активные задания проверяются дальше.

## Запись в PostgreSQL

Снимок загружается одним `COPY` во временную staging-таблицу и затем объединяется
с целевой таблицей одной транзакцией. При временном обрыве PostgreSQL повторяется
весь staging-цикл; число повторов задается через `ASSEMBLY_INFO_DB_MAX_RETRIES`.
Ограничения ожидания блокировки и выполнения SQL задаются через
`ASSEMBLY_INFO_DB_LOCK_TIMEOUT` и `ASSEMBLY_INFO_DB_STATEMENT_TIMEOUT`.

Обновление существующей строки выполняется только если хотя бы одно поле реально
изменилось. Это снижает лишнюю запись WAL и нагрузку на индексы при повторном cron-запуске.

## Инкрементальная проверка статусов

Финальными по умолчанию считаются `sold`, `canceled`, `canceled_by_client`,
`declined_by_client`, `defect` и `canceled_by_carrier`. Список можно переопределить
через `ASSEMBLY_INFO_TERMINAL_WB_STATUSES`. Заказы со статусами `sorted` и
`ready_for_pickup` продолжают проверяться, поскольку они могут перейти дальше.

Если после повторов staging-цикл не записан, job завершается с ошибкой без частичного
изменения целевой таблицы. Повторный запуск идемпотентен по ключу статуса.

## Запуск

Полный список заказов WB по-прежнему загружается для сохранения истории, но
запросы текущих статусов выполняются только для заказов моложе 11 суток. Окно
можно изменить переменной `ASSEMBLY_INFO_STATUS_LOOKBACK_DAYS`.

Для чтения уже завершённых заданий репозиторий использует составной индекс по
`wb_status`, `account` и `id`, который создаётся автоматически при первом
обращении к истории и затем используется повторными запусками.

Entrypoints:

- `python -m src_oop.jobs.assembly_info.run` — прямой CLI-запуск;
- `python main.py assembly_status_model_run` — запуск через общий реестр задач;
- `src_oop.jobs.assembly_info.run.assembly_status_model_run` — программный entrypoint.

Токены читаются существующей функцией `load_api_tokens()`. Подключение к PostgreSQL использует общий `Database` и переменные `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`.

Лимиты можно переопределить через `ASSEMBLY_INFO_TIMEOUT`, `ASSEMBLY_INFO_MAX_RETRIES` и `ASSEMBLY_INFO_CONCURRENCY`.
