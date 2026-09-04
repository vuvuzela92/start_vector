# Database Access Management

Модуль централизованного управления доступами сотрудников и микросервисов к
PostgreSQL и ClickHouse.

## Первый инкремент

Реализован контракт API и единая модель ролей:

- `full_access` — полный доступ к выбранной базе;
- `read_all` — чтение выбранной базы или схемы;
- `read_tables` — чтение явно перечисленных таблиц.

Для сотрудника создаётся индивидуальная учётная запись (`human`). Для
микросервиса создаётся отдельная учётная запись на сочетание «микросервис ×
среда × СУБД» (`service`); общий пароль между сервисами не используется.
Пароли не передаются в API и Telegram-боте: для сервиса хранится только ссылка
на секрет в Vault или другом хранилище секретов.

Руководитель создаёт распоряжение без второго подтверждения. Оно проходит
жизненный цикл `pending → active`, где `pending` означает ожидание технического
применения прав. При ошибке исполнитель фиксирует статус `failed`, а при отзыве
доступа — `revoked`.

На этой стадии модуль не подключается к корпоративным СУБД и не выполняет
`GRANT` или `REVOKE`. Он сохраняет заявки и журнал аудита в отдельной
служебной PostgreSQL-базе.

## Запуск

```powershell
venv\Scripts\python.exe -m uvicorn src_oop.jobs.database_access_management.api:app --host 127.0.0.1 --port 8010
```

После запуска доступны:

```text
GET /health
GET /v1/access-levels
POST /v1/access-grants
GET /v1/database-targets
POST /v1/database-targets
```

Пример персональной заявки:

```json
{
  "principal": {
    "principal_id": "ivanov_i",
    "principal_type": "human",
    "login_name": "ivanov_i",
    "display_name": "Иванов Иван",
    "secret_ref": "env://EMPLOYEE_IVANOV_PASSWORD"
  },
  "target_id": "analytics-postgresql-prod",
  "engine": "postgresql",
  "level": "read_all",
  "scope": {"database": "analytics"},
  "reason": "Работа с отчётами",
  "requested_by": "manager_1"
}
```

Для MVP исполнитель поддерживает административную ссылку цели
`env://postgresql/admin`, которая использует существующие `DB_*` переменные.
Ссылка на пароль учётной записи имеет формат `env://ИМЯ_ПЕРЕМЕННОЙ`. В рабочем
контуре эти ссылки должны быть заменены на ссылки Vault без изменения API.

При удалении пользователя PostgreSQL сервис сначала передаёт объекты текущей
базы технической роли `vector_admin`, затем отзывает его прямые права и удаляет
логин в той же транзакции. Таблицы и данные не удаляются.

## Telegram-бот

Для запуска укажите `DB_MANAGER_SV_TOKEN` и ID управляющей Telegram-группы
в `DATABASE_ACCESS_MANAGER_TELEGRAM_CHAT_IDS`, затем выполните:

```powershell
venv\Scripts\python.exe -m src_oop.jobs.database_access_management.run
```

Интерактивная документация API: `http://127.0.0.1:8010/docs`.

## Подготовка служебной базы

Для MVP служебные таблицы размещаются в схеме `access_management` базы из
переменных `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST` и `DB_PORT`.
Схема не смешивается с бизнес-таблицами `public`. При необходимости можно
переопределить её переменной `DATABASE_ACCESS_MANAGEMENT_SCHEMA` или указать
отдельную строку подключения в `DATABASE_ACCESS_MANAGEMENT_DATABASE_URL`.

После создания пустой базы однократно выполните:

```powershell
venv\Scripts\python.exe -m src_oop.jobs.database_access_management.setup
```

Команда создаст таблицы `access_grants` и `access_audit_log`. Она не выдаёт
доступы в PostgreSQL или ClickHouse.
