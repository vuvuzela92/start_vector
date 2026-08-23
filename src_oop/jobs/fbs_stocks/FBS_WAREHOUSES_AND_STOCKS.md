# Управление FBS-складами и остатками WB

Документ описывает актуальные ручные команды для контура FBS-складов и остатков.

Важно: сценарии остатков работают только с тестовой Google Sheets-таблицей:

```text
UNIT 2.0 (tested) управление остатками -> MAIN (tested)
```

## Основные сущности

`warehouses_fbs` в PostgreSQL хранит связь между нашими логическими складами и складами WB по аккаунтам:

- `warehouse_id` - наш общий внутренний идентификатор склада.
- `warehouse_name` - название логического склада в нашей системе.
- `account` - ЛК WB. В проекте нормализуется к `UPPERCASE`.
- `wb_warehouse_id` - WB `warehouseId` внутри конкретного ЛК.
- `wb_office_id` - WB `officeId`, к которому привязан склад.
- `status` - для управления остатками используются только строки со значением `active`.

Текущие складские соответствия в коде:

```text
warehouse_id=2 -> Вешки
warehouse_id=1 -> Казань
warehouse_id=3 -> Волгоград
warehouse_id=4 -> Шушары
warehouse_id=5 -> Екатеринбург
warehouse_id=6 -> Владивосток
```

## Основные env-переменные

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"                   # ограничить запуск одним ЛК
export WB_FBS_ACCOUNTS="СТАРТ0854,СТАРТ5020"       # список ЛК для создания склада
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/result.json"

export WB_FBS_OFFICE_ID="3091602"                  # WB officeId для создания склада
export WB_FBS_WAREHOUSE_NAME="Наш склад: Казань"
export WB_FBS_WAREHOUSE_ID="2017474"               # WB warehouseId для удаления / импорта существующего склада
export WB_FBS_OUR_WAREHOUSE_ID="1"                 # наш warehouse_id
export WB_FBS_IMPORT_SOURCE_PATH="src_oop/jobs/fbs_warehouses/files/synced_warehouses.json"

export WB_FBS_CREATE_MISSING_COLUMNS=true          # разрешить автоматически добавить служебные колонки в тестовую таблицу
export WB_FBS_AUTO_REFILL_APPLY=true               # разрешить отдельному cron-сценарию автопополнения реально писать в WB
export WB_FBS_AUTO_REFILL_VESHKI_ONLY=true         # автопополнение только на Вешки до значения Минимальный остаток
```

`WB_FBS_WAREHOUSE_ID` - это именно WB `warehouseId`. Для удаления нельзя использовать имя переменной `WAREHOUSE_ID_ENV`: это внутреннее имя константы в коде, а не env-переменная shell.

## Получение данных по складам WB

Есть три основные команды:

- `list_wb_offices` - получает офисы WB, из которых выбирается `officeId` для создания склада.
- `list_fbs_warehouses` - получает уже созданные FBS-склады продавца в ЛК.
- `sync_fbs_warehouses_from_wb` - получает действующие FBS-склады продавца, обновляет известные строки в `warehouses_fbs` и автоматически добавляет в БД склады, которых там еще нет.

### Получить офисы WB для одного ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/wb_offices_СТАРТ0854.json"
python main.py list_wb_offices
```

### Получить офисы WB по всем ЛК

```bash
unset WB_FBS_ACCOUNT
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/wb_offices_all_accounts.json"
python main.py list_wb_offices
```

### Получить текущие FBS-склады продавца для одного ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/fbs_warehouses_СТАРТ0854.json"
python main.py list_fbs_warehouses
```

### Синхронизировать `warehouses_fbs` с действующими складами WB

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/synced_warehouses_СТАРТ0854.json"
python main.py sync_fbs_warehouses_from_wb
```

Что делает `sync_fbs_warehouses_from_wb` сейчас:

- обновляет уже известные активные связки;
- возвращает удаленные раньше строки в `active`, если склад снова есть в WB;
- автоматически добавляет в БД действующие склады, которых еще нет в `warehouses_fbs`;
- если на другом ЛК уже есть склад с тем же `warehouse_name`, использует тот же наш `warehouse_id`;
- если название новое, создает новый `warehouse_id`;
- пропускает склады, которые WB уже пометил как удаляемые.

В JSON-результате проверяйте поля:

```text
updated_rows
inserted_rows
unmatched_warehouses
```

## Создание складов

### Создать склад в одном ЛК

Если склад уже существует в нашей системе на других ЛК, заранее задайте `WB_FBS_OUR_WAREHOUSE_ID`.
Если это новый логический склад, не задавайте эту переменную: система сама возьмет следующий `warehouse_id`.

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OFFICE_ID="3091602"
export WB_FBS_WAREHOUSE_NAME="Наш склад: Казань"
export WB_FBS_OUR_WAREHOUSE_ID="1"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/created_warehouse_СТАРТ0854.json"
python main.py create_fbs_warehouse
```

Команда:

- создает склад на WB;
- сразу пишет его в `warehouses_fbs`;
- пропускает создание, если для этого `account + warehouse_id` уже есть активная строка в БД.

В результате смотрите блок `database_import`.

### Создать один и тот же склад на выбранных ЛК

```bash
export WB_FBS_ACCOUNTS="СТАРТ0854,СТАРТ5020"
export WB_FBS_OFFICE_ID="3091602"
export WB_FBS_WAREHOUSE_NAME="Наш склад: Казань"
export WB_FBS_OUR_WAREHOUSE_ID="1"
python main.py create_fbs_warehouse
```

### Создать склад на всех ЛК

```bash
unset WB_FBS_ACCOUNT
unset WB_FBS_ACCOUNTS
export WB_FBS_OFFICE_ID="3091602"
export WB_FBS_WAREHOUSE_NAME="Наш склад: Казань"
export WB_FBS_OUR_WAREHOUSE_ID="1"
python main.py create_fbs_warehouse
```

Если склад уже существует на части ЛК, эти аккаунты будут пропущены без повторного вызова WB API.

### Ручной импорт из файла

`import_created_fbs_warehouse` и `import_existing_fbs_warehouse` оставлены для ручного восстановления старых случаев, но в обычном сценарии после `create_fbs_warehouse` уже не нужны.

Пример для существующего WB-склада:

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_WAREHOUSE_ID="1748583"
export WB_FBS_IMPORT_SOURCE_PATH="src_oop/jobs/fbs_warehouses/files/synced_warehouses_СТАРТ0854.json"
python main.py import_existing_fbs_warehouse
```

## Удаление склада WB

Удаление использует WB `warehouseId`, а не наш `warehouse_id`.

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_WAREHOUSE_ID="2017474"
python main.py delete_fbs_warehouse
```

После успешного удаления строка в `warehouses_fbs` помечается как `deleted`.

## Обновление текущих остатков в тестовой таблице

### Для одного ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
python main.py update_fbs_stocks_in_unit
```

### Для всех ЛК

```bash
unset WB_FBS_ACCOUNT
python main.py update_fbs_stocks_in_unit
```

Если нужных колонок в тестовой таблице еще нет:

```bash
export WB_FBS_CREATE_MISSING_COLUMNS=true
python main.py update_fbs_stocks_in_unit
```

Команда заполняет колонку:

```text
ФБС общий остаток
```

`ФБС общий остаток` - это сумма остатков по всем активным внутренним FBS-складам аккаунта.

## Ручное применение новых остатков

Пользователь управляет остатками через колонки:

```text
Новый остаток для всех складов
Новый остаток Вешки
```

Правила:

- `Новый остаток для всех складов` - указанное значение ставится на каждый активный внутренний склад.
- `Новый остаток Вешки` - значение ставится только на Вешки, а остальные активные внутренние склады приводятся к `0`.
- Если заполнены обе колонки в одной строке, приоритет получает `Новый остаток Вешки`, а значение из `Новый остаток для всех складов` игнорируется.
- После успешной реальной отправки управляющая ячейка очищается.
- После реальной отправки сценарий ждет, пока WB начнет отдавать новые значения, и затем перечитывает `ФБС общий остаток`.
- Если новых команд в таблице нет, сценарий не отправляет пустые запросы записи в WB и делает только актуализацию `ФБС общий остаток`.

### Отправка новых остатков

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
python main.py apply_new_fbs_stocks_from_unit
```

`apply_new_fbs_stocks_from_unit` теперь запускает только ручной сценарий:

- читает `Новый остаток для всех складов` и `Новый остаток Вешки`;
- если команды найдены, отправляет новые остатки в WB;
- очищает успешно примененные управляющие ячейки;
- затем обновляет `ФБС общий остаток` из WB.

Для регулярного cron-запуска это означает следующее:

- если пользователь заполнил одну из управляющих колонок, сценарий применяет новое значение;
- если обе управляющие колонки пустые, сценарий работает как безопасная актуализация текущего `ФБС общий остаток` без записи в WB.

Режим работы: задача всегда реально пишет подготовленные новые остатки в WB.

## Автопополнение остатков

В тестовой таблице участвуют поля:

```text
ФБС общий остаток
Минимальный остаток
```

И лист:

```text
Сопост -> wild / Добавляем
```

Логика:

- cron читает строки с положительным `Минимальный остаток`;
- `ФБС общий остаток` делится на число активных внутренних складов аккаунта;
- если средний остаток на один склад меньше `Минимальный остаток`, то:
  - в обычном режиме берется значение `Добавляем` по `wild` и ставится на каждый активный внутренний склад;
  - при `WB_FBS_AUTO_REFILL_VESHKI_ONLY=true` на Вешки ставится значение из `Минимальный остаток`, а остальные активные склады по этой строке приводятся к `0`;
- после реальной отправки заново обновляется `ФБС общий остаток`.

### Отдельный dry-run сценарий автопополнения

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
unset WB_FBS_AUTO_REFILL_APPLY
unset WB_FBS_AUTO_REFILL_VESHKI_ONLY
python main.py auto_refill_fbs_stocks_from_unit
```

### Отдельный реальный запуск автопополнения

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_AUTO_REFILL_APPLY=true
unset WB_FBS_AUTO_REFILL_VESHKI_ONLY
python main.py auto_refill_fbs_stocks_from_unit
```

### Режим автопополнения только на Вешки

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_AUTO_REFILL_APPLY=true
export WB_FBS_AUTO_REFILL_VESHKI_ONLY=true
python main.py auto_refill_fbs_stocks_from_unit
```

`auto_refill_fbs_stocks_from_unit` теперь запускается отдельно от ручного сценария.
Это значит, что после `apply_new_fbs_stocks_from_unit` автопополнение по
`Минимальный остаток` больше не стартует автоматически в том же прогоне.

## Безопасный порядок работы

1. Сначала получить или синхронизировать склады WB.
2. Проверить, что в `warehouses_fbs` есть нужные пары `account + warehouse_id + wb_warehouse_id`.
3. Обновить `ФБС общий остаток` через `update_fbs_stocks_in_unit`.
4. Для ручного сценария заполнить `Новый остаток для всех складов` или `Новый остаток Вешки`.
5. Выполнить нужную команду после проверки управляющих значений в UNIT.
6. Для отдельного cron-сценария автопополнения включить `WB_FBS_AUTO_REFILL_APPLY=true` только после проверки результата.


## Мой сценарий добавления одного склада, для одного аккаунта (не трогать)

1. Получаем список складов ВБ, к которым будет осуществляться привязка наших складов.
*Зададим путь для сохранения данных*

```bash
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/synced_warehouses_СТАРТ0854.json"
```
*Запускаем получение данных*

```bash
python main.py list_wb_offices
```
*Получаем структуру вида*

```json
{
  "operation": "list_offices",
  "accounts_total": 1,
  "retries_used": 0,
  "results": [
    {
      "account": "СТАРТ0854",
      "retries_used": 0,
      "payload": [
        {
          "federalDistrict": "Сибирский федеральный округ",
          "address": "РФ, Республика Хакасия, г. Абакан, ул. Складская 11",
          "name": "Абакан-2",
          "city": "Абакан",
          "id": 10236,
          "longitude": 91.37692,
          "latitude": 53.71977,
          "cargoType": 1,
          "deliveryType": 1,
          "selected": false
        },
        {
          "federalDistrict": "Сибирский федеральный округ",
          "address": "Республика Хакасия , г. Абакан, ул. Складская 11а",
          "name": "Абакан КГТ+",
          "city": "Абакан",
          "id": 90054,
          "longitude": 91.37562,
          "latitude": 53.719887,
          "cargoType": 3,
          "deliveryType": 1,
          "selected": false
        }
      ]
    }
  ]
}
```

2. Задаем аккаунт, на котором планируем создать склад
```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
```

3. Выбираем  ID офиса ВБ,  которому привязан наш склад из данных полученных на шаге 1
```bash
export WB_FBS_OFFICE_ID=3090292
```

4.  Задаем имя склада для нашей системы
```bash
export WB_FBS_WAREHOUSE_NAME="Наш склад: Екатеринбург"
```
5. Создаем наш склад на ВБ
```bash
python main.py create_fbs_warehouse
```
Получаем ответ
```json
{
  "operation": "create_warehouse",
  "results": [
    {
      "account": "СТАРТ0854",
      "payload": {
        "id": 2032191
      }
    }
  ]
}
```
6. Для экспорта в БД в таблицу warehouses_fbs задаем добавляем ВБэшный id склада из предыдущего шага, к которому осуществлена привязка в кавычках  
```bash
 export WB_FBS_WAREHOUSE_ID="2032191"
 ```

 7. Выгружаем данные о вновь созданном складе в БД в таблицу warehouses_fbs
```bash
 python main.py import_existing_fbs_warehouse
 ```
 


 
## Мой сценарий добавления одного склада для одного аккаунта
```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OFFICE_ID="3091602"
export WB_FBS_WAREHOUSE_NAME="Наш склад: Казань"
export WB_FBS_OUR_WAREHOUSE_ID="1"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/created_warehouse_СТАРТ0854.json"

python main.py create_fbs_warehouse
Если такой склад уже есть в нашей системе и мы добавляем его для нового ЛК, обязательно задаём существующий id:
Мой склад / Вешки      -> WB_FBS_OUR_WAREHOUSE_ID="2"
Наш склад: Казань      -> WB_FBS_OUR_WAREHOUSE_ID="1"
Наш склад: Волгоград   -> WB_FBS_OUR_WAREHOUSE_ID="3"
Наш склад: Шушары      -> WB_FBS_OUR_WAREHOUSE_ID="4"
Наш склад: Екатеринбург -> WB_FBS_OUR_WAREHOUSE_ID="5"
Наш склад: Владивосток -> WB_FBS_OUR_WAREHOUSE_ID="6"
```
