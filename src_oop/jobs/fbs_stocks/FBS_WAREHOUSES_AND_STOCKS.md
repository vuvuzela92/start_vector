# Управление FBS-складами и остатками WB

Документ описывает ручные команды для контура FBS-складов и остатков.

Важно: остатки читаются и обновляются только через тестовую таблицу Google Sheets:

```text
UNIT 2.0 (tested) управление остатками -> MAIN (tested)
```

## Основные сущности

`warehouses_fbs` в PostgreSQL хранит связь наших складов и складов WB:

- `warehouse_id` - наш общий идентификатор склада.
- `warehouse_name` - наше название склада.
- `account` - ЛК WB.
- `wb_warehouse_id` - ID склада WB внутри конкретного ЛК.
- `wb_office_id` - ID офиса WB, к которому привязан склад.

Текущие складские соответствия в коде:

```text
warehouse_id=2 -> Вешки
warehouse_id=1 -> Казань
warehouse_id=3 -> Волгоград
warehouse_id=4 -> Шушары
warehouse_id=5 -> Екатеринбург
warehouse_id=6 -> Владивосток
```

В сценариях остатков участвуют только активные строки `warehouses_fbs`, где `status = 'active'`.

## Env-переменные

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"              # один ЛК; если не задано, команды чтения работают по всем доступным данным
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/wb_warehouses.json"

export WB_FBS_OFFICE_ID="3091602"             # officeId WB для создания склада
export WB_FBS_WAREHOUSE_NAME="Наш склад: Казань"
export WB_FBS_WAREHOUSE_ID="2017474"          # WB warehouseId для удаления/импорта существующего склада
export WB_FBS_OUR_WAREHOUSE_ID="1"            # наш warehouse_id из warehouses_fbs
export WB_FBS_IMPORT_SOURCE_PATH="src_oop/jobs/fbs_warehouses/files/created_warehouse.json"

export WB_FBS_CREATE_MISSING_COLUMNS=true     # разрешить добавить недостающие колонки в тестовую UNIT-таблицу
export WB_FBS_APPLY_STOCKS=true               # разрешить реальную отправку новых остатков в WB
```

## Создание складов

## Получение данных по складам WB в файлы

Есть два разных списка:

- `list_wb_offices` - склады/офисы WB, из которых выбирается `officeId` для создания нашего FBS-склада.
- `list_fbs_warehouses` - уже созданные FBS-склады продавца в конкретном ЛК.
- `sync_fbs_warehouses_from_wb` - получает FBS-склады продавца и дополнительно дозаполняет уже известные строки `warehouses_fbs`.

### Все офисы WB для выбора `officeId` по одному ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/wb_offices_СТАРТ0854.json"
python main.py list_wb_offices
```

Результат сохранится в файл из `WB_FBS_OUTPUT_PATH`.

### Все офисы WB для выбора `officeId` по всем ЛК

Если `WB_FBS_ACCOUNT` не задан, команда пройдет по всем токенам из `tokens.json`.

```bash
unset WB_FBS_ACCOUNT
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/wb_offices_all_accounts.json"
python main.py list_wb_offices
```

### Все созданные FBS-склады продавца по одному ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/fbs_warehouses_СТАРТ0854.json"
python main.py list_fbs_warehouses
```

### Все созданные FBS-склады продавца по всем ЛК

```bash
unset WB_FBS_ACCOUNT
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/fbs_warehouses_all_accounts.json"
python main.py list_fbs_warehouses
```

### Получить FBS-склады и сверить их со справочником `warehouses_fbs`

Для одного ЛК:

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/synced_warehouses_СТАРТ0854.json"
python main.py sync_fbs_warehouses_from_wb
```

Для всех ЛК:

```bash
unset WB_FBS_ACCOUNT
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/synced_warehouses_all_accounts.json"
python main.py sync_fbs_warehouses_from_wb
```

В результате:

- уже привязанные склады обновятся в `warehouses_fbs`;
- новые или неизвестные склады попадут в `unmatched_warehouses`;
- полный JSON сохранится в файл из `WB_FBS_OUTPUT_PATH`.

### 1. Получить офисы WB для одного ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/wb_offices_СТАРТ0854.json"
python main.py list_wb_offices
```

Из результата выбрать `officeId`.

### 2. Создать склад WB в одном ЛК

Если создается склад, который уже есть в нашей системе на других ЛК, заранее задайте общий
`WB_FBS_OUR_WAREHOUSE_ID`. Если это новый логический склад, не задавайте эту переменную:
система возьмет следующий свободный `warehouse_id`.

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OFFICE_ID="3091602"
export WB_FBS_WAREHOUSE_NAME="Наш склад: Казань"
export WB_FBS_OUR_WAREHOUSE_ID="1"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/created_warehouse_СТАРТ0854.json"
python main.py create_fbs_warehouse
```

Команда создает склад на WB и сразу записывает его в `warehouses_fbs`.
В выводе проверяйте блок `database_import`:

```json
{
  "database_import": {
    "warehouse_id": 1,
    "warehouse_name": "Наш склад: Казань",
    "written_rows": 1
  }
}
```

### 3. Записать созданный склад в `warehouses_fbs` из файла вручную

Обычно этот шаг больше не нужен. Используйте его только для старого файла создания или ручного
восстановления записи, если склад уже создан на WB, но не попал в БД.

### 4. Создать один и тот же склад на всех ЛК

Сначала убедитесь, что выбранный `officeId` подходит для всех ЛК. Затем можно запускать цикл.

```bash
export WB_FBS_OFFICE_ID="3091602"
export WB_FBS_WAREHOUSE_NAME="Наш склад: Казань"
export WB_FBS_OUR_WAREHOUSE_ID="1"

for account in $(python -c "from src_oop.core.utils_general import load_api_tokens; print('\n'.join(load_api_tokens().keys()))"); do
  export WB_FBS_ACCOUNT="$account"
  export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/created_warehouse_${account}.json"
  python main.py create_fbs_warehouse
done
```

## Существующие склады WB

### Получить список складов одного ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_OUTPUT_PATH="src_oop/jobs/fbs_warehouses/files/synced_warehouses_СТАРТ0854.json"
python main.py sync_fbs_warehouses_from_wb
```

Команда обновляет только уже известные строки `warehouses_fbs`.
Непривязанные склады попадут в `unmatched_warehouses`.

### Добавить существующий WB-склад как новый наш склад

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_WAREHOUSE_ID="1748583"
export WB_FBS_IMPORT_SOURCE_PATH="src_oop/jobs/fbs_warehouses/files/synced_warehouses_СТАРТ0854.json"
python main.py import_existing_fbs_warehouse
```

Если существующий WB-склад относится к уже созданному нашему складу, задайте `WB_FBS_OUR_WAREHOUSE_ID`.

## Удаление склада WB

Удаление использует WB `warehouseId`, а не наш `warehouse_id`.

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_WAREHOUSE_ID="2017474"
python main.py delete_fbs_warehouse
```

## Текущие остатки из WB в Google Sheets

### Один ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
python main.py update_fbs_stocks_in_unit
```

### Все ЛК

```bash
unset WB_FBS_ACCOUNT
python main.py update_fbs_stocks_in_unit
```

Если в тестовой таблице нет колонок складского блока, явно разрешите их создание:

```bash
export WB_FBS_CREATE_MISSING_COLUMNS=true
python main.py update_fbs_stocks_in_unit
```

Команда заполняет:

```text
ФБС общий остаток
```

`ФБС общий остаток` - это сумма остатков по всем активным внутренним FBS-складам аккаунта.

## Отправка новых остатков в WB

Новые значения пользователь заполняет в тестовой таблице:

```text
Новый остаток для всех складов
Новый остаток Вешки
```

Пустые ячейки не отправляются. Значение должно быть целым неотрицательным числом.

Правила применения:

- `Новый остаток для всех складов` - указанное значение устанавливается на каждый активный внутренний склад.
- `Новый остаток Вешки` - указанное значение устанавливается на склад Вешки, а остальные активные внутренние склады приводятся к `0`.
- Если в одной строке заполнены оба поля, команда останавливается с ошибкой: нужно оставить значение только в одной управляющей колонке.
- После успешной реальной отправки управляющая ячейка очищается, а `ФБС общий остаток` перечитывается из WB.

### Dry-run для одного ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
unset WB_FBS_APPLY_STOCKS
python main.py apply_new_fbs_stocks_from_unit
```

### Реальная отправка для одного ЛК

```bash
export WB_FBS_ACCOUNT="СТАРТ0854"
export WB_FBS_APPLY_STOCKS=true
python main.py apply_new_fbs_stocks_from_unit
```

### Dry-run по всем ЛК

```bash
unset WB_FBS_ACCOUNT
unset WB_FBS_APPLY_STOCKS
python main.py apply_new_fbs_stocks_from_unit
```

### Реальная отправка по всем ЛК

```bash
unset WB_FBS_ACCOUNT
export WB_FBS_APPLY_STOCKS=true
python main.py apply_new_fbs_stocks_from_unit
```

## Безопасный порядок работы

1. Создать или синхронизировать склады WB.
2. Проверить, что `warehouses_fbs` содержит нужные пары `account + warehouse_id`.
3. Обновить текущие остатки в тестовой таблице: `update_fbs_stocks_in_unit`.
4. Заполнить `Новый остаток для всех складов` или `Новый остаток Вешки`.
5. Запустить dry-run: `apply_new_fbs_stocks_from_unit`.
6. Если план корректный, установить `WB_FBS_APPLY_STOCKS=true` и повторить запуск.


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