# Telegram-бот add_new_items

Этот бот является частью job `add_new_items` и нужен для запуска сценария
добавления новых товаров из рабочей Telegram-группы.

## Что делает бот

- принимает команду на запуск `add_new_items_run`;
- показывает текущий статус выполнения;
- показывает итог последнего запуска;
- после завершения процесса присылает отдельное итоговое сообщение в чат.

## Где находится код

- Telegram-entrypoint: [run.py](C:/Users/123/Desktop/start_vector/src_oop/jobs/add_new_items/run.py)
- Telegram-router: [telegram_router.py](C:/Users/123/Desktop/start_vector/src_oop/jobs/add_new_items/telegram_router.py)
- Telegram-service: [telegram_service.py](C:/Users/123/Desktop/start_vector/src_oop/jobs/add_new_items/telegram_service.py)
- Telegram-config: [telegram_config.py](C:/Users/123/Desktop/start_vector/src_oop/jobs/add_new_items/telegram_config.py)

## Переменные окружения

Бот использует следующие env-переменные:

- `ADD_NEW_ITEMS_TELEGRAM_BOT_TOKEN`
- `ADD_NEW_ITEMS_TELEGRAM_ALLOWED_CHAT_IDS`
- `ADD_NEW_ITEMS_TELEGRAM_ALLOWED_USER_IDS`
- `ADD_NEW_ITEMS_TELEGRAM_SUBPROCESS_TIMEOUT_SECONDS`
- `ADD_NEW_ITEMS_TELEGRAM_LOG_TAIL_LINES`
- `ADD_NEW_ITEMS_TELEGRAM_LOG_MAX_LENGTH`

Если `ADD_NEW_ITEMS_TELEGRAM_ALLOWED_USER_IDS` не задана, доступ разрешается
всем сообщениям из разрешенной группы.

## Команды пользователя

Slash-команды:

- `/start`
- `/help`
- `/run_add_new_items`
- `/status`
- `/last_result`

Экранные кнопки:

- `Запустить добавление`
- `Показать статус`
- `Последний результат`
- `Справка`

## Поведение в чате

1. Пользователь запускает добавление через кнопку или команду.
2. Бот сразу пишет, что процесс принят в работу.
3. Бот выполняет `python main.py add_new_items_run` в отдельном процессе.
4. После завершения бот присылает отдельное итоговое сообщение.

Важно:

- завершение без системной ошибки не гарантирует, что каждая строка попала во
  все целевые места;
- после любого запуска нужно проверять колонки:
  - `Добавлено в MAIN (tested)`
  - `Добавлено в Автопилот`
  - `Добавлено в products`

## Локальный запуск

Локальный запуск из корня проекта:

```bash
# Запускает самого Telegram-бота, который потом постоянно слушает новые команды.
# Эту команду используют для ручной проверки на локальной машине.
python main.py add_new_items_telegram_bot
```

## Systemd на сервере

Имя systemd-сервиса:

```bash
add-new-items-bot
```

Основные команды обслуживания:

```bash
# Показывает, запущен ли бот сейчас, какой у него статус и не падал ли он после старта.
sudo systemctl status add-new-items-bot

# Перезапускает бота после обновления кода, переменных окружения или service-файла.
sudo systemctl restart add-new-items-bot

# Полностью останавливает бота. После этой команды он перестанет отвечать в Telegram.
sudo systemctl stop add-new-items-bot

# Включает сервис в автозапуск после перезагрузки сервера.
sudo systemctl enable add-new-items-bot

# Применяет изменения в service-файле, если редактировался `/etc/systemd/system/add-new-items-bot.service`.
sudo systemctl daemon-reload

# Показывает live-логи systemd-сервиса в реальном времени.
# Удобно использовать сразу после перезапуска, чтобы убедиться, что бот дошёл до `Start polling`.
journalctl -u add-new-items-bot -f
```

## Быстрая шпаргалка по status

Проверка состояния сервиса:

```bash
# Основная команда для быстрой проверки: запущен ли бот и не упал ли он.
sudo systemctl status add-new-items-bot
```

Как читать результат:

- `Loaded: loaded`  
  service-файл найден и корректно читается системой.

- `enabled`  
  сервис включен в автозапуск после перезагрузки сервера.

- `Active: active (running)`  
  бот сейчас работает. Это главный признак, что все в порядке.

- `Main PID: ... (python)`  
  у сервиса есть живой основной процесс Python.

- `/root/start_vector/.venv/bin/python /root/start_vector/main.py add_new_items_telegram_bot`  
  это фактическая команда запуска. По ней можно быстро проверить, что service
  использует правильный Python и правильный task.

Что означает проблема:

- `Active: failed`  
  бот упал и сейчас не работает.

- `Active: inactive`  
  бот остановлен.

- `status=203/EXEC`  
  обычно неверный путь в `ExecStart`.

- постоянные перезапуски в журнале  
  сервис стартует, падает и `systemd` пытается поднять его снова.

Если видите проблему:

```bash
# Сначала смотрим общий статус.
sudo systemctl status add-new-items-bot
```

```bash
# Затем открываем live-лог сервиса и смотрим, на чем именно он падает.
journalctl -u add-new-items-bot -f
```

## Что проверять при проблемах

Если бот не отвечает:

1. Проверить статус сервиса:

```bash
# Если здесь не `active (running)`, нужно смотреть причину падения ниже по журналу.
sudo systemctl status add-new-items-bot
```

2. Проверить live-логи сервиса:

```bash
# Показывает новые строки сразу по мере появления.
# Это основной способ понять, дошла ли команда до бота и начал ли он polling.
journalctl -u add-new-items-bot -f
```

3. Посмотреть последние строки application log проекта:

```bash
# Показывает последние 100 строк внутреннего лога проекта.
# Удобно, если нужно быстро увидеть итог последнего запуска add_new_items.
tail -n 100 /root/start_vector/logs/app.log
```

```bash
# Показывает внутренний лог проекта в реальном времени.
# Полезно, если add_new_items уже запущен и нужно следить за прогрессом до завершения.
tail -f /root/start_vector/logs/app.log
```

4. Проверить, что сервис действительно использует нужную команду запуска:

```bash
# Печатает текущий service-файл, чтобы убедиться в правильных путях к проекту и Python.
sudo cat /etc/systemd/system/add-new-items-bot.service
```

5. Если сервис не поднимается, проверить ручной запуск той же командой:

```bash
# Запускает бота вручную тем же Python, что используется в service-файле.
# Если здесь ошибка, значит проблема не в systemd, а в окружении или коде.
/root/start_vector/.venv/bin/python /root/start_vector/main.py add_new_items_telegram_bot
```

6. Убедиться, что бот находится в нужной группе, токен актуален, а команда отправляется именно этому боту.

## Бизнес-правило по завершению

О завершении процесса пользователь должен судить не по консоли сервера, а по
отдельному итоговому сообщению в Telegram.

Если процесс завершился:

- успешно;
- с ошибкой;
- по таймауту;
- частично;

бот должен отразить это в понятном сообщении и напомнить про проверку колонок
`Добавлено ...` в таблице-источнике.
