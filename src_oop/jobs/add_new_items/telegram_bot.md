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

Из корня проекта:

```bash
python main.py add_new_items_telegram_bot
```

## Systemd на сервере

Имя сервиса:

```bash
add-new-items-bot
```

Основные команды обслуживания:

```bash
sudo systemctl status add-new-items-bot
sudo systemctl restart add-new-items-bot
sudo systemctl stop add-new-items-bot
journalctl -u add-new-items-bot -f
```

## Что проверять при проблемах

Если бот не отвечает:

1. Проверить статус сервиса.
2. Проверить live-логи через `journalctl`.
3. Проверить application log проекта:

```bash
/root/start_vector/logs/app.log
```

4. Убедиться, что бот находится в нужной группе и токен актуален.

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
