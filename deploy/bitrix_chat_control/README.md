# Bitrix Chat Control: server deployment

Этот документ описывает нормальный боевой контур для `bitrix_chat_control` на Linux-сервере.

## Что будет работать на сервере

- постоянный `systemd`-процесс для `telegram_bot`;
- таймер `systemd` для регулярного `sync_bitrix_chats`;
- таймер `systemd` для `daily_report`;
- таймер `systemd` для `weekly_report`;
- отдельный env-файл сервиса вне репозитория;
- запуск только через Bitrix REST, без MCP.

## Целевая структура на сервере

```text
/opt/start_vector
/opt/start_vector/.venv
/etc/start-vector/bitrix-chat-control.env
```

## Шаг 1. Подготовить сервер

Пример для Ubuntu:

```bash
sudo timedatectl set-timezone Europe/Moscow
sudo adduser --system --group --home /opt/start_vector vector
sudo mkdir -p /opt/start_vector
sudo mkdir -p /etc/start-vector
sudo chown -R vector:vector /opt/start_vector
sudo chown root:vector /etc/start-vector
sudo chmod 750 /etc/start-vector
```

Почему это важно:

- `telegram_bot`, `sync`, `daily`, `weekly` должны жить под отдельным техпользователем;
- env с секретами должен лежать вне git-репозитория;
- московский часовой пояс упрощает расписание таймеров.

## Шаг 2. Развернуть код проекта

```bash
cd /opt
sudo -u vector git clone <YOUR_REPOSITORY_URL> start_vector
cd /opt/start_vector
sudo -u vector python3.12 -m venv .venv
sudo -u vector .venv/bin/pip install --upgrade pip
sudo -u vector .venv/bin/pip install -r requirements.txt
```

Если проект уже лежит на сервере, достаточно обновить код и зависимости.

## Шаг 3. Подготовить env-файл

Скопируйте шаблон:

```bash
sudo cp deploy/bitrix_chat_control/bitrix-chat-control.env.example /etc/start-vector/bitrix-chat-control.env
sudo chown root:vector /etc/start-vector/bitrix-chat-control.env
sudo chmod 640 /etc/start-vector/bitrix-chat-control.env
```

Далее заполните секреты и параметры:

- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `WB_FBS_TG_BOT_TOKEN`
- `TELEGRAM_ALLOWED_CHAT_IDS`
- `TELEGRAM_SERVICE_CHAT_IDS`
- `OPENAI_API_KEY`
- `BITRIX_RUNTIME_TRANSPORT=rest`
- `BITRIX_REST_PORTAL_URL`
- `BITRIX_REST_AUTH_MODE=webhook`
- `BITRIX_REST_WEBHOOK_USER_ID`
- `BITRIX_REST_WEBHOOK_TOKEN`

Важно:

- на сервере не нужен `MCP`;
- для продакшена используем только Bitrix REST;
- секреты не храним в `.env` внутри репозитория.

## Шаг 4. Один раз инициализировать БД

```bash
cd /opt/start_vector
sudo -u vector env $(grep -v '^#' /etc/start-vector/bitrix-chat-control.env | xargs) \
    .venv/bin/python main.py bitrix_chat_control_create_tables
```

Команда:

- создаст схему `bitrix_chat_control`, если её ещё нет;
- перенесёт legacy-таблицы модуля из `public`, если они уже существуют;
- подтянет monitored chats.

## Шаг 5. Установить systemd units

Скопируйте unit-файлы:

```bash
sudo cp deploy/bitrix_chat_control/systemd/*.service /etc/systemd/system/
sudo cp deploy/bitrix_chat_control/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

По умолчанию unit-файлы ожидают:

- проект в `/opt/start_vector`
- виртуальное окружение в `/opt/start_vector/.venv`
- env-файл в `/etc/start-vector/bitrix-chat-control.env`
- пользователя `vector`

Если ваши пути отличаются, сначала поправьте unit-файлы.

## Шаг 6. Включить процессы и таймеры

```bash
sudo systemctl enable --now start-vector-bitrix-chat-control-bot.service
sudo systemctl enable --now start-vector-bitrix-chat-control-sync.timer
sudo systemctl enable --now start-vector-bitrix-chat-control-daily-report.timer
sudo systemctl enable --now start-vector-bitrix-chat-control-weekly-report.timer
```

## Шаг 7. Проверить, что всё живо

Проверка статуса:

```bash
sudo systemctl status start-vector-bitrix-chat-control-bot.service
sudo systemctl status start-vector-bitrix-chat-control-sync.timer
sudo systemctl status start-vector-bitrix-chat-control-daily-report.timer
sudo systemctl status start-vector-bitrix-chat-control-weekly-report.timer
```

Проверка журналов:

```bash
sudo journalctl -u start-vector-bitrix-chat-control-bot.service -f
sudo journalctl -u start-vector-bitrix-chat-control-sync.service -n 100
sudo journalctl -u start-vector-bitrix-chat-control-daily-report.service -n 100
sudo journalctl -u start-vector-bitrix-chat-control-weekly-report.service -n 100
```

Проверка ближайших запусков таймеров:

```bash
systemctl list-timers --all | grep start-vector-bitrix-chat-control
```

## Что запускается по расписанию

- `sync` каждые 5 минут;
- `daily_report` каждый день в `09:05` по московскому времени;
- `weekly_report` каждый понедельник в `09:15` по московскому времени.

Если нужен другой интервал, меняйте соответствующие `.timer` unit-файлы.

## Ручные команды для эксплуатации

Запустить sync вручную:

```bash
sudo systemctl start start-vector-bitrix-chat-control-sync.service
```

Перезапустить Telegram-бота:

```bash
sudo systemctl restart start-vector-bitrix-chat-control-bot.service
```

Остановить Telegram-бота:

```bash
sudo systemctl stop start-vector-bitrix-chat-control-bot.service
```

## Обновление после новой версии кода

```bash
cd /opt/start_vector
sudo -u vector git pull
sudo -u vector .venv/bin/pip install -r requirements.txt
sudo systemctl restart start-vector-bitrix-chat-control-bot.service
sudo systemctl start start-vector-bitrix-chat-control-sync.service
```

Если менялись unit-файлы:

```bash
sudo cp deploy/bitrix_chat_control/systemd/*.service /etc/systemd/system/
sudo cp deploy/bitrix_chat_control/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart start-vector-bitrix-chat-control-bot.service
sudo systemctl restart start-vector-bitrix-chat-control-sync.timer
sudo systemctl restart start-vector-bitrix-chat-control-daily-report.timer
sudo systemctl restart start-vector-bitrix-chat-control-weekly-report.timer
```

## Принципиальное правило

На сервере не используем `MCP` для боевого рантайма.

- `MCP` остаётся только для интерактивной проверки внутри Codex;
- production-контур работает через `Bitrix REST + Telegram token + env`.
