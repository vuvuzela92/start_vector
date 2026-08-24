# Ограниченный запуск задач Start Vector из Hermes

Этот контур разрешает Hermes запускать только утверждённые имена задач из
`src_oop/tasks_registry.py`. Он не предоставляет интерактивную оболочку, `sudo`,
произвольные команды или доступ к секретам в ответе SSH.

## 1. Подготовить технического пользователя

На сервере Start Vector выполните от администратора:

```bash
sudo adduser --system --group --home /nonexistent --shell /usr/sbin/nologin hermes_runner
sudo install -d -o hermes_runner -g hermes_runner -m 750 /var/log/start-vector/hermes-jobs
sudo install -d -o hermes_runner -g hermes_runner -m 700 /var/lib/hermes_runner/.ssh
```

У пользователя не должно быть `sudo`, доступа на запись к репозиторию, виртуальному
окружению, launcher-скрипту или конфигурации запуска.

Launcher должен иметь возможность читать код и запускать виртуальное окружение. Если
`/opt/start_vector` закрыт для остальных пользователей, выдайте `hermes_runner` только
права чтения и прохода по каталогам через ACL, не добавляя его в группу с правом записи:

```bash
sudo setfacl -R -m u:hermes_runner:rX /opt/start_vector
```

## 2. Установить launcher и конфигурацию

```bash
cd /opt/start_vector
sudo install -o root -g root -m 755 deploy/hermes_task_runner/hermes_task_runner.py /usr/local/bin/run-start-vector-task
sudo install -d -o root -g vector -m 750 /etc/start-vector
sudo install -o root -g root -m 640 deploy/hermes_task_runner/hermes-runner.conf.example /etc/start-vector/hermes-runner.conf
sudo install -o root -g root -m 640 deploy/hermes_task_runner/hermes-allowed-tasks.txt.example /etc/start-vector/hermes-allowed-tasks.txt
sudo setfacl -m u:hermes_runner:--x /etc/start-vector
sudo setfacl -m u:hermes_runner:r-- /etc/start-vector/hermes-runner.conf
sudo setfacl -m u:hermes_runner:r-- /etc/start-vector/hermes-allowed-tasks.txt
```

Заполните `/etc/start-vector/hermes-runner.conf` реальными путями. Файл с секретами
`ENV_FILE` должен быть вне репозитория, не доступен всем пользователям и доступен на
чтение только `hermes_runner` через ACL без права записи. Не размещайте его в Git и не
передавайте его содержимое Hermes. Для нового env-файла используйте:

```bash
sudo install -o root -g root -m 640 /dev/null /etc/start-vector/<job>.env
sudo setfacl -m u:hermes_runner:r-- /etc/start-vector/<job>.env
```

Заполните `/etc/start-vector/hermes-allowed-tasks.txt` только предварительно
проверенными задачами. Не включайте `delete_fbs_warehouse`, `create_fbs_warehouse`,
`apply_new_fbs_stocks_from_unit` и другие действия с необратимым эффектом, пока не
будет отдельного механизма подтверждения.

## 3. Ограничить SSH-ключ Hermes

Поместите публичный ключ сервера Hermes в
`/var/lib/hermes_runner/.ssh/authorized_keys` одной строкой:

```text
restrict,command="/usr/local/bin/run-start-vector-task" ssh-ed25519 AAAA... hermes-start-vector
```

Права на файл:

```bash
sudo chown -R hermes_runner:hermes_runner /var/lib/hermes_runner/.ssh
sudo chmod 700 /var/lib/hermes_runner/.ssh
sudo chmod 600 /var/lib/hermes_runner/.ssh/authorized_keys
```

Опция `restrict` отключает интерактивный терминал и SSH-форвардинги. Убедитесь, что в
`sshd_config` для пользователя не разрешён парольный вход. Если применяете `Match User`,
зафиксируйте `PasswordAuthentication no` и `PermitTTY no`.

## 4. Формат вызова с сервера Hermes

Единственная разрешённая команда:

```bash
ssh -i /secure/path/hermes_start_vector_ed25519 hermes_runner@START_VECTOR_HOST "run order_feed"
```

Ответ содержит только статус, `job_id` и код завершения. Полный stdout/stderr задачи
остаётся в `/var/log/start-vector/hermes-jobs/` с правами `0600`.

## 5. Проверка до выдачи доступа Hermes

Администратор Star Vector должен проверить:

```bash
sudo -u hermes_runner SSH_ORIGINAL_COMMAND='run order_feed' /usr/local/bin/run-start-vector-task
sudo -u hermes_runner SSH_ORIGINAL_COMMAND='run order_feed; id' /usr/local/bin/run-start-vector-task
```

Первый вызов допустим только если `order_feed` добавлен в allowlist; второй обязан быть
отклонён. Проверяйте запуск сначала на безопасной read-only или тестовой задаче.
