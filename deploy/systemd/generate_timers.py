"""Создаёт пользовательские таймеры systemd из расписания Windows."""

import argparse
import json
import re
from pathlib import Path


def calendar_expression(schedule: str, timezone: str) -> str:
    """Переводит время выгрузки в OnCalendar, сохраняя московский часовой пояс."""
    interval = re.fullmatch(r"every (\d+) minutes", schedule)
    if interval:
        minutes = int(interval.group(1))
        if minutes < 1 or 60 % minutes:
            raise ValueError(f"Неподдерживаемый интервал минут: {schedule}")
        return f"*-*-* *:0/{minutes}:00 {timezone}"

    interval = re.fullmatch(r"every (\d+) hours, starting (\d{2}):00", schedule)
    if interval:
        hours, start = map(int, interval.groups())
        if hours < 1 or 24 % hours or start >= 24:
            raise ValueError(f"Неподдерживаемый интервал часов: {schedule}")
        return f"*-*-* {start}/{hours}:00:00 {timezone}"

    hourly = re.fullmatch(r"hourly at minute (.+)", schedule)
    if hourly:
        minutes = re.split(r", | and ", hourly.group(1))
        if any(not minute.isdigit() or int(minute) >= 60 for minute in minutes):
            raise ValueError(f"Неверные минуты: {schedule}")
        return f"*-*-* *:{','.join(minutes)}:00 {timezone}"

    daily = re.fullmatch(r"daily at (.+)", schedule)
    if daily:
        times = re.split(r" and ", daily.group(1))
        if any(not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value) for value in times):
            raise ValueError(f"Неверное ежедневное время: {schedule}")
        return "\n".join(f"OnCalendar=*-*-* {value}:00 {timezone}" for value in times)

    raise ValueError(f"Неизвестный формат расписания: {schedule}")


def main() -> None:
    """Готовит таймеры для переноса выгрузок без изменения работающего расписания."""
    parser = argparse.ArgumentParser(description="Подготовить таймеры Start Vector")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--project-dir", default="/home/vuvuzela92/start_vector")
    parser.add_argument("--skip", action="append", default=[])
    args = parser.parse_args()
    if not args.project_dir.startswith("/") or "\\" in args.project_dir:
        raise ValueError("Каталог проекта должен быть абсолютным путём Linux")

    schedule_path = Path(__file__).resolve().parents[1] / "windows_task_scheduler" / "schedule.json"
    config = json.loads(schedule_path.read_text(encoding="utf-8"))
    timezone = config["timezone"]
    tasks = config["tasks"]
    names = [task["name"] for task in tasks]
    if len(names) != len(set(names)) or any(not re.fullmatch(r"[a-z0-9_]+", name) for name in names):
        raise ValueError("Имена задач должны быть уникальны и безопасны для systemd")
    unknown_skips = set(args.skip) - set(names)
    if unknown_skips:
        raise ValueError(f"Неизвестные исключения: {sorted(unknown_skips)}")

    # Проверяем все выражения до записи файлов, чтобы не оставить неполный набор.
    calendars = {task["name"]: calendar_expression(task["schedule"], timezone) for task in tasks}
    args.output.mkdir(parents=True, exist_ok=True)
    service = (
        "[Unit]\nDescription=Start Vector: %i\n\n"
        "[Service]\nType=oneshot\n"
        f"WorkingDirectory={args.project_dir}\n"
        f"ExecStart={args.project_dir}/.venv/bin/python {args.project_dir}/main.py %i\n"
        "TimeoutStartSec=1h\nKillMode=control-group\n"
    )
    (args.output / "start-vector@.service").write_text(service, encoding="utf-8")
    for name, expression in calendars.items():
        if name in args.skip:
            continue
        calendar_lines = expression if expression.startswith("OnCalendar=") else f"OnCalendar={expression}"
        timer = (
            f"[Unit]\nDescription=Расписание Start Vector: {name}\n\n"
            f"[Timer]\n{calendar_lines}\nAccuracySec=1s\nPersistent=false\n"
            f"Unit=start-vector@{name}.service\n\n[Install]\nWantedBy=timers.target\n"
        )
        (args.output / f"start-vector-{name}.timer").write_text(timer, encoding="utf-8")
    print(f"Подготовлено таймеров: {len(tasks) - len(args.skip)}")


if __name__ == "__main__":
    main()
