"""Показывает каталог задач проекта и состояние пользовательских таймеров."""

import ast
import json
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def read_descriptions() -> dict[str, str]:
    """Читает бизнес-описания задач из реестра без запуска его импортов и API-клиентов."""
    source = (PROJECT_ROOT / "src_oop" / "tasks_registry.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in tree.body:
        if not isinstance(node, ast.AnnAssign) or not isinstance(node.target, ast.Name):
            continue
        if node.target.id != "TASKS" or not isinstance(node.value, ast.Dict):
            continue
        descriptions = {}
        for key, value in zip(node.value.keys, node.value.values, strict=True):
            if not isinstance(key, ast.Constant) or not isinstance(value, ast.Dict):
                continue
            for field, text in zip(value.keys, value.values, strict=True):
                if isinstance(field, ast.Constant) and field.value == "desc":
                    descriptions[str(key.value)] = ast.literal_eval(text)
        return descriptions
    raise ValueError("В src_oop/tasks_registry.py не найден реестр TASKS")


def read_timer_states() -> dict[str, str]:
    """Читает состояния таймеров, чтобы отличать перенесённые выгрузки от подготовленных."""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "list-unit-files", "--type=timer", "--no-legend", "--no-pager"],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return {}
    states = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 2:
            states[fields[0]] = fields[1]
    return states


def main() -> None:
    """Выводит все запланированные выгрузки с описанием и состоянием переноса на Linux."""
    config = json.loads(
        (PROJECT_ROOT / "deploy" / "windows_task_scheduler" / "schedule.json").read_text(
            encoding="utf-8"
        )
    )
    descriptions = read_descriptions()
    states = read_timer_states()
    print(f"Задач в расписании: {len(config['tasks'])}. Часовой пояс: {config['timezone']}\n")
    for task in config["tasks"]:
        name = task["name"]
        timer_name = (
            "start-vector-funnel-sales.timer"
            if name == "funnel_sales"
            else f"start-vector-{name}.timer"
        )
        state = states.get(timer_name, "нет таймера")
        print(f"{name} | {state} | {task['schedule']}")
        print(f"  {descriptions.get(name, 'Нет описания в реестре')}")
    extra = set(descriptions) - {task["name"] for task in config["tasks"]}
    if extra:
        print(f"\nВ реестре без расписания: {', '.join(sorted(extra))}")


if __name__ == "__main__":
    main()
