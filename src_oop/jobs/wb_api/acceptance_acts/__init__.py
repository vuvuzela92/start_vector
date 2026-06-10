"""Модуль актов приёма-передачи Wildberries в OOP-архитектуре.

Пакет уже содержит рабочий streaming/chunked pipeline:
- WB API client;
- распаковку архивов;
- Excel parser;
- validator;
- normalizers для ФБО и ФБС;
- repository для записи в PostgreSQL;
- orchestration service;
- run entrypoints.

Также пакет поддерживает dry-run режим, в котором используются реальные API,
архивы и Excel-файлы, но пропускаются запись в БД и refresh materialized view.

TODO:
- при необходимости отдельно облегчить импорт пакета, если понадобится
  отказаться от реэкспорта entrypoint-функций через `__init__.py`.
"""

from src_oop.jobs.wb_api.acceptance_acts.run import (
    run_all_acceptance_acts,
    run_fbo_acceptance_acts,
    run_fbs_acceptance_acts,
)

__all__ = [
    "run_fbo_acceptance_acts",
    "run_fbs_acceptance_acts",
    "run_all_acceptance_acts",
]
