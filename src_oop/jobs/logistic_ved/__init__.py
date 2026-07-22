from src_oop.jobs.fbo_supplies.run import fbo_supplies_run

# Совместимость на переходный период: старый импорт возвращает новую точку входа.
logistic_ved_run = fbo_supplies_run

__all__ = ["fbo_supplies_run", "logistic_ved_run"]
