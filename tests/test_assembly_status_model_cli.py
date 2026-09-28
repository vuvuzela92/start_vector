from src_oop.tasks_registry import TASKS


def test_assembly_status_model_run_registered_in_main_cli() -> None:
    """Проверяет, что job доступен под согласованным именем общего CLI."""
    assert "assembly_status_model_run" in TASKS
    assert "assembly_status_mode_run" not in TASKS
