from pathlib import Path


def test_assembly_status_model_run_module_has_cli_entrypoint() -> None:
    """Проверяет наличие отдельного CLI-режима для запуска job через python -m."""
    run_file = Path("src_oop/jobs/assembly_info/run.py")
    source = run_file.read_text(encoding="utf-8")
    assert "if __name__ == \"__main__\":" in source
    assert "asyncio.run(assembly_status_model_run())" in source
