"""Entrypoint синхронизации сборочных заданий WB."""

import asyncio

from src_oop.jobs.assembly_info.service import AssemblyInfoService


async def assembly_status_model_run() -> None:
    """Запускает загрузку, нормализацию и сохранение сборочных заданий WB."""
    await AssemblyInfoService().run()


def main() -> None:
    """Запускает CLI-сценарий сбора сборочных заданий и статусов WB."""
    asyncio.run(assembly_status_model_run())


if __name__ == "__main__":
    main()
