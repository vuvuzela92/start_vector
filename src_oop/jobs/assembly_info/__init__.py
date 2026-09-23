"""Синхронизация сборочных заданий и их статусов из Wildberries."""

from src_oop.jobs.assembly_info.run import assembly_status_model_run
from src_oop.jobs.assembly_info.service import AssemblyInfoService

__all__ = ["AssemblyInfoService", "assembly_status_model_run"]
