"""Phase change hooks."""
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Type

from smart_workflow import TaskContext, TaskError

class BasePhaseChangeEngine(ABC):
    """Handle side effects when phase changes."""

    def __init__(self, context: TaskContext | None = None) -> None:
        self._context = context

    @abstractmethod
    def on_phase_change(self, old_phase: str | None, new_phase: str, context: TaskContext) -> None:
        """Run when phase changes."""


class DefaultPhaseChangeEngine(BasePhaseChangeEngine):
    """Default no-op handler that logs the phase change."""

    def on_phase_change(self, old_phase: str | None, new_phase: str, context: TaskContext) -> None:
        context.logger.info(f"phase changed: {old_phase} -> {new_phase}")


def load_phase_change_engine(path: str) -> Type[BasePhaseChangeEngine]:
    if ":" in path:
        module_name, class_name = path.split(":", 1)
    elif "." in path:
        module_name, class_name = path.rsplit(".", 1)
    else:
        raise TaskError(f"無法解析 PhaseChange Engine 路徑：{path}")

    module = import_module(module_name)
    attr = getattr(module, class_name, None)
    if attr is None or not inspect.isclass(attr):
        raise TaskError(f"在模組 {module_name} 找不到 PhaseChange Engine {class_name}")
    if not issubclass(attr, BasePhaseChangeEngine):
        raise TaskError(f"{class_name} 必須繼承 BasePhaseChangeEngine")
    return attr
