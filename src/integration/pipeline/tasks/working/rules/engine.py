"""Rule engine plugin interface."""
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, Type

from smart_workflow import TaskContext, TaskError


@dataclass
class RuleEngineResult:
    """Result returned by rule engines."""

    task_payload: Dict[str, Any] | None = None
    context_updates: Dict[str, Any] | None = None


class BaseRuleEngine(ABC):
    """Base interface for project-specific rule logic."""

    def __init__(self, context: TaskContext | None = None) -> None:
        self._context = context

    @abstractmethod
    def process(self, context: TaskContext, payload: Dict[str, Any] | None) -> RuleEngineResult:
        """Evaluate rules based on payload and return result."""


class DefaultRuleEngine(BaseRuleEngine):
    """Fallback engine that simply returns global object count."""

    def process(self, context: TaskContext, payload: Dict[str, Any] | None) -> RuleEngineResult:
        summary = (payload or {}).get("global_summary") or {}
        total = summary.get("total", 0)
        context.logger.debug("DefaultRuleEngine processed payload with %d global objects", total)
        return RuleEngineResult(task_payload={"global_objects": total})


def load_rule_engine(path: str) -> Type[BaseRuleEngine]:
    if ":" in path:
        module_name, class_name = path.split(":", 1)
    elif "." in path:
        module_name, class_name = path.rsplit(".", 1)
    else:
        raise TaskError(f"無法解析規則 Engine 路徑：{path}")

    module = import_module(module_name)
    attr = getattr(module, class_name, None)
    if attr is None or not inspect.isclass(attr):
        raise TaskError(f"在模組 {module_name} 找不到規則 Engine {class_name}")
    if not issubclass(attr, BaseRuleEngine):
        raise TaskError(f"{class_name} 必須繼承 BaseRuleEngine")
    return attr
