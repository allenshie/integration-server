"""Rule engine plugin interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Type

from smart_workflow import TaskContext

from integration.pipeline.tasks.plugin_loader import load_plugin_class


@dataclass
class RuleEngineResult:
    """Result returned by rule engines."""

    task_payload: Dict[str, Any] | None = None
    context_updates: Dict[str, Any] | None = None
    events: List[Dict[str, Any]] | None = None


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
        context.logger.debug(f"DefaultRuleEngine processed payload with {total} global objects")
        return RuleEngineResult(task_payload={"global_objects": total})


def load_rule_engine(path: str) -> Type[BaseRuleEngine]:
    return load_plugin_class(path, BaseRuleEngine, "規則 Engine")
