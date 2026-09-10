"""Event dispatch engine interface."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Type

from smart_workflow import TaskContext

from integration.pipeline.tasks.plugin_loader import load_plugin_class


@dataclass
class EventDispatchResult:
    dispatched: int = 0
    skipped: int = 0
    failed: int = 0
    details: Dict[str, Any] | None = None


class BaseEventDispatchEngine(ABC):
    """Base interface for event dispatch engines."""

    def __init__(self, context: TaskContext | None = None) -> None:
        self._context = context

    @abstractmethod
    def dispatch(self, events: List[Dict[str, Any]], context: TaskContext) -> EventDispatchResult:
        """Dispatch events and return summary."""


class DefaultEventDispatchEngine(BaseEventDispatchEngine):
    """Fallback engine that only logs counts."""

    def dispatch(self, events: List[Dict[str, Any]], context: TaskContext) -> EventDispatchResult:
        count = len(events)
        for event in events:
            context.logger.debug(
                f"event dispatch: id={event.get('id')} name={event.get('name')} "
                f"timestamp={event.get('timestamp')} event_type={event.get('event_type')}",
            )
        return EventDispatchResult(dispatched=count, skipped=0, failed=0)


def load_event_dispatch_engine(path: str) -> Type[BaseEventDispatchEngine]:
    return load_plugin_class(path, BaseEventDispatchEngine, "EventDispatch Engine")
