"""Tracking handler plugin interface."""
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, Iterable, List, Type

from smart_workflow import TaskContext, TaskError

from .engine import MCMOTEngine, MCMOTResult


@dataclass
class TrackingResult:
    tracked_objects: List[Dict[str, Any]]
    global_objects: List[Dict[str, Any]]
    processed_events: int


class BaseTrackingHandler(ABC):
    """Base interface for tracking engines."""

    def __init__(self, context: TaskContext | None = None) -> None:
        self._context = context

    @abstractmethod
    def process(self, context: TaskContext, events: Iterable[Dict[str, Any]]) -> TrackingResult:
        """Return tracking/global objects for the given events."""


class DefaultTrackingHandler(BaseTrackingHandler):
    """Handler that delegates to the existing MCMOT engine."""

    def __init__(self, context: TaskContext | None = None) -> None:
        super().__init__(context)
        self._engine = context.get_resource("mcmot_engine") if context else None

    def process(self, context: TaskContext, events: Iterable[Dict[str, Any]]) -> TrackingResult:
        engine = self._ensure_engine(context)
        events_list = list(events)
        if not events_list:
            result = engine.process_events([])
            return TrackingResult(tracked_objects=[], global_objects=result.global_objects, processed_events=0)
        result = engine.process_events(events_list)
        return TrackingResult(
            tracked_objects=result.tracked_objects,
            global_objects=result.global_objects,
            processed_events=len(events_list),
        )

    def _ensure_engine(self, context: TaskContext) -> MCMOTEngine:
        if self._engine is None:
            self._engine = context.require_resource("mcmot_engine")
        return self._engine


def load_tracking_handler(path: str) -> Type[BaseTrackingHandler]:
    if ":" in path:
        module_name, class_name = path.split(":", 1)
    elif "." in path:
        module_name, class_name = path.rsplit(".", 1)
    else:
        raise TaskError(f"無法解析 Tracking Handler 路徑：{path}")

    module = import_module(module_name)
    attr = getattr(module, class_name, None)
    if attr is None or not inspect.isclass(attr):
        raise TaskError(f"在模組 {module_name} 找不到 Tracking Handler {class_name}")
    if not issubclass(attr, BaseTrackingHandler):
        raise TaskError(f"{class_name} 必須繼承 BaseTrackingHandler")
    return attr
