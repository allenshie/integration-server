"""Ingestion handler plugin interface."""
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib import import_module
from typing import Any, Dict, List, Type

from smart_workflow import TaskContext, TaskError


@dataclass
class IngestionResult:
    """Normalized ingestion output."""

    events: List[Dict[str, Any]]
    raw_count: int
    dropped: int


class BaseIngestionHandler(ABC):
    """Base interface for ingestion handlers."""

    def __init__(self, context: TaskContext | None = None) -> None:
        self._context = context

    @abstractmethod
    def process(self, context: TaskContext, raw_events: List[Dict[str, Any]]) -> IngestionResult:
        """Return deduplicated events and stats."""


class DefaultIngestionHandler(BaseIngestionHandler):
    """Replicates the existing normalization/dedup logic."""

    def __init__(self, context: TaskContext | None = None) -> None:
        super().__init__(context)
        self._max_age_seconds = context.config.edge_event_max_age_seconds if context else None

    def process(self, context: TaskContext, raw_events: List[Dict[str, Any]]) -> IngestionResult:
        max_age_seconds = self._max_age_seconds or context.config.edge_event_max_age_seconds
        max_age = timedelta(seconds=max_age_seconds)
        now = datetime.now(timezone.utc)

        latest_events: Dict[str, Dict[str, Any]] = {}
        dropped = 0
        for item in raw_events:
            parsed = self._normalize_event(item, now, max_age)
            if parsed is None:
                dropped += 1
                continue
            camera_id = parsed["camera_id"]
            current = latest_events.get(camera_id)
            if current is None or parsed["timestamp"] > current["timestamp"]:
                latest_events[camera_id] = parsed

        deduped_events = list(latest_events.values())
        return IngestionResult(events=deduped_events, raw_count=len(raw_events), dropped=dropped)

    @staticmethod
    def _normalize_event(
        item: Dict[str, Any],
        now: datetime,
        max_age: timedelta,
    ) -> Dict[str, Any] | None:
        camera_id = item.get("camera_id")
        timestamp_str = item.get("timestamp")
        if not camera_id or not timestamp_str:
            return None
        try:
            event_time = datetime.fromisoformat(timestamp_str)
        except Exception:  # pylint: disable=broad-except
            return None
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        if now - event_time > max_age:
            return None
        detections = item.get("detections") or []
        return {
            "camera_id": camera_id,
            "timestamp": event_time,
            "detections": detections,
        }


def load_ingestion_handler(path: str) -> Type[BaseIngestionHandler]:
    if ":" in path:
        module_name, class_name = path.split(":", 1)
    elif "." in path:
        module_name, class_name = path.rsplit(".", 1)
    else:
        raise TaskError(f"無法解析 Ingestion Handler 路徑：{path}")

    module = import_module(module_name)
    attr = getattr(module, class_name, None)
    if attr is None or not inspect.isclass(attr):
        raise TaskError(f"在模組 {module_name} 找不到 Ingestion Handler {class_name}")
    if not issubclass(attr, BaseIngestionHandler):
        raise TaskError(f"{class_name} 必須繼承 BaseIngestionHandler")
    return attr
