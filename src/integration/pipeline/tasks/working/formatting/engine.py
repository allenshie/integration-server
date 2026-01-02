"""Engine objects for formatting MC-MOT output."""
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Dict, Iterable, Type

from smart_workflow import TaskContext, TaskError

from .expect_output import ExpectOutputTransformer


class BaseFormatEngine(ABC):
    """Interface for converting MC-MOT result into downstream payload."""

    @abstractmethod
    def build_payload(
        self,
        context: TaskContext,
        events: Iterable[Dict[str, Any]],
        tracked: Iterable[Dict[str, Any]],
        global_objects: Iterable[Dict[str, Any]],
        snapshot_path: str | None,
    ) -> Dict[str, Any]:
        """Return a dict to store in `rules_payload`."""


class DefaultFormatEngine(BaseFormatEngine):
    """Original summary logic used by the integration core."""

    def __init__(self, timezone_override: timezone | None = None) -> None:
        self._timezone = timezone_override or timezone.utc
        self._expect_transformer = ExpectOutputTransformer(tz=self._timezone)

    def build_payload(
        self,
        context: TaskContext,
        events: Iterable[Dict[str, Any]],
        tracked: Iterable[Dict[str, Any]],
        global_objects: Iterable[Dict[str, Any]],
        snapshot_path: str | None,
    ) -> Dict[str, Any]:
        events_list = list(events)
        tracked_list = list(tracked)
        global_list = list(global_objects)
        expect_output = self._expect_transformer.transform(tracked_list, global_list)
        payload = {
            "events": events_list,
            "tracked_objects": tracked_list,
            "global_objects": global_list,
            "camera_summary": self._summarize_by_camera(tracked_list),
            "global_summary": self._summarize_global(global_list),
            "metadata": {
                "generated_at": datetime.now(self._timezone).isoformat(),
                "global_map_snapshot": snapshot_path,
            },
            "expect_output": expect_output,
        }
        return payload

    @staticmethod
    def _summarize_by_camera(tracked: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        summary: Dict[str, Dict[str, Any]] = {}
        for item in tracked:
            camera_id = item.get("camera_id") or "unknown"
            camera_entry = summary.setdefault(camera_id, {"total": 0, "classes": {}})
            camera_entry["total"] += 1
            class_name = item.get("class_name") or "unknown"
            classes = camera_entry.setdefault("classes", {})
            classes[class_name] = classes.get(class_name, 0) + 1
        return summary

    @staticmethod
    def _summarize_global(global_objects: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        total = 0
        class_counter: Dict[str, int] = {}
        for obj in global_objects:
            total += 1
            class_name = obj.get("class_name") or "unknown"
            class_counter[class_name] = class_counter.get(class_name, 0) + 1
        return {"total": total, "classes": class_counter}


def load_format_engine(path: str) -> Type[BaseFormatEngine]:
    if ":" in path:
        module_name, class_name = path.split(":", 1)
    elif "." in path:
        module_name, class_name = path.rsplit(".", 1)
    else:
        raise TaskError(f"無法解析格式策略路徑：{path}")

    module = import_module(module_name)
    attr = getattr(module, class_name, None)
    if attr is None or not inspect.isclass(attr):
        raise TaskError(f"在模組 {module_name} 找不到格式策略 {class_name}")
    if not issubclass(attr, BaseFormatEngine):
        raise TaskError(f"{class_name} 必須繼承 BaseFormatEngine")
    return attr
