"""Event dispatch stage."""
from __future__ import annotations

from typing import Any, Dict, List

from smart_workflow import TaskContext, TaskResult, TaskError

from integration.pipeline.tasks.base import QuietTaskBase
from integration.pipeline.tasks.summary import EVENT_DISPATCH_STATS_RESOURCE, store_stage_stats
from .engine import (
    BaseEventDispatchEngine,
    DefaultEventDispatchEngine,
    load_event_dispatch_engine,
)


class EventDispatchTask(QuietTaskBase):
    name = "event_dispatch"

    def __init__(self, context: TaskContext | None = None) -> None:
        self._engine: BaseEventDispatchEngine | None = None

    def run(self, context: TaskContext) -> TaskResult:
        if self._engine is None:
            cfg = getattr(context.config, "event_dispatch", None)
            self._engine = self._init_engine(cfg, context)
        events: List[Dict[str, Any]] = context.get_resource("rule_events") or []
        if not isinstance(events, list):
            raise TaskError("rule_events 必須是 list")
        for event in events:
            if not isinstance(event, dict):
                raise TaskError("rule_events 必須是 dict list")
            for key in ("id", "name", "timestamp", "event_type"):
                if key not in event:
                    raise TaskError(f"event missing field: {key}")

        result = self._engine.dispatch(events, context)
        store_stage_stats(
            context,
            EVENT_DISPATCH_STATS_RESOURCE,
            {
                "dispatched": result.dispatched,
                "skipped": result.skipped,
                "failed": result.failed,
                "input_count": len(events),
                "input_unit": "events",
                "result_count": result.dispatched,
                "result_unit": "dispatched",
            },
        )
        payload = {
            "dispatched": result.dispatched,
            "skipped": result.skipped,
            "failed": result.failed,
        }
        if result.details:
            payload["details"] = result.details
        return TaskResult(status="event_dispatch_done", payload=payload)

    def _init_engine(self, cfg, context: TaskContext | None) -> BaseEventDispatchEngine:
        engine_path = getattr(cfg, "engine_class", None) if cfg else None
        return self._init_plugin(
            plugin_name="事件派送 Engine",
            loader=load_event_dispatch_engine,
            plugin_path=engine_path,
            default_factory=lambda: DefaultEventDispatchEngine(context=context),
            init_kwargs={"context": context},
        )
