"""Rule evaluation stage."""
from __future__ import annotations

from typing import Any, Dict

from smart_workflow import TaskContext, TaskResult, TaskError

from integration.pipeline.tasks.base import QuietTaskBase
from integration.pipeline.tasks.summary import RULE_STATS_RESOURCE, store_stage_stats
from .engine import BaseRuleEngine, DefaultRuleEngine, RuleEngineResult, load_rule_engine


class RuleEvaluationTask(QuietTaskBase):
    name = "rule_evaluation"

    def __init__(self, context: TaskContext | None = None) -> None:
        cfg = getattr(context.config, "rules", None) if context else None
        self._detail = getattr(cfg, "detail", None)
        self._engine: BaseRuleEngine | None = None

    def run(self, context: TaskContext) -> TaskResult:
        if self._engine is None:
            cfg = getattr(context.config, "rules", None)
            self._engine = self._init_engine(cfg, context)
        payload: Dict[str, Any] | None = context.get_resource("rules_payload")
        summary = (payload or {}).get("global_summary") or {}
        detail_suffix = f" ({self._detail})" if self._detail else ""
        engine_result = self._engine.process(context, payload)
        self._apply_context_updates(context, engine_result)
        self._apply_rule_events(context, engine_result)
        warning_events = context.get_resource("rule_events") or []
        store_stage_stats(
            context,
            RULE_STATS_RESOURCE,
            {
                "warnings": len(warning_events),
                "input_count": summary.get("total", 0),
                "input_unit": "objects",
                "result_count": len(warning_events),
                "result_unit": "warnings",
                "failed": 0,
            },
        )
        total = summary.get("total", 0)
        context.logger.debug(
            f"完成節點：違規/作業規則判定{detail_suffix}，全域物件總數 {total}",
        )
        result_payload = engine_result.task_payload if engine_result and engine_result.task_payload else {"global_objects": total}
        return TaskResult(status="rules_done", payload=result_payload)

    def _init_engine(self, cfg, context: TaskContext | None) -> BaseRuleEngine:
        engine_path = getattr(cfg, "engine_class", None) if cfg else None
        return self._init_plugin(
            plugin_name="規則 Engine",
            loader=load_rule_engine,
            plugin_path=engine_path,
            default_factory=lambda: DefaultRuleEngine(context=context),
            init_kwargs={"context": context},
        )

    def _apply_context_updates(self, context: TaskContext, result: RuleEngineResult | None) -> None:
        if not result or not result.context_updates:
            return
        for key, value in result.context_updates.items():
            context.set_resource(key, value)

    def _apply_rule_events(self, context: TaskContext, result: RuleEngineResult | None) -> None:
        events = None
        if result and result.events is not None:
            events = result.events
        elif result and result.context_updates:
            events = result.context_updates.get("rule_events")

        if events is None:
            return
        if not isinstance(events, list):
            raise TaskError("rule_events 必須是 list")
        for event in events:
            if not isinstance(event, dict):
                raise TaskError("rule_events 必須是 dict list")
            for key in ("id", "name", "timestamp", "event_type"):
                if key not in event:
                    raise TaskError(f"event missing field: {key}")
        context.set_resource("rule_events", events)
