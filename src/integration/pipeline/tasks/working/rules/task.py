"""Rule evaluation stage."""
from __future__ import annotations

from typing import Any, Dict

from smart_workflow import BaseTask, TaskContext, TaskResult, TaskError

from .engine import BaseRuleEngine, DefaultRuleEngine, RuleEngineResult, load_rule_engine


class RuleEvaluationTask(BaseTask):
    name = "rule_evaluation"

    def __init__(self, context: TaskContext | None = None) -> None:
        cfg = getattr(context.config, "rules", None) if context else None
        self._detail = getattr(cfg, "detail", None)
        self._engine = self._init_engine(cfg, context)

    def run(self, context: TaskContext) -> TaskResult:
        payload: Dict[str, Any] | None = context.get_resource("rules_payload")
        summary = (payload or {}).get("global_summary") or {}
        detail_suffix = f" ({self._detail})" if self._detail else ""
        engine_result = self._engine.process(context, payload)
        self._apply_context_updates(context, engine_result)
        total = summary.get("total", 0)
        context.logger.info(
            "完成節點：違規/作業規則判定%s，全域物件總數 %s",
            detail_suffix,
            total,
        )
        result_payload = engine_result.task_payload if engine_result and engine_result.task_payload else {"global_objects": total}
        return TaskResult(status="rules_done", payload=result_payload)

    def _init_engine(self, cfg, context: TaskContext | None) -> BaseRuleEngine:
        engine_path = getattr(cfg, "engine_class", None) if cfg else None
        if not engine_path:
            return DefaultRuleEngine(context=context)
        try:
            engine_cls = load_rule_engine(engine_path)
        except Exception as exc:  # pylint: disable=broad-except
            raise TaskError(f"無法載入規則 Engine：{engine_path}") from exc

        try:
            return engine_cls(context=context)
        except TypeError:
            try:
                return engine_cls()
            except TypeError as exc:  # pragma: no cover
                raise TaskError(f"規則 Engine {engine_path} 無法初始化") from exc

    def _apply_context_updates(self, context: TaskContext, result: RuleEngineResult | None) -> None:
        if not result or not result.context_updates:
            return
        for key, value in result.context_updates.items():
            context.set_resource(key, value)
