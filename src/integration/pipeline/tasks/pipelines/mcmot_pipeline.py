"""Working hours pipeline composed of multiple stages."""
from __future__ import annotations

import time
from typing import List

from smart_workflow import BaseTask, TaskContext, TaskResult

from integration.pipeline.tasks.base import QuietTaskBase
from integration.pipeline.tasks.summary import (
    SUMMARY_INTERVAL_SECONDS,
    mark_pipeline_summary_error,
    render_pipeline_summary,
    reset_pipeline_cycle_stats,
    reset_pipeline_summary,
)
from integration.pipeline.tasks.nodes.ingestion.task import IngestionTask
from integration.pipeline.tasks.nodes.tracking.task import MCMOTTask
from integration.pipeline.tasks.nodes.matching_broadcast.task import MatchingBroadcastTask
from integration.pipeline.tasks.nodes.rules.task import RuleEvaluationTask
from integration.pipeline.tasks.nodes.event_dispatch.task import EventDispatchTask


class MCMOTPipelineTask(QuietTaskBase):
    name = "mcmot_pipeline"

    def __init__(self, context: TaskContext, nodes: List[BaseTask] | None = None) -> None:
        self.pipeline_nodes: List[BaseTask] = nodes if nodes is not None else self._build_nodes(context)
        self._last_summary_time = 0.0
        self._throughput_started_at = 0.0
        self._throughput_last_report_at = 0.0
        self._throughput_totals = {
            "raw": 0,
            "events": 0,
            "duplicates": 0,
            "active_batches": 0,
            "idle_batches": 0,
        }
        self._throughput_last_report_totals = dict(self._throughput_totals)
        self._active_latency_total_ms = 0.0
        self._active_latency_last_report_total_ms = 0.0
        configured_interval = getattr(context.config, "pipeline_summary_interval_seconds", SUMMARY_INTERVAL_SECONDS)
        try:
            self._summary_interval_seconds = float(configured_interval)
        except (TypeError, ValueError):
            self._summary_interval_seconds = SUMMARY_INTERVAL_SECONDS
        if self._summary_interval_seconds <= 0:
            self._summary_interval_seconds = SUMMARY_INTERVAL_SECONDS

    def run(self, context: TaskContext) -> TaskResult:
        # Keep the previous summary window intact.  Each task reports facts
        # for this cycle; the aggregator resets only after a summary is
        # emitted.
        reset_pipeline_cycle_stats(context)
        run_started_at = time.monotonic()
        try:
            if not self.pipeline_nodes:
                self._maybe_log_summary(context, status="ok")
                context.logger.debug("mcmot pipeline completed without nodes")
                return TaskResult(status="mcmot_pipeline_done")

            ingestion_result = self.pipeline_nodes[0].execute(context)
            has_new_data = self._has_new_data(context, ingestion_result)
            self._record_throughput(ingestion_result, has_new_data, run_started_at)
            if not has_new_data and context.get_resource("trajectory_store") is None:
                self._maybe_log_summary(context, status="ok")
                context.logger.debug("mcmot pipeline skipped: no new data")
                return TaskResult(status="mcmot_pipeline_skipped", payload={"reason": "no_new_data"})

            mcmot_result = self.pipeline_nodes[1].execute(context)
            mcmot_payload = getattr(mcmot_result, "payload", None) or {}
            if context.get_resource("trajectory_store") is not None and not mcmot_payload.get("success", True):
                self._maybe_log_summary(context, status="error")
                return TaskResult(status="mcmot_pipeline_failed", payload={"reason": "mcmot_failed"})
            if not has_new_data and not mcmot_payload.get("matching_performed", False):
                self._maybe_log_summary(context, status="ok")
                context.logger.debug("mcmot pipeline skipped: cadence not due")
                return TaskResult(status="mcmot_pipeline_skipped", payload={"reason": "cadence_not_due"})
            if context.get_resource("trajectory_store") is not None and not mcmot_payload.get("matching_performed", False):
                self._maybe_log_summary(context, status="ok")
                return TaskResult(status="mcmot_pipeline_skipped", payload={"reason": "cadence_not_due"})

            for node in self.pipeline_nodes[2:]:
                node.execute(context)
            run_finished_at = time.monotonic()
            self._record_active_latency(run_started_at, run_finished_at)
        except Exception:
            self._maybe_log_summary(context, status="error")
            raise
        self._maybe_log_summary(context, status="ok")
        context.logger.debug("mcmot pipeline completed")
        return TaskResult(status="mcmot_pipeline_done")

    def _build_nodes(self, context: TaskContext) -> List[BaseTask]:
        nodes: List[BaseTask] = [
            IngestionTask(context),
            MCMOTTask(context),
            MatchingBroadcastTask(context),
        ]
        format_task = self._build_format_task(context)
        if format_task:
            nodes.append(format_task)
        nodes.append(RuleEvaluationTask(context))
        nodes.append(EventDispatchTask(context))
        return nodes

    def _build_format_task(self, context: TaskContext) -> BaseTask | None:
        cfg = getattr(context.config, "format_task", None)
        enabled = getattr(cfg, "enabled", True)
        if not enabled:
            context.logger.info("FORMAT_TASK_ENABLED=0，略過格式轉換節點")
            return None
        from integration.pipeline.tasks.nodes.formatting.task import (
            FormatConversionTask,
        )

        strategy = getattr(cfg, "strategy_class", None)
        if strategy:
            context.logger.info(f"使用格式轉換策略：{strategy}")
        else:
            context.logger.info("使用預設格式轉換策略")
        return FormatConversionTask(context)

    @classmethod
    def describe_flow(cls, config) -> str:
        def _class_name(path: str | None, default: str) -> str:
            if not path:
                return default
            if ":" in path:
                return path.split(":", 1)[1]
            return path.rsplit(".", 1)[-1]

        ingestion_handler = _class_name(
            getattr(getattr(config, "ingestion_task", None), "engine_class", None),
            "DefaultIngestionEngine",
        )
        format_engine = _class_name(
            getattr(getattr(config, "format_task", None), "strategy_class", None),
            "DefaultFormatEngine",
        )
        matching_broadcast_enabled = getattr(getattr(config, "matching_broadcast", None), "enabled", False)
        rules_engine = _class_name(
            getattr(getattr(config, "rules", None), "engine_class", None),
            "DefaultRuleEngine",
        )
        dispatch_engine = _class_name(
            getattr(getattr(config, "event_dispatch", None), "engine_class", None),
            "DefaultEventDispatchEngine",
        )

        return (
            f"IngestionTask(engine={ingestion_handler}) -> "
            f"MCMOTTask(engine=MCMOTEngine) -> "
            f"MatchingBroadcastTask(enabled={matching_broadcast_enabled}) -> "
            f"FormatConversionTask(strategy={format_engine}) -> "
            f"RuleEvaluationTask(engine={rules_engine}) -> "
            f"EventDispatchTask(engine={dispatch_engine})"
        )

    def _maybe_log_summary(self, context: TaskContext, status: str) -> None:
        if status == "error":
            mark_pipeline_summary_error(context)

        now = time.monotonic()
        if self._last_summary_time > 0.0:
            elapsed = now - self._last_summary_time
            if elapsed < self._summary_interval_seconds:
                return

        phase_state = context.get_resource("phase_task_state")
        phase_name = "-"
        if isinstance(phase_state, dict):
            phase_name = str(phase_state.get("last_phase") or "-")

        throughput = self._build_throughput_snapshot(now)
        latency = self._build_latency_snapshot(now)
        summary = render_pipeline_summary(
            context,
            phase_name,
            self._summary_interval_seconds,
            status=status,
            throughput=throughput,
            latency=latency,
        )
        context.logger.info(summary)
        self._last_summary_time = now
        self._throughput_last_report_at = now
        self._throughput_last_report_totals = dict(self._throughput_totals)
        self._active_latency_last_report_total_ms = self._active_latency_total_ms
        reset_pipeline_summary(context)

    def _record_throughput(self, result: TaskResult | None, has_new_data: bool, started_at: float) -> None:
        payload = getattr(result, "payload", None)
        if not isinstance(payload, dict):
            payload = {}

        if self._throughput_started_at <= 0.0:
            self._throughput_started_at = started_at

        self._throughput_totals["raw"] += self._to_non_negative_int(payload.get("raw"))
        self._throughput_totals["events"] += self._to_non_negative_int(payload.get("events"))
        self._throughput_totals["duplicates"] += self._to_non_negative_int(payload.get("duplicates"))
        if has_new_data:
            self._throughput_totals["active_batches"] += 1
        else:
            self._throughput_totals["idle_batches"] += 1

    def _record_active_latency(self, started_at: float, finished_at: float) -> None:
        elapsed_ms = max(0.0, finished_at - started_at) * 1000.0
        self._active_latency_total_ms += elapsed_ms

    def _build_throughput_snapshot(self, now: float) -> dict[str, float | int] | None:
        if self._throughput_started_at <= 0.0:
            return None

        baseline = self._throughput_last_report_at or self._throughput_started_at
        elapsed_seconds = now - baseline
        if elapsed_seconds <= 0.0:
            elapsed_seconds = 0.0

        if self._throughput_last_report_at > 0.0:
            base_totals = self._throughput_last_report_totals
        else:
            base_totals = {
                "raw": 0,
                "events": 0,
                "duplicates": 0,
                "active_batches": 0,
                "idle_batches": 0,
            }

        raw_delta = self._throughput_totals["raw"] - base_totals["raw"]
        events_delta = self._throughput_totals["events"] - base_totals["events"]
        duplicates_delta = self._throughput_totals["duplicates"] - base_totals["duplicates"]
        active_batches_delta = self._throughput_totals["active_batches"] - base_totals["active_batches"]
        idle_batches_delta = self._throughput_totals["idle_batches"] - base_totals["idle_batches"]

        if elapsed_seconds > 0.0:
            source_fps = raw_delta / elapsed_seconds
            processed_fps = events_delta / elapsed_seconds
            duplicate_skip_fps = duplicates_delta / elapsed_seconds
        else:
            source_fps = 0.0
            processed_fps = 0.0
            duplicate_skip_fps = 0.0

        return {
            "elapsed_seconds": elapsed_seconds,
            "source_fps": source_fps,
            "processed_fps": processed_fps,
            "duplicate_skip_fps": duplicate_skip_fps,
            "active_batches": active_batches_delta,
            "idle_batches": idle_batches_delta,
        }

    def _build_latency_snapshot(self, now: float) -> dict[str, float | int | None] | None:
        if self._throughput_started_at <= 0.0:
            return None

        baseline = self._throughput_last_report_at or self._throughput_started_at
        elapsed_seconds = now - baseline
        if elapsed_seconds <= 0.0:
            elapsed_seconds = 0.0

        active_batches_delta = self._throughput_totals["active_batches"]
        if self._throughput_last_report_at > 0.0:
            active_batches_delta -= self._throughput_last_report_totals["active_batches"]

        active_latency_delta_ms = self._active_latency_total_ms - self._active_latency_last_report_total_ms
        avg_active_ms: float | None = None
        if active_batches_delta > 0:
            avg_active_ms = active_latency_delta_ms / active_batches_delta

        return {
            "elapsed_seconds": elapsed_seconds,
            "avg_active_ms": avg_active_ms,
        }

    @staticmethod
    def _to_non_negative_int(value: object) -> int:
        try:
            number = int(value)
        except (TypeError, ValueError):
            return 0
        return number if number > 0 else 0

    @staticmethod
    def _has_new_data(context: TaskContext, result: TaskResult | None) -> bool:
        payload = getattr(result, "payload", None)
        if isinstance(payload, dict) and "has_new_data" in payload:
            return payload.get("has_new_data") is not False

        resource_value = context.get_resource("pipeline_has_new_data")
        if isinstance(resource_value, bool):
            return resource_value
        return True
