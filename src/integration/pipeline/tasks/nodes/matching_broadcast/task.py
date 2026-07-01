"""Task that publishes MC-MOT matching results for downstream consumers."""
from __future__ import annotations

from smart_workflow import TaskContext, TaskResult

from integration.pipeline.tasks.base import QuietTaskBase
from integration.pipeline.tasks.summary import MATCHING_BROADCAST_STATS_RESOURCE, store_stage_stats

from .engine import BaseMatchingBroadcastEngine, DefaultMatchingBroadcastEngine, MatchingBroadcastResult
from .recorder import MatchingBroadcastObservationWriter, build_matching_broadcast_records


class MatchingBroadcastTask(QuietTaskBase):
    """Publish the current matching table as a shared payload."""

    name = "matching_broadcast"

    def __init__(self, context: TaskContext | None = None) -> None:
        self._engine: BaseMatchingBroadcastEngine | None = None

    def run(self, context: TaskContext) -> TaskResult:
        if self._engine is None:
            self._engine = self._init_engine(context)

        tracked_objects = list(context.get_resource("mc_mot_tracked") or [])
        global_objects = list(context.get_resource("mc_mot_global_objects") or [])
        edge_events = list(context.get_resource("edge_events") or [])
        recorded, recording_failed = self._maybe_record_observations(
            context,
            edge_events,
            tracked_objects,
            global_objects,
        )
        result = self._engine.broadcast(tracked_objects, context)
        self._store_stage_stats(context, result, recorded=recorded, recording_failed=recording_failed)
        return self._build_task_result(result, recorded=recorded, recording_failed=recording_failed)

    def _init_engine(self, context: TaskContext | None) -> BaseMatchingBroadcastEngine:
        return self._init_plugin(
            plugin_name="Matching Broadcast Engine",
            plugin_cls=DefaultMatchingBroadcastEngine,
            init_kwargs={"context": context},
        )

    @staticmethod
    def _store_stage_stats(
        context: TaskContext,
        result: MatchingBroadcastResult,
        *,
        recorded: int,
        recording_failed: int,
    ) -> None:
        store_stage_stats(
            context,
            MATCHING_BROADCAST_STATS_RESOURCE,
            {
                "dispatched": result.dispatched,
                "skipped": result.skipped,
                "failed": result.failed,
                "recorded": recorded,
                "recording_failed": recording_failed,
            },
        )

    @staticmethod
    def _build_task_result(
        result: MatchingBroadcastResult,
        *,
        recorded: int,
        recording_failed: int,
    ) -> TaskResult:
        if result.dispatched > 0:
            status = "matching_broadcast_done"
        elif result.failed > 0:
            status = "matching_broadcast_failed"
        else:
            status = "matching_broadcast_skipped"

        payload = result.task_payload or {}
        if result.reason and "reason" not in payload:
            payload = dict(payload)
            payload["reason"] = result.reason
        if recorded > 0 or recording_failed > 0:
            payload = dict(payload)
            payload["recorded"] = recorded
            payload["recording_failed"] = recording_failed
        return TaskResult(status=status, payload=payload)

    @staticmethod
    def _maybe_record_observations(
        context: TaskContext,
        edge_events: list[dict[str, object]],
        tracked_objects: list[dict[str, object]],
        global_objects: list[dict[str, object]],
    ) -> tuple[int, int]:
        matching_cfg = getattr(context.config, "matching_broadcast", None)
        recording_cfg = getattr(matching_cfg, "recording", None)
        enabled = bool(getattr(recording_cfg, "enabled", False)) if recording_cfg is not None else False
        if not enabled:
            return 0, 0

        output_path = getattr(recording_cfg, "path", None)
        if not output_path:
            context.logger.warning("matching broadcast recording disabled: no output path configured")
            return 0, 1

        try:
            writer = MatchingBroadcastObservationWriter(output_path)
            records = build_matching_broadcast_records(edge_events, tracked_objects, global_objects)
            return writer.write_records(records), 0
        except Exception as exc:  # pylint: disable=broad-except
            context.logger.warning("matching broadcast recording failed: %s", exc)
            return 0, 1
