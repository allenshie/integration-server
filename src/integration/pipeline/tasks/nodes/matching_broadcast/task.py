"""Task that publishes matching snapshots for downstream consumers."""
from __future__ import annotations

from typing import Any, Mapping
from uuid import uuid4

from smart_workflow import TaskContext, TaskResult

from integration.pipeline.tasks.base import QuietTaskBase
from integration.pipeline.tasks.summary import (
    MATCHING_BROADCAST_STATS_RESOURCE,
    MCMOT_STATE_RESOURCE,
    store_stage_stats,
)

from .engine import BaseMatchingBroadcastEngine, DefaultMatchingBroadcastEngine, MatchingBroadcastResult
from .recorder import MatchingBroadcastObservationWriter, build_matching_broadcast_records
from .schema import MatchingBroadcastBatch


class MatchingBroadcastTask(QuietTaskBase):
    """Publish the current matching snapshot as a shared payload."""

    name = "matching_broadcast"

    def __init__(self, context: TaskContext | None = None) -> None:
        self._engine: BaseMatchingBroadcastEngine | None = None
        self._broadcast_session_id = f"app-{uuid4().hex}"
        self._broadcast_frame_seq = 0

    def run(self, context: TaskContext) -> TaskResult:
        if self._engine is None:
            self._engine = self._init_engine(context)

        tracked_objects = [dict(item) for item in context.get_resource("mc_mot_tracked") or []]
        global_objects = [dict(item) for item in context.get_resource("mc_mot_global_objects") or []]
        edge_events = [dict(item) for item in context.get_resource("edge_events") or []]
        mcmot_state = context.get_resource(MCMOT_STATE_RESOURCE)
        batch, reason = self._build_batch(
            edge_events,
            tracked_objects,
            global_objects,
            mcmot_state=mcmot_state,
        )
        if batch is None:
            context.logger.warning(f"matching broadcast failed: {reason}")
            result = MatchingBroadcastResult(failed=1, reason=reason)
            self._store_stage_stats(context, result, recorded=0, recording_failed=0)
            return self._build_task_result(result, recorded=0, recording_failed=0)

        result = self._engine.broadcast(batch, context)
        recorded, recording_failed = self._maybe_record_observations(
            context,
            edge_events,
            batch,
            result.message_payload,
        )
        self._store_stage_stats(context, result, recorded=recorded, recording_failed=recording_failed)
        return self._build_task_result(result, recorded=recorded, recording_failed=recording_failed)

    def _init_engine(self, context: TaskContext | None) -> BaseMatchingBroadcastEngine:
        return self._init_plugin(
            plugin_name="Matching Broadcast Engine",
            plugin_cls=DefaultMatchingBroadcastEngine,
            init_kwargs={"context": context},
        )

    def _build_batch(
        self,
        edge_events: list[dict[str, Any]],
        tracked_objects: list[dict[str, Any]],
        global_objects: list[dict[str, Any]],
        *,
        mcmot_state: Mapping[str, Any] | None = None,
    ) -> tuple[MatchingBroadcastBatch | None, str | None]:
        # A typed MCMOT attempt may consume pending TrajectoryStore data even
        # when this cycle has no fresh edge event.  In that case the MCMOT
        # state is the authoritative identity for the broadcast attempt.
        state = mcmot_state if isinstance(mcmot_state, Mapping) else {}
        has_mcmot_identity = (
            state.get("last_matching_status") == "success"
            and _normalize_text(state.get("last_matching_at")) is not None
        )
        if not edge_events and not has_mcmot_identity:
            return None, "missing_batch_identity"

        self._broadcast_frame_seq += 1

        capture_ts = None
        for event in edge_events:
            capture_ts = _normalize_text(event.get("capture_ts") or event.get("timestamp"))
            if capture_ts is not None:
                break
        if capture_ts is None and has_mcmot_identity:
            capture_ts = _normalize_text(state.get("last_matching_at"))

        return (
            MatchingBroadcastBatch(
                session_id=self._broadcast_session_id,
                frame_seq=self._broadcast_frame_seq,
                capture_ts=capture_ts,
                tracked_objects=tracked_objects,
                global_objects=global_objects,
            ),
            None,
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
                "failed": result.failed + recording_failed,
                "recorded": recorded,
                "recording_failed": recording_failed,
                "input_count": len(context.get_resource("mc_mot_tracked") or []),
                "input_unit": "tracked",
                "result_count": result.dispatched,
                "result_unit": "dispatched",
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

        payload: dict[str, Any] = {
            "session_id": result.session_id,
            "frame_seq": result.frame_seq,
            "dispatched": result.dispatched,
            "skipped": result.skipped,
            "failed": result.failed,
            "recorded": recorded,
            "recording_failed": recording_failed,
        }
        if result.reason:
            payload["reason"] = result.reason
        if result.task_payload:
            payload.update(result.task_payload)
        return TaskResult(status=status, payload=payload)

    @staticmethod
    def _maybe_record_observations(
        context: TaskContext,
        edge_events: list[dict[str, Any]],
        batch: MatchingBroadcastBatch,
        snapshot_payload: Mapping[str, Any] | None,
    ) -> tuple[int, int]:
        matching_cfg = getattr(context.config, "matching_broadcast", None)
        recording_cfg = getattr(matching_cfg, "recording", None)
        enabled = bool(getattr(recording_cfg, "enabled", False)) if recording_cfg is not None else False
        if not enabled or snapshot_payload is None:
            return 0, 0

        output_path = getattr(recording_cfg, "path", None)
        if not output_path:
            context.logger.warning("matching broadcast recording disabled: no output path configured")
            return 0, 1

        try:
            writer = MatchingBroadcastObservationWriter(output_path)
            records = build_matching_broadcast_records(edge_events, batch, snapshot_payload)
            return writer.write_records(records), 0
        except Exception as exc:  # pylint: disable=broad-except
            context.logger.warning(f"matching broadcast recording failed: {exc}")
            return 0, 1


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
