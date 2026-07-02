"""Task that publishes matching snapshots for downstream consumers."""
from __future__ import annotations

from typing import Any, Mapping

from smart_workflow import TaskContext, TaskResult

from integration.pipeline.tasks.base import QuietTaskBase
from integration.pipeline.tasks.summary import MATCHING_BROADCAST_STATS_RESOURCE, store_stage_stats

from .engine import BaseMatchingBroadcastEngine, DefaultMatchingBroadcastEngine, MatchingBroadcastResult
from .recorder import MatchingBroadcastObservationWriter, build_matching_broadcast_records
from .schema import MatchingBroadcastBatch


class MatchingBroadcastTask(QuietTaskBase):
    """Publish the current matching snapshot as a shared payload."""

    name = "matching_broadcast"

    def __init__(self, context: TaskContext | None = None) -> None:
        self._engine: BaseMatchingBroadcastEngine | None = None

    def run(self, context: TaskContext) -> TaskResult:
        if self._engine is None:
            self._engine = self._init_engine(context)

        tracked_objects = [dict(item) for item in context.get_resource("mc_mot_tracked") or []]
        global_objects = [dict(item) for item in context.get_resource("mc_mot_global_objects") or []]
        edge_events = [dict(item) for item in context.get_resource("edge_events") or []]

        batch, reason = self._build_batch(edge_events, tracked_objects, global_objects)
        if batch is None:
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

    @staticmethod
    def _build_batch(
        edge_events: list[dict[str, Any]],
        tracked_objects: list[dict[str, Any]],
        global_objects: list[dict[str, Any]],
    ) -> tuple[MatchingBroadcastBatch | None, str | None]:
        session_ids = {_normalize_text(item.get("session_id")) for item in edge_events}
        frame_seqs = {_coerce_non_negative_int(item.get("frame_seq")) for item in edge_events}
        session_ids.discard(None)
        frame_seqs.discard(None)

        if len(session_ids) != 1 or len(frame_seqs) != 1:
            return None, "inconsistent_batch_identity"

        capture_ts = None
        for event in edge_events:
            capture_ts = _normalize_text(event.get("capture_ts") or event.get("timestamp"))
            if capture_ts is not None:
                break

        return (
            MatchingBroadcastBatch(
                session_id=next(iter(session_ids)),
                frame_seq=next(iter(frame_seqs)),
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
            context.logger.warning("matching broadcast recording failed: %s", exc)
            return 0, 1


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_non_negative_int(value: Any) -> int | None:
    try:
        converted = int(value)
    except (TypeError, ValueError):
        return None
    return converted if converted >= 0 else None
