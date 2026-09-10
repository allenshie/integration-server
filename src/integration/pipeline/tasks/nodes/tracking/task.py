"""MC-MOT integration stage."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from smart_workflow import TaskContext, TaskResult

from integration.pipeline.tasks.base import QuietTaskBase
from integration.pipeline.tasks.summary import (
    MC_MOT_STATS_RESOURCE,
    MCMOT_STATE_RESOURCE,
    store_stage_stats,
)
from integration.pipeline.tasks.nodes.tracking.engine import MCMOTEngine, MCMOTResult
from integration.visualization import GlobalMapRenderer, OverlayResult


class MCMOTTask(QuietTaskBase):
    name = "mc_mot"

    def __init__(self, context: TaskContext | None = None) -> None:
        self._engine: MCMOTEngine | None = None

    def run(self, context: TaskContext) -> TaskResult:
        events = list(context.get_resource("edge_events") or [])
        processed_events = len(events)
        if not context.config.mcmot_enabled:
            result = MCMOTResult(
                tracked_objects=[],
                global_objects=[],
                success=True,
                matching_performed=False,
                reason="mcmot_disabled",
            )
            store_stage_stats(
                context,
                MC_MOT_STATS_RESOURCE,
                {
                    "events": processed_events,
                    "tracked": 0,
                    "global": 0,
                    "input_count": None,
                    "input_unit": "snapshots",
                    "result_count": None,
                    "result_unit": "tracked",
                    "attempts": 0,
                    "skipped": 0,
                    "failed": 0,
                    "active_global": 0,
                },
            )
            self._update_mcmot_state(context, result, matching_at=None)
            context.logger.debug(f"MC-MOT 已停用，略過 {processed_events} 筆事件")
            context.set_resource("mc_mot_tracked", [])
            context.set_resource("mc_mot_global_objects", [])
            return TaskResult(
                status="mc_mot_skipped",
                payload={
                    "events": processed_events,
                    "snapshot_objects": None,
                    "tracked": 0,
                    "global_objects": 0,
                    "success": True,
                    "matching_performed": False,
                    "reason": "mcmot_disabled",
                },
            )

        if self._engine is None:
            self._engine = self._init_engine(context)
        context.set_resource("mcmot_engine", self._engine)
        self._ensure_global_map_renderer(context)

        trajectory_store = context.get_resource("trajectory_store")
        snapshot_object_count: int | None = None
        input_unit = "snapshots"
        matching_at: datetime | None = None
        matching_due: bool | None = None
        if trajectory_store is not None:
            # Task 只負責觸發與資料連接；cadence、重試與 watermark
            # 仍由 MCMOT 管理，未到期時只執行 lifecycle maintenance。
            matching_due = self._engine.is_matching_due()
        if trajectory_store is not None:
            assert matching_due is not None
            if not matching_due:
                self._engine.maintain()
                result = self._engine_result_for_skipped_cycle()
            else:
                snapshot = trajectory_store.snapshot_since(self._engine.last_successful_watermark)
                snapshot_object_count = len(snapshot.objects)
                matching_at = datetime.now(timezone.utc)
                try:
                    result = self._engine.process_trajectory_snapshot(snapshot)
                except Exception as exc:
                    self._record_matching_exception(
                        context,
                        processed_events=processed_events,
                        snapshot_object_count=snapshot_object_count,
                        input_unit=input_unit,
                        reason=_failure_reason(exc),
                    )
                    raise
                if (
                    result.success
                    and result.matching_performed
                    and result.committed_watermark is not None
                    and result.committed_watermark <= snapshot.high_watermark
                ):
                    try:
                        trajectory_store.ack(result.committed_watermark)
                    except Exception:
                        self._record_matching_exception(
                            context,
                            processed_events=processed_events,
                            snapshot_object_count=snapshot_object_count,
                            input_unit=input_unit,
                            reason="commit_failed",
                            matching_at=matching_at,
                        )
                        raise
        else:
            input_unit = "legacy_events"
            snapshot_object_count = processed_events
            matching_at = datetime.now(timezone.utc)
            result = self._engine.process_events(events)
        context.set_resource("mc_mot_tracked", result.tracked_objects)
        context.set_resource("mc_mot_global_objects", result.global_objects)
        self._store_cycle_stats(
            context,
            processed_events=processed_events,
            snapshot_object_count=snapshot_object_count,
            input_unit=input_unit,
            result=result,
            matching_at=matching_at,
        )

        self._maybe_render_global_map(context, result.global_objects, result.tracked_objects)

        context.logger.debug(
            f"MC-MOT cycle：edge_events={processed_events}、"
            f"snapshot_objects={snapshot_object_count if result.matching_performed else '-'}、"
            f"tracked={len(result.tracked_objects)}、active_global={len(result.global_objects)}",
        )
        payload = {
            "events": processed_events,
            "snapshot_objects": snapshot_object_count if result.matching_performed else None,
            "tracked": len(result.tracked_objects),
            "global_objects": len(result.global_objects),
        }
        if trajectory_store is not None:
            payload.update(
                {
                    "success": result.success,
                    "matching_performed": result.matching_performed,
                    "committed_watermark": result.committed_watermark,
                    "reason": _matching_reason(result),
                }
            )
        return TaskResult(status="mc_mot_done" if result.success else "mc_mot_failed", payload=payload)

    def _store_cycle_stats(
        self,
        context: TaskContext,
        *,
        processed_events: int,
        snapshot_object_count: int | None,
        input_unit: str,
        result: MCMOTResult,
        matching_at: datetime | None,
        failure_reason: str | None = None,
    ) -> None:
        tracked_count = len(result.tracked_objects)
        global_count = len(result.global_objects)
        matching_performed = bool(result.matching_performed)
        reason = failure_reason or _matching_reason(result)
        attempts = 1 if matching_performed else 0
        cadence_skipped = 1 if reason == "cadence_not_due" else 0
        failed = 0 if result.success else 1
        store_stage_stats(
            context,
            MC_MOT_STATS_RESOURCE,
            {
                # Keep legacy fields for existing consumers, but summary
                # renders the typed input_count/result_count fields below.
                "events": processed_events,
                "tracked": tracked_count,
                "global": global_count,
                "matching_performed": matching_performed,
                "committed_watermark": result.committed_watermark,
                "snapshot_objects": snapshot_object_count,
                "input_count": snapshot_object_count if matching_performed else None,
                "input_unit": input_unit,
                "result_count": tracked_count if matching_performed else None,
                "result_unit": "tracked",
                "attempts": attempts,
                "skipped": cadence_skipped,
                "failed": failed,
                "active_global": global_count,
            },
        )
        self._update_mcmot_state(context, result, matching_at=matching_at, reason=reason)

    def _record_matching_exception(
        self,
        context: TaskContext,
        *,
        processed_events: int,
        snapshot_object_count: int,
        input_unit: str,
        reason: str,
        result: MCMOTResult | None = None,
        matching_at: datetime | None = None,
    ) -> None:
        if result is None:
            previous_globals = list(context.get_resource("mc_mot_global_objects") or [])
            result = MCMOTResult(
                tracked_objects=[],
                global_objects=previous_globals,
                success=False,
                matching_performed=True,
                committed_watermark=self._engine.last_successful_watermark,
                reason=reason,
            )
        self._store_cycle_stats(
            context,
            processed_events=processed_events,
            snapshot_object_count=snapshot_object_count,
            input_unit=input_unit,
            result=result,
            matching_at=matching_at or datetime.now(timezone.utc),
            failure_reason=reason,
        )
    def _update_mcmot_state(
        self,
        context: TaskContext,
        result: MCMOTResult,
        *,
        matching_at: datetime | None,
        reason: str | None = None,
    ) -> None:
        state = context.get_resource(MCMOT_STATE_RESOURCE)
        state = dict(state) if isinstance(state, dict) else {}
        state["active_global"] = len(result.global_objects)
        state.setdefault("last_successful_watermark", 0)
        if result.success and result.committed_watermark is not None:
            state["last_successful_watermark"] = int(result.committed_watermark)

        if result.matching_performed:
            state["last_matching_at"] = _format_timestamp(matching_at or datetime.now(timezone.utc))
            state["last_matching_status"] = "success" if result.success else "failed"
        else:
            state["last_matching_status"] = "skipped"
        state["last_reason"] = reason or _matching_reason(result)
        context.set_resource(MCMOT_STATE_RESOURCE, state)

    def _engine_result_for_skipped_cycle(self) -> MCMOTResult:
        return MCMOTResult(
            tracked_objects=[],
            global_objects=self._engine.get_all_global_objects(),
            success=True,
            matching_performed=False,
            committed_watermark=self._engine.last_successful_watermark,
            reason="cadence_not_due",
        )

    def _maybe_render_global_map(self, context: TaskContext, global_objects, tracked_objects) -> None:
        renderer = context.get_resource("global_map_renderer")
        if renderer is None:
            return
        try:
            result: OverlayResult | None = renderer.render(global_objects, tracked_objects or [])
            if result and result.image_path:
                context.set_resource("global_map_snapshot", str(result.image_path))
        except Exception as exc:  # pylint: disable=broad-except
            context.logger.warning(f"全局地圖可視化失敗：{exc}")

    def _init_engine(self, context: TaskContext | None) -> MCMOTEngine:
        tracking_config_path = (
            getattr(context.config, "mcmot_tracking_config_path", None) if context else None
        )
        camera_config_path = (
            getattr(context.config, "mcmot_camera_config_path", None) if context else None
        )
        engine = self._init_plugin(
            plugin_name="MC-MOT 引擎",
            plugin_cls=MCMOTEngine,
            init_kwargs={
                "tracking_config": tracking_config_path,
                "camera_config": camera_config_path,
                "logger": context.logger if context else None,
            },
        )
        if context is not None:
            context.logger.info("MC-MOT engine initialized")
        return engine

    def _ensure_global_map_renderer(self, context: TaskContext) -> None:
        if context.get_resource("global_map_renderer") is not None:
            return
        if not self._is_global_map_visualization_enabled(context):
            return
        vis_cfg = getattr(context.config, "global_map_visualization", None)
        if vis_cfg is None:
            context.logger.warning("已啟用全局可視化但未載入視覺化設定")
            return
        renderer = GlobalMapRenderer(
            vis_cfg=vis_cfg,
            logger=context.logger,
        )
        context.set_resource("global_map_renderer", renderer)
        context.logger.info("Global map renderer initialized")

    @staticmethod
    def _is_global_map_visualization_enabled(context: TaskContext) -> bool:
        enabled = getattr(context.config, "global_map_visualization_enabled", None)
        if enabled is not None:
            return bool(enabled)
        vis_cfg = getattr(context.config, "global_map_visualization", None)
        if vis_cfg is None:
            return False
        return bool(getattr(vis_cfg, "enabled", False))

def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _matching_reason(result: MCMOTResult) -> str:
    if not result.success:
        return _failure_reason(result.reason)
    if not result.matching_performed:
        return "cadence_not_due" if result.reason in {None, "matching_not_due"} else str(result.reason)
    if result.reason:
        return str(result.reason)
    if result.committed_watermark is not None:
        return "matched_and_committed"
    return "matching_succeeded"


def _failure_reason(value: Any) -> str:
    text = str(value or "").lower()
    if "schema" in text or "validation" in text:
        return "schema_error"
    if "commit" in text or "watermark" in text or "ack" in text:
        return "commit_failed"
    return "matching_failed"
