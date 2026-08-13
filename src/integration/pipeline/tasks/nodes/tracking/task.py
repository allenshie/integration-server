"""MC-MOT integration stage."""
from __future__ import annotations

from smart_workflow import TaskContext, TaskResult

from integration.pipeline.tasks.base import QuietTaskBase
from integration.pipeline.tasks.summary import MC_MOT_STATS_RESOURCE, store_stage_stats
from integration.pipeline.tasks.nodes.tracking.engine import MCMOTEngine
from integration.visualization import GlobalMapRenderer, OverlayResult


class MCMOTTask(QuietTaskBase):
    name = "mc_mot"

    def __init__(self, context: TaskContext | None = None) -> None:
        self._engine: MCMOTEngine | None = None

    def run(self, context: TaskContext) -> TaskResult:
        events = list(context.get_resource("edge_events") or [])
        processed_events = len(events)
        if not context.config.mcmot_enabled:
            store_stage_stats(
                context,
                MC_MOT_STATS_RESOURCE,
                {
                    "events": processed_events,
                    "tracked": 0,
                    "global": 0,
                },
            )
            context.logger.debug("MC-MOT 已停用，略過 %d 筆事件", processed_events)
            context.set_resource("mc_mot_tracked", [])
            context.set_resource("mc_mot_global_objects", [])
            return TaskResult(status="mc_mot_skipped")

        if self._engine is None:
            self._engine = self._init_engine(context)
        context.set_resource("mcmot_engine", self._engine)
        self._ensure_global_map_renderer(context)

        result = self._engine.process_events(events)
        context.set_resource("mc_mot_tracked", result.tracked_objects)
        context.set_resource("mc_mot_global_objects", result.global_objects)
        store_stage_stats(
            context,
            MC_MOT_STATS_RESOURCE,
            {
                "events": processed_events,
                "tracked": len(result.tracked_objects),
                "global": len(result.global_objects),
            },
        )

        self._maybe_render_global_map(context, result.global_objects, result.tracked_objects)

        context.logger.debug(
            "MC-MOT 處理 %d 筆事件，產生 %d 筆追蹤結果，維護 %d 筆全域物件",
            processed_events,
            len(result.tracked_objects),
            len(result.global_objects),
        )
        return TaskResult(
            status="mc_mot_done",
            payload={
                "events": processed_events,
                "tracked": len(result.tracked_objects),
                "global_objects": len(result.global_objects),
            },
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
            context.logger.warning("全局地圖可視化失敗：%s", exc)

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
