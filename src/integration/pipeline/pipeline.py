"""Integration pipeline composition and startup tasks."""
from __future__ import annotations

from typing import Iterable, List

from smart_workflow import BaseTask, TaskContext, TaskResult, TaskError

from integration.pipeline.tasks.non_working.update import NonWorkingUpdateTask
from integration.pipeline.tasks.working.ingestion.task import IngestionTask
from integration.pipeline.tasks.working.mc_mot.task import MCMOTTask
from integration.pipeline.tasks.working.pipeline import WorkingPipelineTask
from integration.pipeline.tasks.working.rules.task import RuleEvaluationTask
from integration.pipeline.tasks.working.mc_mot.engine import MCMOTEngine
from integration.mcmot.visualization.map_overlay import GlobalMapRenderer


class WorkingPipeline:
    """Sequential working-hours pipeline that reuses task instances."""

    def __init__(self, nodes: Iterable[BaseTask]) -> None:
        self._nodes: List[BaseTask] = list(nodes)

    def execute(self, context: TaskContext) -> None:
        for node in self._nodes:
            node.execute(context)


class InitPipelineTask(BaseTask):
    """Bootstrap working/non-working tasks and store them in TaskContext."""

    name = "integration-pipeline-init"

    def run(self, context: TaskContext) -> TaskResult:
        if context.config.mcmot_enabled:
            if context.config.mcmot is None:
                raise TaskError("MC-MOT 已啟用但設定未載入")
            engine = MCMOTEngine(config=context.config.mcmot, logger=context.logger)
            context.set_resource("mcmot_engine", engine)
            context.logger.info("MC-MOT engine initialized")

            vis_cfg = getattr(context.config, "global_map_visualization", None)
            if vis_cfg and vis_cfg.enabled:
                map_cfg = context.config.mcmot.map
                if map_cfg is None:
                    context.logger.warning("啟用了全局可視化但 mcmot map 未設定")
                else:
                    renderer = GlobalMapRenderer(
                        map_cfg=map_cfg,
                        vis_cfg=vis_cfg,
                        logger=context.logger,
                        camera_configs=context.config.mcmot.cameras,
                    )
                    context.set_resource("global_map_renderer", renderer)
                    context.logger.info("Global map renderer initialized")
        else:
            context.logger.info("MC-MOT disabled via configuration")

        format_task = self._build_format_task(context)
        rules_task = RuleEvaluationTask(context)
        working_nodes = [
            IngestionTask(context),
            MCMOTTask(context),
        ]
        if format_task:
            working_nodes.append(format_task)
        working_nodes.append(rules_task)
        working_pipeline = WorkingPipelineTask(nodes=working_nodes)
        non_working_task = NonWorkingUpdateTask()

        context.set_resource("working_pipeline", working_pipeline)
        context.set_resource("non_working_task", non_working_task)
        return TaskResult(status="pipeline_initialised")

    def _build_format_task(self, context: TaskContext) -> BaseTask | None:
        cfg = getattr(context.config, "format_task", None)
        enabled = getattr(cfg, "enabled", True)
        if not enabled:
            context.logger.info("FORMAT_TASK_ENABLED=0，略過格式轉換節點")
            return None
        from integration.pipeline.tasks.working.formatting.task import FormatConversionTask  # local import to avoid optional cost

        strategy = getattr(cfg, "strategy_class", None)
        if strategy:
            context.logger.info("使用格式轉換策略：%s", strategy)
        else:
            context.logger.info("使用預設格式轉換策略")
        return FormatConversionTask(context)
