"""Integration pipeline composition and startup tasks."""
from __future__ import annotations

<<<<<<< HEAD
import os

from smart_workflow import BaseTask, TaskContext, TaskResult, TaskError

from integration.pipeline.schedule import load_pipeline_schedule, load_task_class
=======
import inspect
from importlib import import_module

from smart_workflow import BaseTask, TaskContext, TaskResult, TaskError

from integration.pipeline.events import get_event_queue
from integration.pipeline.registry import PipelineRegistry
from integration.pipeline.selectors.base import BasePipelineSelector, load_selector_class
from integration.pipeline.selectors.default import WorkingHoursSelector
from integration.pipeline.tasks.working.pipeline import WorkingPipelineTask
from integration.pipeline.tasks.working.mc_mot.engine import MCMOTEngine
from integration.mcmot.visualization.map_overlay import GlobalMapRenderer
>>>>>>> af0d02a71a2e4d67914435c97c22deb90ef8dc66


class InitPipelineTask(BaseTask):
    """Bootstrap working pipeline(s) and store them in TaskContext."""

    name = "integration-pipeline-init"

    def run(self, context: TaskContext) -> TaskResult:
<<<<<<< HEAD
        pipeline_registry = self._build_pipeline_registry(context)
        context.set_resource("pipeline_registry", pipeline_registry)
        if os.getenv("CONFIG_SUMMARY", "").strip().lower() in {"1", "true", "yes"}:
            context.logger.info(self._format_pipeline_summary(pipeline_registry, context))
        return TaskResult(status="pipeline_initialised", payload={"pipelines": list(pipeline_registry.keys())})

    def _build_pipeline_registry(self, context: TaskContext) -> dict[str, BaseTask] | None:
        schedule_path = getattr(context.config, "pipeline_schedule_path", None)
        if not schedule_path:
            raise TaskError("PIPELINE_SCHEDULE_PATH 未設定")
        pipelines, phases = load_pipeline_schedule(schedule_path)
        pipeline_instances: dict[str, BaseTask] = {}
        for name, spec in pipelines.items():
            if spec.enabled_env and os.getenv(spec.enabled_env, "").strip().lower() in {"0", "false", "no", "off"}:
                continue
            pipeline_cls = load_task_class(spec.class_path)
            kwargs = dict(spec.kwargs)
            if "context" not in kwargs:
                try:
                    pipeline = pipeline_cls(context=context, **kwargs)
                except TypeError:
                    pipeline = pipeline_cls(**kwargs)
            else:
                pipeline = pipeline_cls(**kwargs)
            pipeline_instances[name] = pipeline

        registry: dict[str, BaseTask] = {}
        for phase_name, pipeline_name in phases.items():
            pipeline = pipeline_instances.get(pipeline_name)
            if not pipeline:
                raise TaskError(f"phase {phase_name} 找不到 pipeline: {pipeline_name}")
            registry[phase_name] = pipeline
        return registry

    def _format_pipeline_summary(self, registry: dict[str, BaseTask], context: TaskContext) -> str:
        lines = ["pipeline registry summary:"]
        for phase, pipeline in registry.items():
            pipeline_name = pipeline.__class__.__name__
            nodes = self._describe_nodes(pipeline, context)
            lines.append(f"- phase={phase} pipeline={pipeline_name}")
            if nodes:
                lines.append(f"  flow: {nodes}")
        return "\n".join(lines)

    def _describe_nodes(self, pipeline: BaseTask, context: TaskContext) -> str:
        nodes = []
        pipeline_nodes = getattr(pipeline, "pipeline_nodes", None)
        if pipeline_nodes is None:
            pipeline_nodes = getattr(pipeline, "_nodes", None)
        if not pipeline_nodes:
            return ""
        for node in pipeline_nodes:
            nodes.append(self._describe_node(node, context))
        return " -> ".join(nodes)

    def _describe_node(self, node: BaseTask, context: TaskContext) -> str:
        parts = [node.__class__.__name__]
        handler = getattr(node, "_handler", None)
        if handler is not None:
            parts.append(f"handler={handler.__class__.__name__}")
        strategy = getattr(node, "_strategy", None)
        if strategy is not None:
            parts.append(f"strategy={strategy.__class__.__name__}")
        engine = getattr(node, "_engine", None)
        if engine is not None:
            parts.append(f"engine={engine.__class__.__name__}")
        if node.__class__.__name__ == "MCMOTTask":
            mcmot_engine = context.get_resource("mcmot_engine")
            if mcmot_engine is not None:
                parts.append(f"engine={mcmot_engine.__class__.__name__}")
        return f"{parts[0]}({', '.join(parts[1:])})" if len(parts) > 1 else parts[0]
=======
        self._init_mcmot_resources(context)
        get_event_queue(context)  # ensure queue resource exists

        pipeline_classes = {"working": WorkingPipelineTask}
        pipeline_classes.update(
            {name: self._load_pipeline_attr(path) for name, path in context.config.pipeline.task_classes.items()}
        )

        sleep_map = dict(context.config.pipeline.sleep_seconds)
        registry = PipelineRegistry()
        for name, pipeline_cls in pipeline_classes.items():
            default_sleep = sleep_map.get(name)
            if default_sleep is None and name == "working":
                default_sleep = context.config.loop_interval_seconds
            if default_sleep is None and name == "off_hours" and context.config.non_working_idle_seconds:
                # 保留向後相容：若子專案未設定 sleep，可沿用 NON_WORKING_IDLE_SECONDS
                default_sleep = context.config.non_working_idle_seconds
            registry.register(name, self._instantiate_pipeline(pipeline_cls, context), default_sleep=default_sleep)

        context.set_resource("pipeline_registry", registry)
        selector = self._build_selector(context)
        context.set_resource("pipeline_selector", selector)
        context.logger.info("已載入 pipelines：%s", ", ".join(registry.names()))
        return TaskResult(status="pipeline_initialised")

    def _load_pipeline_attr(self, class_path: str):
        attr = self._import_attr(class_path)
        if inspect.isclass(attr) and issubclass(attr, BaseTask):
            return attr
        if callable(attr):
            return attr
        raise TaskError(f"Pipeline {class_path} 必須是 BaseTask 子類或可呼叫工廠")

    def _build_selector(self, context: TaskContext) -> BasePipelineSelector:
        selector_path = context.config.pipeline.selector_class
        if selector_path:
            selector_cls = load_selector_class(selector_path)
            return selector_cls(context)
        return WorkingHoursSelector(context)

    def _import_attr(self, path: str):
        if ":" in path:
            module_name, attr_name = path.split(":", 1)
        elif "." in path:
            module_name, attr_name = path.rsplit(".", 1)
        else:
            raise TaskError(f"無法解析 Pipeline 類別路徑：{path}")

        module = import_module(module_name)
        attr = getattr(module, attr_name, None)
        if attr is None:
            raise TaskError(f"在模組 {module_name} 找不到 {attr_name}")
        return attr

    def _instantiate_pipeline(self, pipeline_cls, context: TaskContext) -> BaseTask:
        if inspect.isclass(pipeline_cls) and issubclass(pipeline_cls, BaseTask):
            try:
                return pipeline_cls(context)
            except TypeError:
                return pipeline_cls()
        if callable(pipeline_cls):
            return pipeline_cls(context)
        raise TaskError("Pipeline 類別/工廠無法被實例化")

    def _init_mcmot_resources(self, context: TaskContext) -> None:
        if not context.config.mcmot_enabled:
            context.logger.info("MC-MOT disabled via configuration")
            return

        if context.config.mcmot is None:
            raise TaskError("MC-MOT 已啟用但設定未載入")

        engine = MCMOTEngine(config=context.config.mcmot, logger=context.logger)
        context.set_resource("mcmot_engine", engine)
        context.logger.info("MC-MOT engine initialized")

        vis_cfg = getattr(context.config, "global_map_visualization", None)
        if not (vis_cfg and vis_cfg.enabled):
            return

        map_cfg = context.config.mcmot.map
        if map_cfg is None:
            context.logger.warning("啟用了全局可視化但 mcmot map 未設定")
            return

        renderer = GlobalMapRenderer(
            map_cfg=map_cfg,
            vis_cfg=vis_cfg,
            logger=context.logger,
            camera_configs=context.config.mcmot.cameras,
        )
        context.set_resource("global_map_renderer", renderer)
        context.logger.info("Global map renderer initialized")
>>>>>>> af0d02a71a2e4d67914435c97c22deb90ef8dc66
