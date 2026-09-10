"""Phase controller task switching working/non-working pipelines."""
from __future__ import annotations

import time

from integration.pipeline.control.phase_engine import BasePhaseEngine, TimeBasedPhaseEngine, load_phase_engine
from integration.pipeline.control.phase_change import (
    BasePhaseChangeEngine,
    DefaultPhaseChangeEngine,
    load_phase_change_engine,
)
from integration.pipeline.control.scheduler import PipelineScheduler
from smart_workflow import BaseTask, TaskContext, TaskResult, TaskError


class PhaseTask(BaseTask):
    name = "phase_controller"

    def __init__(self, context: TaskContext | None = None) -> None:
        self._engine: BasePhaseEngine | None = None
        self._phase_change_engine: BasePhaseChangeEngine | None = None
        self._last_phase: str | None = None
        self._last_publish_time: float = 0.0
        self._last_run_time_by_phase: dict[str, float] = {}

    def execute(self, context: TaskContext) -> TaskResult:
        """Run the phase controller without the default task-start INFO log."""
        try:
            result = self.run(context)
        except TaskError as exc:
            context.report_failure(self.name, detail=str(exc))
            raise
        except Exception as exc:  # noqa: BLE001
            context.report_failure(self.name, detail=str(exc))
            raise

        result = result or TaskResult()
        context.report_success(self.name)
        return result

    def run(self, context: TaskContext) -> TaskResult:
        state = self._load_state(context)
        last_phase = state["last_phase"]
        last_publish_time = state["last_publish_time"]
        last_run_time_by_phase = state["last_run_time_by_phase"]

        # 1) 初始化 phase engine（僅第一次）
        engine = context.get_resource("phase_task_engine")
        if engine is None:
            if self._engine is None:
                self._engine = self._init_engine(context)
            engine = self._engine
            context.set_resource("phase_task_engine", engine)
        self._engine = engine

        # 2) 解析目前工作階段（phase）
        if self._engine:
            phase = self._engine.resolve(context)
        else:
            scheduler: PipelineScheduler = context.require_resource("scheduler")
            phase = scheduler.current_phase()
            
        # 3) 回報 heartbeat、必要時推播 phase 變更
        context.monitor.heartbeat(phase=phase.name)
        publish_cfg = getattr(context.config, "phase_messaging", None)
        broadcast_enabled = getattr(publish_cfg, "enabled", True) if publish_cfg else True
        publish_backend = (getattr(publish_cfg, "backend", None) or "mqtt").strip().lower()
        heartbeat_seconds = getattr(publish_cfg, "heartbeat_seconds", 0) if publish_cfg else 0
        changed, heartbeat_due = self._phase_change_flags(
            phase.name,
            heartbeat_seconds,
            last_phase,
            last_publish_time,
        )
        message = (
            f"phase task: phase={phase.name} changed={changed} "
            f"heartbeat_due={heartbeat_due} broadcast_enabled={broadcast_enabled}"
        )
        if changed or heartbeat_due:
            context.logger.info(message)
        else:
            context.logger.debug(message)
        self._maybe_notify_phase_change(context, phase.name, changed, last_phase)
        self._maybe_publish_phase(
            context,
            phase.name,
            changed,
            heartbeat_due,
            broadcast_enabled,
            publish_backend,
            state,
        )
        state["last_phase"] = phase.name

        try:
            # 4) 根據 phase 的 interval_seconds 進行節流判斷
            phase_policies = context.get_resource("pipeline_policies") or {}
            policy = phase_policies.get(phase.name)
            if policy is not None and policy.enabled:
                now = time.time()
                last_run = last_run_time_by_phase.get(phase.name, 0.0)
                if (now - last_run) < policy.interval:
                    return TaskResult(status="phase_skipped", payload={"phase": phase.name})

            # 5) 依 phase 取得對應 pipeline 並執行
            pipeline_registry = context.get_resource("pipeline_registry") or {}
            pipeline = pipeline_registry.get(phase.name)
            if pipeline:
                pipeline.execute(context)
                last_run_time_by_phase[phase.name] = time.time()
                return TaskResult(status="phase_pipeline", payload={"phase": phase.name})
            raise TaskError(f"phase {phase.name} 未設定對應 pipeline")
        finally:
            self._cleanup_context(context)

    def _init_engine(self, context: TaskContext | None) -> BasePhaseEngine:
        cfg = getattr(context.config, "phase_task", None) if context else None
        engine_path = getattr(cfg, "engine_class", None) if cfg else None
        if not engine_path:
            return TimeBasedPhaseEngine(context=context)
        try:
            engine_cls = load_phase_engine(engine_path)
        except Exception as exc:  # pylint: disable=broad-except
            raise TaskError(f"無法載入 Phase Engine：{engine_path}") from exc
        try:
            return engine_cls(context=context)
        except TypeError:
            try:
                return engine_cls()
            except TypeError as exc:  # pragma: no cover
                raise TaskError(f"Phase Engine {engine_path} 無法初始化") from exc

    def _init_phase_change_engine(self, context: TaskContext | None) -> BasePhaseChangeEngine:
        cfg = getattr(context.config, "phase_change", None) if context else None
        engine_path = getattr(cfg, "engine_class", None) if cfg else None
        if not engine_path:
            return DefaultPhaseChangeEngine(context=context)
        try:
            engine_cls = load_phase_change_engine(engine_path)
        except Exception as exc:  # pylint: disable=broad-except
            raise TaskError(f"無法載入 PhaseChange Engine：{engine_path}") from exc
        try:
            return engine_cls(context=context)
        except TypeError:
            try:
                return engine_cls()
            except TypeError as exc:  # pragma: no cover
                raise TaskError(f"PhaseChange Engine {engine_path} 無法初始化") from exc

    def _phase_change_flags(
        self,
        phase_name: str,
        heartbeat_seconds: int,
        last_phase: str | None,
        last_publish_time: float,
    ) -> tuple[bool, bool]:
        changed = phase_name != last_phase
        heartbeat_due = False
        if not changed and heartbeat_seconds > 0 and last_publish_time > 0:
            now = time.time()
            heartbeat_due = (now - last_publish_time) >= heartbeat_seconds
        return changed, heartbeat_due

    def _maybe_notify_phase_change(
        self,
        context: TaskContext,
        phase_name: str,
        changed: bool,
        last_phase: str | None,
    ) -> None:
        if not changed:
            return
        phase_change_engine = context.get_resource("phase_task_change_engine")
        if phase_change_engine is None:
            phase_change_engine = self._phase_change_engine or self._init_phase_change_engine(context)
            context.set_resource("phase_task_change_engine", phase_change_engine)
        self._phase_change_engine = phase_change_engine
        phase_change_engine.on_phase_change(last_phase, phase_name, context)

    def _maybe_publish_phase(
        self,
        context: TaskContext,
        phase_name: str,
        changed: bool,
        heartbeat_due: bool,
        broadcast_enabled: bool,
        publish_backend: str,
        state: dict[str, object],
    ) -> None:
        if not broadcast_enabled:
            return

        if not changed and not heartbeat_due:
            return

        now = time.time()
        state["last_publish_time"] = now
        messaging = context.get_resource("messaging_client")
        if messaging is None:
            context.logger.warning("phase publish skipped: messaging_client not ready")
            return

        try:
            published = messaging.publish(
                "phase_publish",
                {"phase": phase_name, "timestamp": now},
            )
        except Exception as exc:  # pylint: disable=broad-except
            context.logger.warning(
                f"phase publish skipped (backend={publish_backend}): {exc}"
            )
            return

        if not published:
            context.logger.warning(
                f"phase publish failed: backend={publish_backend} phase={phase_name}"
            )

    def _load_state(self, context: TaskContext) -> dict[str, object]:
        state = context.get_resource("phase_task_state")
        if not isinstance(state, dict):
            state = {}
        if "last_phase" not in state:
            state["last_phase"] = None
        if "last_publish_time" not in state:
            state["last_publish_time"] = 0.0
        if "last_run_time_by_phase" not in state or not isinstance(
            state.get("last_run_time_by_phase"), dict
        ):
            state["last_run_time_by_phase"] = {}
        context.set_resource("phase_task_state", state)
        return state

    def _cleanup_context(self, context: TaskContext) -> None:
        for key in self._ephemeral_context_keys():
            context.set_resource(key, None)

    @staticmethod
    def _ephemeral_context_keys() -> tuple[str, ...]:
        return (
            "edge_events",
            "pipeline_has_new_data",
            "pipeline_dirty_camera_ids",
            "mc_mot_tracked",
            "mc_mot_global_objects",
            "global_map_snapshot",
            "rules_payload",
            "rule_events",
            "warehouse_modeling_task_list",
            "warehouse_modeling_status",
            "warehouse_modeling_should_create",
            "warehouse_modeling_mock_published",
        )
