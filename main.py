"""Entry point for the integration daemon."""
from __future__ import annotations

import os
import sys
from contextlib import suppress
from pathlib import Path

from loguru import logger

CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
SRC_DIR = CURRENT_DIR / "src"

if __package__ in {None, ""}:  # support running via `python main.py`
    for path in (PARENT_DIR, SRC_DIR):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

from dotenv import load_dotenv

# 先載入 repo 根目錄/.env（若有），再載入 integration/.env 覆寫
load_dotenv(PARENT_DIR / ".env")
load_dotenv(CURRENT_DIR / ".env", override=True)

from integration.utils.paths import set_core_root

set_core_root(CURRENT_DIR)

from integration.config.settings import AppConfig, load_config
from integration.api.event_store import EdgeEventStore
from integration.api.trajectory_store import TrajectoryStore
from integration.pipeline.pipeline import InitPipelineTask
from integration.pipeline.control.scheduler import PipelineScheduler
from integration.pipeline.control import PhaseTask
from integration.runtime.config_summary import log_config_summary, should_print_config_summary
from integration.runtime.edge_runtime import (
    close_messaging_client,
    init_messaging_client,
    start_edge_event_receiver,
)
from integration.runtime.health_runtime import start_health_server, stop_health_server
from smart_workflow import (
    HealthAwareWorkflowRunner,
    MonitoringClient,
    TaskContext,
    Workflow,
    WorkflowRunner,
)

LOGGER = logger.bind(component=__name__)

LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)
_LOG_LEVELS = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}


def safe_loop_interval_seconds(value: float) -> float:
    """Keep the outer runner from spinning when phase throttling is disabled."""
    try:
        return max(float(value), 0.01)
    except (TypeError, ValueError):
        return 0.01


def setup_logging(level: str = "INFO") -> None:
    level_name = str(level or "INFO").upper()
    if level_name not in _LOG_LEVELS:
        level_name = "INFO"

    logger.remove()
    logger.add(
        sys.stderr,
        level=level_name,
        format=LOG_FORMAT,
        colorize=sys.stderr.isatty(),
        backtrace=False,
        diagnose=False,
    )


def build_context(config: AppConfig) -> TaskContext:
    monitor = MonitoringClient(
        config.monitor_endpoint,
        service_name=config.monitor_service_name,
    )
    context = TaskContext(logger=LOGGER, config=config, monitor=monitor)
    scheduler_cfg = getattr(config, "scheduler", None)
    engine_class = getattr(scheduler_cfg, "engine_class", None) if scheduler_cfg else None
    context.set_resource(
        "scheduler",
        PipelineScheduler(config.working_windows, config.timezone, engine_class, context=context),
    )
    context.set_resource("edge_event_store", EdgeEventStore())
    trajectory_config = config.trajectory_store
    context.set_resource(
        "trajectory_store",
        TrajectoryStore(
            retention_seconds=trajectory_config.retention_seconds,
            max_points=trajectory_config.max_points,
            idle_after_seconds=trajectory_config.idle_after_seconds,
            grace_after_seconds=trajectory_config.grace_after_seconds,
            expire_after_seconds=trajectory_config.expire_after_seconds,
            remove_after_seconds=trajectory_config.remove_after_seconds,
            max_removed_records=trajectory_config.max_removed_records,
        ),
    )
    return context


def build_workflow() -> Workflow:
    workflow = Workflow()
    workflow.add_startup_task(lambda: InitPipelineTask())
    workflow.set_loop(lambda: PhaseTask())
    return workflow


def run_daemon(config: AppConfig, context: TaskContext | None = None) -> None:
    if context is None:
        context = build_context(config)
    init_messaging_client(config, context, LOGGER)

    if should_print_config_summary():
        log_config_summary(config, context, LOGGER)

    store = context.require_resource("edge_event_store")
    start_edge_event_receiver(config, context, store, LOGGER)

    workflow = build_workflow()
    health_server, health_state = start_health_server(context, LOGGER)

    if health_state is not None:
        safe_loop_interval = safe_loop_interval_seconds(config.loop_interval_seconds)
        runner = HealthAwareWorkflowRunner(
            context=context,
            workflow=workflow,
            loop_interval=safe_loop_interval,
            retry_backoff=config.retry_backoff_seconds,
            health_state=health_state,
        )
    else:
        safe_loop_interval = safe_loop_interval_seconds(config.loop_interval_seconds)
        runner = WorkflowRunner(
            context=context,
            workflow=workflow,
            loop_interval=safe_loop_interval,
            retry_backoff=config.retry_backoff_seconds,
        )

    try:
        runner.run()
    finally:
        close_messaging_client(context)
        stop_health_server(health_server)


def main(config: AppConfig | None = None, context: TaskContext | None = None) -> int:
    if config is None:
        config = load_config()
    setup_logging(config.log_level)
    run_daemon(config, context=context)
    return 0


if __name__ == "__main__":
    with suppress(SystemExit):
        sys.exit(main())
