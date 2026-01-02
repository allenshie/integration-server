"""Entry point for the integration daemon."""
from __future__ import annotations

import logging
import os
import sys
import threading
from contextlib import suppress
from pathlib import Path

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
from integration.api.http_server import start_edge_event_server
from integration.pipeline.pipeline import InitPipelineTask
from integration.pipeline.scheduler import PipelineScheduler
from integration.pipeline.tasks import PhaseTask
from integration.storage.state import ZoneStateRepository
from smart_workflow import MonitoringClient, TaskContext, Workflow, WorkflowRunner

LOGGER = logging.getLogger(__name__)

def setup_logging(level: str = "INFO") -> None:
    level_value = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=level_value,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def build_context(config: AppConfig) -> TaskContext:
    monitor = MonitoringClient(
        config.monitor_endpoint,
        service_name=config.monitor_service_name,
    )
    context = TaskContext(logger=LOGGER, config=config, monitor=monitor)
    context.set_resource("scheduler", PipelineScheduler(config.working_windows, config.timezone))
    context.set_resource("zone_repo", ZoneStateRepository())
    context.set_resource("edge_event_store", EdgeEventStore())
    return context



def build_workflow() -> Workflow:
    workflow = Workflow()
    workflow.add_startup_task(lambda: InitPipelineTask())
    workflow.set_loop(lambda: PhaseTask())
    return workflow


def run_daemon(config: AppConfig) -> None:
    context = build_context(config)
    store = context.require_resource("edge_event_store")
    server = start_edge_event_server(config.edge_event_host, config.edge_event_port, store)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    workflow = build_workflow()
    runner = WorkflowRunner(
        context=context,
        workflow=workflow,
        loop_interval=config.loop_interval_seconds,
        retry_backoff=config.retry_backoff_seconds,
    )
    runner.run()


def main() -> int:
    config = load_config()
    setup_logging(config.log_level)
    run_daemon(config)
    return 0


if __name__ == "__main__":
    with suppress(SystemExit):
        sys.exit(main())
