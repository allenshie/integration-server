"""Ingestion stage task."""
from __future__ import annotations

from smart_workflow import BaseTask, TaskContext, TaskResult, TaskError

from .handler import BaseIngestionHandler, DefaultIngestionHandler, IngestionResult, load_ingestion_handler


class IngestionTask(BaseTask):
    name = "ingestion"

    def __init__(self, context: TaskContext | None = None) -> None:
        self._handler = self._init_handler(context)

    def run(self, context: TaskContext) -> TaskResult:
        store = context.require_resource("edge_event_store")
        raw_events = store.pop_all()
        result = self._handler.process(context, raw_events)
        context.set_resource("edge_events", result.events)
        context.logger.info(
            "匯入 %d 台相機的最新事件（原始 %d 筆，丟棄 %d 筆）",
            len(result.events),
            result.raw_count,
            result.dropped,
        )
        return TaskResult(
            status="ingestion_done",
            payload={
                "events": len(result.events),
                "raw": result.raw_count,
                "dropped": result.dropped,
            },
        )

    def _init_handler(self, context: TaskContext | None) -> BaseIngestionHandler:
        cfg = getattr(context.config, "ingestion_task", None) if context else None
        handler_path = getattr(cfg, "handler_class", None) if cfg else None
        if not handler_path:
            return DefaultIngestionHandler(context=context)
        try:
            handler_cls = load_ingestion_handler(handler_path)
        except Exception as exc:  # pylint: disable=broad-except
            raise TaskError(f"無法載入 Ingestion Handler：{handler_path}") from exc
        try:
            return handler_cls(context=context)
        except TypeError:
            try:
                return handler_cls()
            except TypeError as exc:  # pragma: no cover
                raise TaskError(f"Ingestion Handler {handler_path} 無法初始化") from exc
