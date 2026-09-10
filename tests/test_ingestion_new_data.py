from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from smart_workflow import TaskResult

from integration.pipeline.tasks.nodes.ingestion.engine import DefaultIngestionEngine
from integration.pipeline.tasks.nodes.ingestion.task import IngestionTask
from integration.pipeline.tasks.pipelines.mcmot_pipeline import MCMOTPipelineTask


class DummyContext:
    def __init__(self, resources: dict[str, object] | None = None) -> None:
        self._resources = dict(resources or {})
        self.config = SimpleNamespace(
            edge_events=SimpleNamespace(max_age_seconds=60.0),
            edge_event_max_age_seconds=60.0,
            pipeline_summary_interval_seconds=60.0,
        )
        self.logger = logging.getLogger("ingestion-new-data-test")
        self.reported_success: list[str] = []
        self.reported_failure: list[tuple[str, str | None]] = []

    def get_resource(self, key: str):
        return self._resources.get(key)

    def set_resource(self, key: str, value) -> None:  # noqa: ANN001
        self._resources[key] = value

    def require_resource(self, key: str):
        if key not in self._resources:
            raise KeyError(key)
        return self._resources[key]

    def report_success(self, name: str) -> None:
        self.reported_success.append(name)

    def report_failure(self, name: str, detail: str | None = None) -> None:
        self.reported_failure.append((name, detail))


class _Store:
    def __init__(self, batches: list[list[dict[str, object]]]) -> None:
        self._batches = list(batches)

    def pop_all(self) -> list[dict[str, object]]:
        if not self._batches:
            return []
        return self._batches.pop(0)


class _TrajectoryMaintenanceStore:
    def __init__(self) -> None:
        self.maintenance_calls = 0
        self.append_calls = 0

    def append_edge_events(self, events):
        self.append_calls += 1
        return len(events)

    def maintenance(self):
        self.maintenance_calls += 1


class _RecordingNode:
    def __init__(self, result: TaskResult) -> None:
        self._result = result
        self.calls = 0

    def execute(self, context: DummyContext) -> TaskResult:
        _ = context
        self.calls += 1
        return self._result


def _edge_event(
    *,
    camera_id: str,
    session_id: str,
    frame_seq: int,
    capture_ts: datetime,
    publish_offset_ms: int = 20,
) -> dict[str, object]:
    return {
        "camera_id": camera_id,
        "session_id": session_id,
        "frame_seq": frame_seq,
        "capture_ts": capture_ts.isoformat(),
        "timestamp": (capture_ts + timedelta(milliseconds=publish_offset_ms)).isoformat(),
        "detections": [
            {
                "track_id": frame_seq,
                "class_name": "person",
                "bbox": [1, 2, 3, 4],
                "bbox_confidence_score": 0.9,
            }
        ],
        "models": ["detect"],
    }


def test_default_ingestion_engine_detects_new_frames_per_camera() -> None:
    context = DummyContext()
    engine = DefaultIngestionEngine(context=context)
    capture_1 = datetime.now(timezone.utc) - timedelta(seconds=2)
    capture_2 = capture_1 + timedelta(seconds=1)

    first = engine.process(
        context,
        [
            _edge_event(camera_id="cam-01", session_id="sess-a", frame_seq=1, capture_ts=capture_1),
            _edge_event(camera_id="cam-02", session_id="sess-b", frame_seq=3, capture_ts=capture_2),
        ],
    )

    assert first.has_new_data is True
    assert first.dirty_camera_ids == ["cam-01", "cam-02"]
    assert [event["camera_id"] for event in first.events] == ["cam-01", "cam-02"]
    assert [event["frame_seq"] for event in first.events] == [1, 3]

    second = engine.process(
        context,
        [
            _edge_event(camera_id="cam-01", session_id="sess-a", frame_seq=1, capture_ts=capture_1),
            _edge_event(camera_id="cam-02", session_id="sess-b", frame_seq=4, capture_ts=capture_2 + timedelta(seconds=1)),
        ],
    )

    assert second.has_new_data is True
    assert second.dirty_camera_ids == ["cam-02"]
    assert second.duplicate_count == 1
    assert [event["camera_id"] for event in second.events] == ["cam-02"]
    assert second.events[0]["frame_seq"] == 4


def test_default_ingestion_engine_reports_event_filter_reasons() -> None:
    context = DummyContext()
    engine = DefaultIngestionEngine(context=context)
    now = datetime.now(timezone.utc)

    result = engine.process(
        context,
        [
            {"timestamp": now.isoformat(), "detections": []},
            {"camera_id": "cam-01", "detections": []},
            {"camera_id": "cam-01", "timestamp": "not-a-timestamp", "detections": []},
            {
                "camera_id": "cam-01",
                "timestamp": (now - timedelta(seconds=120)).isoformat(),
                "detections": [],
            },
            "not-a-mapping",
        ],
    )

    assert result.events == []
    assert result.dropped == 5
    assert result.drop_reasons == {
        "missing_camera_id": 1,
        "missing_timestamp": 1,
        "invalid_timestamp": 1,
        "stale_event": 1,
        "invalid_event_schema": 1,
    }


def test_ingestion_dedup_cache_is_bounded() -> None:
    context = DummyContext()
    context.config.edge_events = SimpleNamespace(
        max_age_seconds=60.0,
        dedup_ttl_seconds=60.0,
        dedup_max_entries=2,
    )
    engine = DefaultIngestionEngine(context=context)
    capture = datetime.now(timezone.utc) - timedelta(seconds=2)

    result = engine.process(
        context,
        [
            _edge_event(camera_id="cam-01", session_id="sess-a", frame_seq=1, capture_ts=capture),
            _edge_event(camera_id="cam-01", session_id="sess-a", frame_seq=2, capture_ts=capture + timedelta(milliseconds=1)),
            _edge_event(camera_id="cam-01", session_id="sess-a", frame_seq=3, capture_ts=capture + timedelta(milliseconds=2)),
        ],
    )

    assert result.has_new_data is True
    assert len(engine._seen_event_identities) == 2

    # Frame 1 was evicted by the bounded cache and is therefore accepted as
    # a new event rather than being retained forever as dedup history.
    repeated = engine.process(
        context,
        [_edge_event(camera_id="cam-01", session_id="sess-a", frame_seq=1, capture_ts=capture)],
    )
    assert repeated.duplicate_count == 0
    assert repeated.events[0]["frame_seq"] == 1


def test_ingestion_task_preserves_latest_snapshot_when_batch_is_duplicate() -> None:
    capture_ts = datetime.now(timezone.utc) - timedelta(seconds=2)
    event = _edge_event(camera_id="cam-01", session_id="sess-a", frame_seq=7, capture_ts=capture_ts)
    context = DummyContext(
        resources={
            "edge_event_store": _Store([[event], [event]]),
        }
    )
    task = IngestionTask(context)

    first_result = task.run(context)
    first_snapshot = context.get_resource("edge_events_latest")

    second_result = task.run(context)

    assert first_result.payload["has_new_data"] is True
    assert first_snapshot is not None
    assert len(first_snapshot) == 1
    assert first_snapshot[0]["camera_id"] == "cam-01"
    assert first_snapshot[0]["session_id"] == "sess-a"
    assert first_snapshot[0]["frame_seq"] == 7
    assert second_result.payload["has_new_data"] is False
    assert second_result.payload["duplicates"] == 1
    assert context.get_resource("edge_events_latest") == first_snapshot
    assert context.get_resource("pipeline_has_new_data") is False
    assert context.get_resource("pipeline_dirty_camera_ids") == []


def test_ingestion_task_runs_trajectory_maintenance_without_new_events() -> None:
    trajectory_store = _TrajectoryMaintenanceStore()
    context = DummyContext(
        resources={
            "edge_event_store": _Store([[]]),
            "trajectory_store": trajectory_store,
        }
    )

    result = IngestionTask(context).run(context)

    assert result.status == "ingestion_done"
    assert trajectory_store.append_calls == 0
    assert trajectory_store.maintenance_calls == 1


def test_mcmot_pipeline_skips_followup_nodes_when_ingestion_has_no_new_data() -> None:
    context = DummyContext()
    ingestion = _RecordingNode(TaskResult(status="ingestion_done", payload={"has_new_data": False}))
    second = _RecordingNode(TaskResult(status="mc_mot_done"))
    third = _RecordingNode(TaskResult(status="rules_done"))
    pipeline = MCMOTPipelineTask(context, nodes=[ingestion, second, third])

    result = pipeline.run(context)

    assert result.status == "mcmot_pipeline_skipped"
    assert ingestion.calls == 1
    assert second.calls == 0
    assert third.calls == 0
