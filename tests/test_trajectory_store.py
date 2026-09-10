from __future__ import annotations

from datetime import datetime, timedelta, timezone
import io
import logging
import sys
from types import ModuleType, SimpleNamespace

import pytest
from loguru import logger as loguru_logger

from integration.api.trajectory_store import (
    DetectionEvent,
    TimestampContractError,
    TrajectoryStore,
)
from integration.pipeline.tasks.nodes.ingestion.engine import DefaultIngestionEngine
from integration.pipeline.tasks.nodes.tracking.task import MCMOTTask
from integration.pipeline.tasks.summary import MC_MOT_STATS_RESOURCE, MCMOT_STATE_RESOURCE


def _event(frame_seq: int, capture_ts: datetime, local_id: int = 7) -> dict:
    return {
        "camera_id": "cam-1",
        "session_id": "session-1",
        "frame_seq": frame_seq,
        "capture_ts": capture_ts.isoformat(),
        "timestamp": (capture_ts + timedelta(milliseconds=5)).isoformat(),
        "detections": [
            {
                "track_id": local_id,
                "class_name": "person",
                "score": 0.9,
                "bbox": [frame_seq, 2, frame_seq + 10, 20],
            }
        ],
    }


def test_ingestion_keeps_all_frames_and_store_builds_complete_trajectory():
    base = datetime.now(timezone.utc) - timedelta(seconds=2)
    engine = DefaultIngestionEngine()
    context = type("Context", (), {"config": type("Config", (), {"edge_events": type("E", (), {"max_age_seconds": 60})()})()})()

    result = engine.process(context, [_event(1, base), _event(2, base + timedelta(milliseconds=100))])

    assert [item["frame_seq"] for item in result.events] == [1, 2]
    store = TrajectoryStore(retention=10)
    high = store.append_edge_events(result.events)
    snapshot = store.snapshot_since(0)

    assert high == 2
    assert len(snapshot.objects) == 1
    assert snapshot.objects[0].local_id == "7"
    assert len(snapshot.objects[0].local_trajectory) == 2
    assert snapshot.objects[0].local_trajectory[0][0] == base


def test_edge_events_without_track_id_are_skipped_without_failing_valid_objects():
    base = datetime.now(timezone.utc) - timedelta(seconds=2)
    event = _event(1, base)
    event["detections"].append(
        {
            "track_id": None,
            "class_name": "car",
            "score": 0.8,
            "bbox": [5, 6, 15, 16],
        }
    )
    store = TrajectoryStore()

    output = io.StringIO()
    handler_id = loguru_logger.add(output, level="WARNING", format="{message}")
    try:
        high = store.append_edge_events([event])
    finally:
        loguru_logger.remove(handler_id)

    snapshot = store.snapshot_since(0)
    assert high == 1
    assert len(snapshot.objects) == 1
    assert snapshot.objects[0].local_id == "7"
    assert "skip untracked edge detection" in output.getvalue()
    assert "camera_id=cam-1" in output.getvalue()


def test_edge_events_use_track_id_when_local_id_alias_is_null():
    base = datetime.now(timezone.utc) - timedelta(seconds=2)
    event = _event(1, base)
    event["detections"][0]["local_id"] = None
    store = TrajectoryStore()

    high = store.append_edge_events([event])

    assert high == 1
    assert store.snapshot_since(0).objects[0].local_id == "7"


def test_edge_events_with_only_untracked_detections_do_not_advance_watermark():
    base = datetime.now(timezone.utc) - timedelta(seconds=2)
    event = _event(1, base)
    event["detections"][0]["track_id"] = None
    store = TrajectoryStore()

    output = io.StringIO()
    handler_id = loguru_logger.add(output, level="WARNING", format="{message}")
    try:
        high = store.append_edge_events([event])
    finally:
        loguru_logger.remove(handler_id)

    assert high == 0
    assert store.snapshot_since(0).objects == ()
    assert "skipped untrackable edge detections" in output.getvalue()


def test_snapshot_watermark_ack_and_new_revision_are_safe():
    base = datetime.now(timezone.utc) - timedelta(seconds=2)
    store = TrajectoryStore(retention=10)

    store.append_edge_events([_event(1, base)])
    first = store.snapshot_since(0)
    assert first.high_watermark == 1
    assert len(first.objects) == 1

    store.ack(first.high_watermark)
    assert store.snapshot_since(first.high_watermark).objects == ()

    store.append_edge_events([_event(2, base + timedelta(milliseconds=100))])
    second = store.snapshot_since(first.high_watermark)
    assert second.high_watermark == 2
    assert second.objects[0].revision == 2
    assert len(second.objects[0].local_trajectory) == 2

    # A failed matching attempt does not ack revision 2; retrying from the
    # last successful watermark returns the object again.
    retry = store.snapshot_since(first.high_watermark)
    assert retry.objects[0].revision == 2


def test_store_rejects_naive_timestamp_before_mutating_watermark():
    store = TrajectoryStore()

    with pytest.raises(TimestampContractError, match=r"timestamp") as raised:
        store.append_events(
            [DetectionEvent("cam-a", 1, [0, 0, 10, 10], datetime(2026, 1, 1))]
        )

    assert raised.value.path == "timestamp"
    assert store.snapshot_since(0).high_watermark == 0
    assert store.snapshot_since(0).objects == ()


def test_store_normalizes_offset_aware_timestamps_to_utc():
    source = datetime(2026, 1, 1, 20, 0, tzinfo=timezone(timedelta(hours=8)))
    store = TrajectoryStore()
    store.append_events([DetectionEvent("cam-a", 1, [0, 0, 10, 10], source)])

    obj = store.snapshot_since(0).objects[0]
    expected = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert obj.latest_timestamp == expected
    assert obj.first_seen_at == expected
    assert obj.local_trajectory[0][0] == expected
    assert obj.latest_timestamp.tzinfo is timezone.utc


def test_mcmot_failure_does_not_ack_and_successful_retry_does(monkeypatch):
    state = {"calls": 0}
    module = ModuleType("mcmot")

    class FakeMCMOT:
        def __init__(self, tracking_config, camera_config):
            self.config = SimpleNamespace(cameras=[])
            self.last_successful_watermark = 0

        def process_trajectory_snapshot(self, *, objects, high_watermark):
            state["calls"] += 1
            if state["calls"] == 1:
                return {
                    "tracked_objects": [],
                    "global_objects": [],
                    "success": False,
                    "matching_performed": True,
                    "committed_watermark": 0,
                }
            self.last_successful_watermark = high_watermark
            return {
                "tracked_objects": [],
                "global_objects": [],
                "success": True,
                "matching_performed": True,
                "committed_watermark": high_watermark,
            }

    module.MCMOT = FakeMCMOT
    monkeypatch.setitem(sys.modules, "mcmot", module)

    class Context:
        def __init__(self, store):
            self.config = SimpleNamespace(
                mcmot_enabled=True,
                mcmot_tracking_config_path="/tmp/tracking.yaml",
                mcmot_camera_config_path="/tmp/camera.yaml",
                global_map_visualization_enabled=False,
            )
            self._resources = {"trajectory_store": store}
            self.logger = logging.getLogger("trajectory-store-mcmot-test")

        def get_resource(self, key):
            return self._resources.get(key)

        def set_resource(self, key, value):
            self._resources[key] = value

        def require_resource(self, key):
            return self._resources[key]

        def report_success(self, name):
            _ = name

        def report_failure(self, name, detail=None):
            _ = name, detail

    base = datetime.now(timezone.utc) - timedelta(seconds=2)
    store = TrajectoryStore()
    store.append_edge_events([_event(1, base)])
    context = Context(store)
    task = MCMOTTask()

    failed = task.run(context)
    assert failed.status == "mc_mot_failed"
    assert store.snapshot_since(0).objects
    failed_stats = context.get_resource(MC_MOT_STATS_RESOURCE)
    assert failed_stats["input_count"] == 1
    assert failed_stats["attempts"] == 1
    assert failed_stats["failed"] == 1
    assert context.get_resource(MCMOT_STATE_RESOURCE)["last_matching_status"] == "failed"
    assert context.get_resource(MCMOT_STATE_RESOURCE)["last_reason"] == "matching_failed"

    retried = task.run(context)
    assert retried.status == "mc_mot_done"
    assert store.snapshot_since(1).objects == ()


def test_matching_not_due_does_not_ack_new_revision(monkeypatch):
    state = {"calls": 0, "acks": 0, "due": True, "maintain_calls": 0}
    module = ModuleType("mcmot")

    class FakeMCMOT:
        def __init__(self, tracking_config, camera_config):
            self.config = SimpleNamespace(cameras=[])
            self.last_successful_watermark = 0

        def is_matching_due(self):
            return state["due"]

        def maintain(self):
            state["maintain_calls"] += 1

        def get_all_global_objects(self):
            return []

        def process_trajectory_snapshot(self, *, objects, high_watermark):
            state["calls"] += 1
            if state["calls"] == 1:
                state["due"] = False
                self.last_successful_watermark = high_watermark
                return {
                    "tracked_objects": [],
                    "global_objects": [],
                    "success": True,
                    "matching_performed": True,
                    "committed_watermark": high_watermark,
                }
            return {
                "tracked_objects": [],
                "global_objects": [],
                "success": True,
                "matching_performed": False,
                "committed_watermark": self.last_successful_watermark,
            }

    module.MCMOT = FakeMCMOT
    monkeypatch.setitem(sys.modules, "mcmot", module)

    class Context:
        def __init__(self, store):
            self.config = SimpleNamespace(
                mcmot_enabled=True,
                mcmot_tracking_config_path="/tmp/tracking.yaml",
                mcmot_camera_config_path="/tmp/camera.yaml",
                global_map_visualization_enabled=False,
            )
            self._resources = {"trajectory_store": store, "edge_events": []}
            self.logger = logging.getLogger("trajectory-store-cadence-test")

        def get_resource(self, key):
            return self._resources.get(key)

        def set_resource(self, key, value):
            self._resources[key] = value

        def require_resource(self, key):
            return self._resources[key]

        def report_success(self, name):
            _ = name

        def report_failure(self, name, detail=None):
            _ = name, detail

    base = datetime.now(timezone.utc) - timedelta(seconds=2)
    class CountingTrajectoryStore(TrajectoryStore):
        def __init__(self):
            super().__init__()
            self.snapshot_calls = 0

        def snapshot_since(self, watermark):
            self.snapshot_calls += 1
            return super().snapshot_since(watermark)

    store = CountingTrajectoryStore()
    store.append_edge_events([_event(1, base)])
    context = Context(store)
    task = MCMOTTask()

    first = task.run(context)
    assert first.payload["committed_watermark"] == 1
    store.append_edge_events([_event(2, base + timedelta(milliseconds=100))])
    second = task.run(context)

    assert second.payload["matching_performed"] is False
    assert state["calls"] == 1
    assert state["maintain_calls"] == 1
    assert store.snapshot_calls == 1
    assert store.acknowledged_watermark == 1
    assert store.snapshot_since(1).objects
    skipped_stats = context.get_resource(MC_MOT_STATS_RESOURCE)
    assert skipped_stats["input_count"] is None
    assert skipped_stats["result_count"] is None
    assert skipped_stats["attempts"] == 0
    assert skipped_stats["skipped"] == 1
    assert skipped_stats["failed"] == 0
    assert context.get_resource(MCMOT_STATE_RESOURCE)["last_matching_status"] == "skipped"
    assert context.get_resource(MCMOT_STATE_RESOURCE)["last_reason"] == "cadence_not_due"


def test_time_window_is_primary_and_raw_trajectory_is_not_ema_overwritten():
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = TrajectoryStore(
        retention_seconds=5,
        max_points=100,
        ema_alpha=0.01,
    )
    store.append_events(
        [
            DetectionEvent("cam-a", 1, [0, 0, 10, 10], base),
            DetectionEvent("cam-a", 1, [10, 0, 20, 20], base + timedelta(seconds=1)),
        ]
    )

    obj = store.snapshot_since(0).objects[0]
    assert obj.raw_local_trajectory == (
        (base, 5.0, 10.0),
        (base + timedelta(seconds=1), 15.0, 20.0),
    )
    assert obj.ema_point == obj.latest_point
    assert obj.first_seen_at == base
    assert obj.last_observed_at == base + timedelta(seconds=1)
    assert obj.last_updated_at is not None
    assert obj.lifecycle_state == "active"

    store.append_events(
        [DetectionEvent("cam-a", 1, [20, 0, 30, 30], base + timedelta(seconds=10))]
    )
    obj = store.snapshot_since(0).objects[0]
    assert obj.local_trajectory == ((base + timedelta(seconds=10), 25.0, 30.0),)


def test_lifecycle_sweep_distinguishes_idle_grace_expired_and_removed():
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = TrajectoryStore(
        retention_seconds=30,
        idle_after_seconds=1,
        grace_after_seconds=2,
        expire_after_seconds=3,
        remove_after_seconds=4,
    )
    store.append_events([DetectionEvent("cam-a", 1, [0, 0, 10, 10], base)])

    assert store.sweep(base + timedelta(seconds=1.5)).transitions[-1].state == "idle"
    assert store.lifecycle_state("cam-a", 1) == "idle"
    assert store.sweep(base + timedelta(seconds=2.5)).transitions[-1].state == "grace"
    assert store.sweep(base + timedelta(seconds=3.5)).transitions[-1].state == "expired"
    store.ack(1)
    result = store.sweep(base + timedelta(seconds=4.5))
    assert result.removed == (("cam-a", "1"),)
    assert store.lifecycle_state("cam-a", 1) == "removed"
    assert store.object_count() == 0


def test_objects_are_isolated_by_camera_and_local_id_and_history_is_bounded():
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = TrajectoryStore(retention_seconds=300, max_points=3)
    events = [
        DetectionEvent("cam-a", 1, [0, 0, 10, 10], base + timedelta(seconds=i))
        for i in range(8)
    ]
    events.extend(
        [
            DetectionEvent("cam-b", 1, [100, 0, 110, 10], base),
            DetectionEvent("cam-a", 2, [200, 0, 210, 10], base),
        ]
    )
    store.append_events(events)

    objects = {(
        item.camera_id,
        item.local_id,
    ): item for item in store.snapshot_since(0).objects}
    assert set(objects) == {("cam-a", "1"), ("cam-b", "1"), ("cam-a", "2")}
    assert len(objects[("cam-a", "1")].raw_local_trajectory) == 3
    assert objects[("cam-b", "1")].latest_point == (105.0, 10.0)
    assert objects[("cam-a", "2")].latest_point == (205.0, 10.0)


def test_unacknowledged_expired_record_is_retained_until_ack():
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = TrajectoryStore(
        retention_seconds=300,
        idle_after_seconds=1,
        grace_after_seconds=2,
        expire_after_seconds=3,
        remove_after_seconds=4,
    )
    store.append_events([DetectionEvent("cam-a", 1, [0, 0, 10, 10], base)])

    deferred = store.maintenance(base + timedelta(seconds=5))
    assert deferred.removed == ()
    assert store.lifecycle_state("cam-a", 1) == "expired"
    assert store.snapshot_since(0).objects[0].local_trajectory

    store.ack(1)
    removed = store.maintenance(base + timedelta(seconds=6))
    assert removed.removed == (("cam-a", "1"),)
    assert store.lifecycle_state("cam-a", 1) == "removed"


def test_point_retention_runs_during_no_event_maintenance():
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    store = TrajectoryStore(retention_seconds=2, max_points=100)
    store.append_events(
        [
            DetectionEvent("cam-a", 1, [0, 0, 10, 10], base),
            DetectionEvent("cam-a", 1, [10, 0, 20, 20], base + timedelta(seconds=1)),
        ]
    )

    store.maintenance(base + timedelta(seconds=4))
    obj = store.snapshot_since(0).objects[0]
    assert obj.local_trajectory == ()
