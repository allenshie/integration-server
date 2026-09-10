from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import mcmot
import pytest

from integration.api.trajectory_store import DetectionEvent, TimestampContractError, TrajectoryStore
from integration.pipeline.tasks.nodes.tracking.engine import MCMOTEngine
from mcmot.core.contracts import TrajectorySnapshotEnvelope
from mcmot.core.mcmot.gallery_components.runtime_factory import GalleryRuntimeFactory


def test_trajectory_snapshot_excludes_external_global_id_annotation():
    store = TrajectoryStore()
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)

    store.append_events(
        [
            DetectionEvent(
                camera_id="cam-a",
                local_id=7,
                bbox=[0, 0, 10, 10],
                timestamp=timestamp,
                metadata={"global_id": "annotation-only", "frame_seq": 1},
            )
        ]
    )

    snapshot_object = store.snapshot_since(0).objects[0]
    payload = MCMOTEngine._serialize_trajectory_object(snapshot_object)

    assert "global_id" not in payload
    assert "global_id" not in payload["metadata"]
    assert payload["local_trajectory"] == [[timestamp, 5.0, 10.0]]


def test_app_serializer_rejects_mixed_naive_trajectory_before_mcmot():
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    obj = SimpleNamespace(
        camera_id="cam-a",
        local_id="7",
        class_name="person",
        latest_bbox=(0.0, 0.0, 10.0, 10.0),
        latest_score=0.9,
        latest_feature=None,
        first_seen_at=timestamp,
        last_observed_at=timestamp,
        last_updated_at=timestamp,
        lifecycle_state="active",
        latest_timestamp=timestamp,
        local_trajectory=((timestamp, 5.0, 10.0), (datetime(2026, 1, 1), 6.0, 11.0)),
        revision=2,
        watermark=2,
        latest_metadata={},
    )

    with pytest.raises(TimestampContractError, match=r"local_trajectory\[1\]\[0\]"):
        MCMOTEngine._serialize_trajectory_object(obj)


def test_valid_app_snapshot_reaches_mcmot_with_utc_aware_times():
    timestamp = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    store = TrajectoryStore()
    store.append_events(
        [DetectionEvent("cam-a", 7, [0, 0, 10, 10], timestamp, class_name="person")]
    )
    snapshot = store.snapshot_since(0)

    class _TypedMCMOT:
        def process_trajectory_snapshot(self, *, objects, high_watermark):
            envelope = TrajectorySnapshotEnvelope.from_parts(
                objects,
                high_watermark,
                captured_at=datetime.now(timezone.utc),
            )
            # This is the operation that previously failed when App and
            # MCMOT mixed offset-aware and naive datetimes.
            _ = datetime.now(timezone.utc) - envelope.objects[0].observed_at
            return {
                "tracked_objects": [],
                "global_objects": [],
                "success": True,
                "matching_performed": True,
                "committed_watermark": high_watermark,
            }

    engine = MCMOTEngine.__new__(MCMOTEngine)
    engine._engine = _TypedMCMOT()
    engine._last_successful_watermark = 0

    result = engine.process_trajectory_snapshot(snapshot)

    assert result.success is True
    assert result.committed_watermark == 1


def test_app_adapter_preserves_attempt_report_as_opaque_result():
    attempt_report = object()

    class _TypedMCMOT:
        def process_trajectory_snapshot(self, *, objects, high_watermark):
            _ = objects
            return {
                "tracked_objects": [],
                "global_objects": [],
                "success": True,
                "matching_performed": True,
                "committed_watermark": high_watermark,
                "attempt_report": attempt_report,
            }

    engine = MCMOTEngine.__new__(MCMOTEngine)
    engine._engine = _TypedMCMOT()
    engine._last_successful_watermark = 0
    snapshot = SimpleNamespace(objects=(), high_watermark=7)

    result = engine.process_trajectory_snapshot(snapshot)

    assert result.attempt_report is attempt_report
    assert engine.last_attempt_report is attempt_report


def test_local_editable_mcmot_runtime_accepts_snapshot_without_global_id():
    mcmot_path = Path(mcmot.__file__).resolve()
    expected_root = Path(__file__).resolve().parents[3] / "MCMOT"
    assert mcmot_path.is_relative_to(expected_root.resolve())

    factory = GalleryRuntimeFactory(settings=SimpleNamespace())
    objects = factory.to_object_data(
        "cam-a",
        [
            {
                "local_id": "7",
                "class_name": "person",
                "local_trajectory": [[datetime(2026, 1, 1), 5.0, 10.0]],
                "global_trajectory": [[datetime(2026, 1, 1), 50.0, 100.0]],
                "revision": 1,
                "watermark": 1,
            }
        ],
    )

    assert len(objects) == 1
    assert objects[0].global_id is None
    assert objects[0].trajectory == [[datetime(2026, 1, 1), 50.0, 100.0]]
