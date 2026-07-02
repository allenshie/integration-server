from __future__ import annotations

import json
from datetime import datetime, timezone

from integration.pipeline.tasks.nodes.matching_broadcast.recorder import (
    MatchingBroadcastObservationWriter,
    build_matching_broadcast_records,
)
from integration.pipeline.tasks.nodes.matching_broadcast.schema import MatchingBroadcastBatch, MatchingBroadcastSnapshot


def test_matching_broadcast_observation_writer_appends_v2_snapshot_records(tmp_path) -> None:
    generated_at = datetime(2026, 6, 23, 10, 5, tzinfo=timezone.utc)
    batch = MatchingBroadcastBatch(
        session_id="sess-a",
        frame_seq=7,
        capture_ts="2026-06-23T10:00:00+00:00",
        tracked_objects=[
            {
                "camera_id": "cam02",
                "class_name": "truck",
                "local_id": 4,
                "global_id": 30,
                "bbox": [5, 6, 7, 8],
                "score": 0.88,
                "timestamp": "2026-06-23T10:00:00+00:00",
                "global_position": {"x": 18.0, "y": 24.0},
            },
            {
                "camera_id": "cam01",
                "class_name": "car",
                "local_id": 1,
                "global_id": 10,
                "bbox": [1, 2, 3, 4],
                "score": 0.97,
                "timestamp": "2026-06-23T10:00:00+00:00",
                "global_position": {"x": 11.0, "y": 22.0},
            },
        ],
        global_objects=[
            {
                "global_id": 30,
                "class_name": "truck",
                "trajectory": [{"timestamp": "2026-06-23T10:00:00+00:00", "x": 18.0, "y": 24.0}],
            },
            {
                "global_id": 10,
                "class_name": "car",
                "trajectory": [{"timestamp": "2026-06-23T10:00:00+00:00", "x": 11.0, "y": 22.0}],
            },
        ],
    )
    snapshot = MatchingBroadcastSnapshot.from_batch(batch, generated_at=generated_at)
    edge_events = [
        {
            "camera_id": "cam01",
            "session_id": "sess-a",
            "frame_seq": 7,
            "capture_ts": "2026-06-23T10:00:00+00:00",
        },
        {
            "camera_id": "cam02",
            "session_id": "sess-a",
            "frame_seq": 7,
            "capture_ts": "2026-06-23T10:00:00+00:00",
        },
    ]

    records = build_matching_broadcast_records(edge_events, batch, snapshot.to_dict())
    output_path = tmp_path / "output" / "matching" / "matching_observations.jsonl"
    writer = MatchingBroadcastObservationWriter(output_path)
    written = writer.write_records(records)

    assert written == 2

    written_lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(written_lines) == 2
    first_record = json.loads(written_lines[0])
    assert first_record["message_type"] == "matching_observation"
    assert first_record["camera_id"] == "cam01"
    assert first_record["frame_seq"] == 7
    assert first_record["matching_snapshot"]["message_type"] == "matching_snapshot"
    assert first_record["matching_snapshot"]["schema_version"] == 2
    assert first_record["matching_snapshot"]["objects"][0]["global_id"] == 10
    assert first_record["tracked_objects"][0]["global_id"] == 10
    assert first_record["global_objects"][0]["global_id"] == 30
