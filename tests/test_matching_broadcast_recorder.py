from __future__ import annotations

from datetime import datetime, timezone

from integration.pipeline.tasks.nodes.matching_broadcast.recorder import (
    MatchingBroadcastObservationWriter,
    build_matching_broadcast_records,
)


def test_matching_broadcast_observation_writer_appends_frame_records(tmp_path) -> None:
    edge_events = [
        {
            "camera_id": "cam01",
            "session_id": "sess-a",
            "frame_seq": 7,
            "capture_ts": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
            "timestamp": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
        },
        {
            "camera_id": "cam02",
            "session_id": "sess-a",
            "frame_seq": 7,
            "capture_ts": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
            "timestamp": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
        },
    ]
    tracked_objects = [
        {
            "camera_id": "cam02",
            "class_name": "truck",
            "local_id": 4,
            "global_id": 30,
            "bbox": [5, 6, 7, 8],
            "score": 0.88,
            "timestamp": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
            "global_position": {"x": 18.0, "y": 24.0},
        },
        {
            "camera_id": "cam02",
            "class_name": "truck",
            "local_id": 9,
            "global_id": None,
            "bbox": [8, 9, 10, 11],
            "score": 0.42,
            "timestamp": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
            "global_position": {"x": 7.0, "y": 6.0},
        },
        {
            "camera_id": "cam01",
            "class_name": "car",
            "local_id": 1,
            "global_id": 10,
            "bbox": [1, 2, 3, 4],
            "score": 0.97,
            "timestamp": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
            "global_position": {"x": 11.0, "y": 22.0},
        },
    ]
    global_objects = [
        {
            "global_id": 10,
            "class_name": "car",
            "camera_id": "cam01",
            "trajectory": [
                {
                    "timestamp": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
                    "x": 11.0,
                    "y": 22.0,
                }
            ],
            "updated_at": datetime(2026, 6, 23, 10, 0, tzinfo=timezone.utc).isoformat(),
        }
    ]

    records = build_matching_broadcast_records(edge_events, tracked_objects, global_objects)
    output_path = tmp_path / "output" / "matching" / "matching_observations.jsonl"
    writer = MatchingBroadcastObservationWriter(output_path)
    written = writer.write_records(records)

    assert written == 2

    written_lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(written_lines) == 2
    assert "\"message_type\":\"matching_observation\"" in written_lines[0]
    assert "\"camera_id\":\"cam01\"" in written_lines[0]
    assert "\"frame_seq\":7" in written_lines[0]
    assert "\"matching_result\"" in written_lines[0]
    assert "\"tracked_objects\"" in written_lines[0]
    assert "\"global_objects\"" in written_lines[0]
    assert records[0]["matching_result"]["camera_matches"]["cam01"][0]["matched"] is True
    assert records[1]["matching_result"]["camera_matches"]["cam02"][0]["matched"] is True
    assert records[1]["matching_result"]["camera_matches"]["cam02"][1]["matched"] is False
    assert records[1]["tracked_objects"][1]["matched"] is False
