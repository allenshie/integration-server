from __future__ import annotations

import logging
from types import SimpleNamespace

from integration.pipeline.tasks.nodes.matching_broadcast.constants import MATCHING_BROADCAST_ROUTE
from integration.pipeline.tasks.nodes.matching_broadcast.task import MatchingBroadcastTask
from integration.pipeline.tasks.summary import MATCHING_BROADCAST_STATS_RESOURCE


class DummyMessagingClient:
    def __init__(self, publish_result: bool = True) -> None:
        self.publish_result = publish_result
        self.calls: list[tuple[str, dict[str, object]]] = []

    def publish(self, route: str, payload: dict[str, object]) -> bool:
        self.calls.append((route, payload))
        return self.publish_result


class DummyContext:
    def __init__(
        self,
        *,
        enabled: bool,
        tracked_objects: list[dict[str, object]] | None = None,
        global_objects: list[dict[str, object]] | None = None,
        edge_events: list[dict[str, object]] | None = None,
        publish_result: bool = True,
    ) -> None:
        self.config = SimpleNamespace(
            matching_broadcast=SimpleNamespace(
                enabled=enabled,
                recording=SimpleNamespace(enabled=False, path=None),
            ),
        )
        self.logger = logging.getLogger("matching-broadcast-test")
        self._resources = {
            "mc_mot_tracked": tracked_objects or [],
            "mc_mot_global_objects": global_objects or [],
            "edge_events": edge_events or [],
            "messaging_client": DummyMessagingClient(publish_result=publish_result),
        }

    def get_resource(self, key: str):
        return self._resources.get(key)

    def set_resource(self, key: str, value) -> None:  # noqa: ANN001
        self._resources[key] = value


def test_matching_broadcast_task_skips_when_disabled() -> None:
    context = DummyContext(
        enabled=False,
        edge_events=[{"session_id": "sess-a", "frame_seq": 7, "camera_id": "cam01"}],
        tracked_objects=[{"camera_id": "cam01", "local_id": 1, "global_id": 99, "class_name": "car"}],
    )

    result = MatchingBroadcastTask().run(context)

    assert result.status == "matching_broadcast_skipped"
    assert context.get_resource("messaging_client").calls == []


def test_matching_broadcast_task_fails_on_inconsistent_batch_identity() -> None:
    context = DummyContext(
        enabled=True,
        edge_events=[
            {"session_id": "sess-a", "frame_seq": 7, "camera_id": "cam01", "capture_ts": "2026-06-23T10:00:00+00:00"},
            {"session_id": "sess-b", "frame_seq": 7, "camera_id": "cam02", "capture_ts": "2026-06-23T10:00:00+00:00"},
        ],
        tracked_objects=[{"camera_id": "cam01", "local_id": 1, "global_id": 99, "class_name": "car"}],
    )

    result = MatchingBroadcastTask().run(context)

    assert result.status == "matching_broadcast_failed"
    assert result.payload == {
        "session_id": None,
        "frame_seq": None,
        "dispatched": 0,
        "skipped": 0,
        "failed": 1,
        "recorded": 0,
        "recording_failed": 0,
        "reason": "inconsistent_batch_identity",
    }
    assert context.get_resource("messaging_client").calls == []
    assert context.get_resource(MATCHING_BROADCAST_STATS_RESOURCE) == {
        "dispatched": 0,
        "skipped": 0,
        "failed": 1,
        "recorded": 0,
        "recording_failed": 0,
    }


def test_matching_broadcast_task_publishes_and_records_v2_snapshot(tmp_path) -> None:
    output_path = tmp_path / "matching_observations.jsonl"
    context = DummyContext(
        enabled=True,
        edge_events=[
            {
                "camera_id": "cam01",
                "session_id": "sess-a",
                "frame_seq": 7,
                "capture_ts": "2026-06-23T10:00:00+00:00",
                "timestamp": "2026-06-23T10:00:00+00:00",
            },
            {
                "camera_id": "cam02",
                "session_id": "sess-a",
                "frame_seq": 7,
                "capture_ts": "2026-06-23T10:00:00+00:00",
                "timestamp": "2026-06-23T10:00:00+00:00",
            },
        ],
        tracked_objects=[
            {
                "camera_id": "cam01",
                "local_id": 1,
                "global_id": 10,
                "class_name": "car",
                "bbox": [1, 2, 3, 4],
                "score": 0.97,
                "timestamp": "2026-06-23T10:00:00+00:00",
                "global_position": {"x": 11.0, "y": 22.0},
            },
            {
                "camera_id": "cam02",
                "local_id": 4,
                "global_id": 30,
                "class_name": "truck",
                "bbox": [5, 6, 7, 8],
                "score": 0.88,
                "timestamp": "2026-06-23T10:00:00+00:00",
                "global_position": {"x": 18.0, "y": 24.0},
            },
        ],
        global_objects=[
            {
                "global_id": 10,
                "class_name": "car",
                "trajectory": [{"timestamp": "2026-06-23T10:00:00+00:00", "x": 11.0, "y": 22.0}],
            },
            {
                "global_id": 30,
                "class_name": "truck",
                "trajectory": [{"timestamp": "2026-06-23T10:00:00+00:00", "x": 18.0, "y": 24.0}],
            },
        ],
    )
    context.config.matching_broadcast.recording = SimpleNamespace(enabled=True, path=str(output_path))

    result = MatchingBroadcastTask().run(context)

    assert result.status == "matching_broadcast_done"
    assert result.payload == {
        "session_id": "sess-a",
        "frame_seq": 7,
        "dispatched": 1,
        "skipped": 0,
        "failed": 0,
        "recorded": 2,
        "recording_failed": 0,
    }
    calls = context.get_resource("messaging_client").calls
    assert len(calls) == 1
    route, payload = calls[0]
    assert route == MATCHING_BROADCAST_ROUTE
    assert payload["message_type"] == "matching_snapshot"
    assert payload["schema_version"] == 2
    assert payload["session_id"] == "sess-a"
    assert payload["frame_seq"] == 7
    assert output_path.exists()
    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
