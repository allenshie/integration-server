from __future__ import annotations

import logging
from types import SimpleNamespace

from integration.pipeline.tasks.nodes.matching_broadcast.constants import MATCHING_BROADCAST_ROUTE
from integration.pipeline.tasks.nodes.matching_broadcast.engine import DefaultMatchingBroadcastEngine
from integration.pipeline.tasks.nodes.matching_broadcast.schema import MatchingBroadcastBatch


class DummyMessagingClient:
    def __init__(self, publish_result: bool = True) -> None:
        self.publish_result = publish_result
        self.calls: list[tuple[str, dict[str, object]]] = []

    def publish(self, route: str, payload: dict[str, object]) -> bool:
        self.calls.append((route, payload))
        return self.publish_result


class DummyContext:
    def __init__(self, enabled: bool, publish_result: bool = True) -> None:
        self.config = SimpleNamespace(matching_broadcast=SimpleNamespace(enabled=enabled))
        self.logger = logging.getLogger("matching-broadcast-engine-test")
        self._resources = {
            "messaging_client": DummyMessagingClient(publish_result=publish_result),
        }

    def get_resource(self, key: str):
        return self._resources.get(key)


def test_default_matching_broadcast_engine_skips_when_disabled() -> None:
    context = DummyContext(enabled=False)
    batch = MatchingBroadcastBatch(
        session_id="sess-a",
        frame_seq=7,
        capture_ts="2026-06-23T10:00:00+00:00",
    )

    result = DefaultMatchingBroadcastEngine().broadcast(batch, context)

    assert result.skipped == 1
    assert result.reason == "disabled"
    assert context.get_resource("messaging_client").calls == []


def test_default_matching_broadcast_engine_publishes_v2_snapshot() -> None:
    context = DummyContext(enabled=True)
    batch = MatchingBroadcastBatch(
        session_id="sess-a",
        frame_seq=7,
        capture_ts="2026-06-23T10:00:00+00:00",
        tracked_objects=[
            {
                "camera_id": "cam-b",
                "local_id": 4,
                "global_id": "2",
                "class_name": "truck",
                "bbox": [5, 6, 7, 8],
                "score": 0.88,
                "timestamp": "2026-06-23T10:00:00+00:00",
                "global_position": {"x": 18.0, "y": 24.0},
            },
            {
                "camera_id": "cam-a",
                "local_id": 1,
                "global_id": 2,
                "class_name": "car",
                "bbox": [1, 2, 3, 4],
                "score": 0.97,
                "timestamp": "2026-06-23T10:00:00+00:00",
                "global_position": {"x": 11.0, "y": 22.0},
            },
            {
                "camera_id": "cam-a",
                "local_id": 3,
                "global_id": 2,
                "class_name": "car",
                "bbox": [2, 3, 4, 5],
                "score": 0.75,
                "timestamp": "2026-06-23T10:00:00+00:00",
                "global_position": {"x": 12.0, "y": 23.0},
            },
        ],
        global_objects=[
            {
                "global_id": "2",
                "class_name": "truck",
                "trajectory": [{"x": 18.0, "y": 24.0, "timestamp": "2026-06-23T10:00:00+00:00"}],
            },
            {
                "global_id": 2,
                "class_name": "car",
                "trajectory": [{"x": 11.5, "y": 22.5, "timestamp": "2026-06-23T10:00:00+00:00"}],
            },
        ],
    )

    result = DefaultMatchingBroadcastEngine().broadcast(batch, context)

    assert result.dispatched == 1
    assert result.message_payload is not None
    calls = context.get_resource("messaging_client").calls
    assert len(calls) == 1
    route, payload = calls[0]
    assert route == MATCHING_BROADCAST_ROUTE
    assert payload["message_type"] == "matching_snapshot"
    assert payload["schema_version"] == 2
    assert payload["session_id"] == "sess-a"
    assert payload["frame_seq"] == 7
    assert payload["capture_ts"] == "2026-06-23T10:00:00+00:00"
    assert [item["global_id"] for item in payload["objects"]] == [2, "2"]
    assert payload["objects"][0]["matched_locals"] == [
        {
            "camera_id": "cam-a",
            "local_id": 1,
            "global_id": 2,
            "global_position": {"x": 11.0, "y": 22.0},
            "class_name": "car",
            "bbox": [1, 2, 3, 4],
            "score": 0.97,
            "timestamp": "2026-06-23T10:00:00+00:00",
        },
        {
            "camera_id": "cam-a",
            "local_id": 3,
            "global_id": 2,
            "global_position": {"x": 12.0, "y": 23.0},
            "class_name": "car",
            "bbox": [2, 3, 4, 5],
            "score": 0.75,
            "timestamp": "2026-06-23T10:00:00+00:00",
        },
    ]
    assert payload["objects"][1]["matched_locals"][0]["camera_id"] == "cam-b"


def test_default_matching_broadcast_engine_publishes_empty_snapshot_for_valid_batch() -> None:
    context = DummyContext(enabled=True)
    batch = MatchingBroadcastBatch(
        session_id="sess-a",
        frame_seq=7,
        capture_ts="2026-06-23T10:00:00+00:00",
        tracked_objects=[
            {"camera_id": "cam-a", "local_id": 9, "global_id": None, "global_position": {"x": 1.0, "y": 2.0}},
        ],
        global_objects=[],
    )

    result = DefaultMatchingBroadcastEngine().broadcast(batch, context)

    assert result.dispatched == 1
    payload = context.get_resource("messaging_client").calls[0][1]
    assert payload["objects"] == []
