from __future__ import annotations

import logging
from types import SimpleNamespace

from integration.pipeline.tasks.nodes.matching_broadcast.constants import MATCHING_BROADCAST_ROUTE
from integration.pipeline.tasks.nodes.matching_broadcast.task import MatchingBroadcastTask


class DummyMessagingClient:
    def __init__(self, publish_result: bool = True) -> None:
        self.publish_result = publish_result
        self.calls: list[tuple[str, dict[str, object]]] = []

    def publish(self, route: str, payload: dict[str, object]) -> bool:
        self.calls.append((route, payload))
        return self.publish_result


class DummyContext:
    def __init__(self, enabled: bool, tracked_objects: list[dict[str, object]], publish_result: bool = True) -> None:
        self.config = SimpleNamespace(
            matching_broadcast=SimpleNamespace(
                enabled=enabled,
                recording=SimpleNamespace(enabled=False, path=None),
            ),
        )
        self.logger = logging.getLogger("matching-broadcast-test")
        self._resources = {
            "mc_mot_tracked": tracked_objects,
            "messaging_client": DummyMessagingClient(publish_result=publish_result),
        }

    def get_resource(self, key: str):
        return self._resources.get(key)

    def set_resource(self, key: str, value) -> None:  # noqa: ANN001
        self._resources[key] = value


def test_matching_broadcast_task_skips_when_disabled() -> None:
    context = DummyContext(
        enabled=False,
        tracked_objects=[{"camera_id": "cam01", "local_id": 1, "global_id": 99, "class_name": "car"}],
    )

    result = MatchingBroadcastTask().run(context)

    assert result.status == "matching_broadcast_skipped"
    assert context.get_resource("messaging_client").calls == []


def test_matching_broadcast_task_publishes_grouped_payload() -> None:
    context = DummyContext(
        enabled=True,
        tracked_objects=[
            {"camera_id": "cam02", "local_id": 4, "global_id": 30, "class_name": "truck"},
            {"camera_id": "cam01", "local_id": 1, "global_id": 10, "class_name": "car"},
            {"camera_id": "cam01", "local_id": 2, "global_id": 11, "class_name": "bus"},
        ],
    )

    result = MatchingBroadcastTask().run(context)

    assert result.status == "matching_broadcast_done"
    calls = context.get_resource("messaging_client").calls
    assert len(calls) == 1
    route, payload = calls[0]
    assert route == MATCHING_BROADCAST_ROUTE
    assert payload["message_type"] == "matching_result"
    assert payload["schema_version"] == 1
    assert list(payload["camera_matches"].keys()) == ["cam01", "cam02"]
    assert payload["camera_matches"]["cam01"][0]["local_id"] == 1
    assert payload["camera_matches"]["cam01"][1]["global_id"] == 11
    assert payload["camera_matches"]["cam02"][0]["class_name"] == "truck"


def test_matching_broadcast_task_records_frame_observations(tmp_path) -> None:
    output_path = tmp_path / "matching_observations.jsonl"
    context = DummyContext(
        enabled=True,
        tracked_objects=[
            {"camera_id": "cam01", "local_id": 1, "global_id": 10, "class_name": "car"},
            {"camera_id": "cam02", "local_id": 4, "global_id": 30, "class_name": "truck"},
        ],
    )
    context.config.matching_broadcast.recording = SimpleNamespace(enabled=True, path=str(output_path))
    context._resources["edge_events"] = [
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
    ]
    context._resources["mc_mot_global_objects"] = [
        {
            "global_id": 10,
            "class_name": "car",
            "camera_id": "cam01",
            "trajectory": [{"timestamp": "2026-06-23T10:00:00+00:00", "x": 11.0, "y": 22.0}],
            "updated_at": "2026-06-23T10:00:00+00:00",
        }
    ]

    result = MatchingBroadcastTask().run(context)

    assert result.status == "matching_broadcast_done"
    assert result.payload is not None
    assert result.payload["recorded"] == 2
    assert output_path.exists()
    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert '"message_type":"matching_observation"' in lines[0]
    assert '"camera_id":"cam01"' in lines[0]
    assert '"frame_seq":7' in lines[0]
