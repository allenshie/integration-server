from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from integration.pipeline.tasks.nodes.tracking.engine import MCMOTEngine, MCMOTResult
from integration.pipeline.tasks.nodes.tracking.task import MCMOTTask
from integration.config.visualization import GlobalMapVisualizationConfig


class DummyContext:
    def __init__(self, config, resources: dict[str, object] | None = None) -> None:
        self.config = config
        self._resources = dict(resources or {})
        self.logger = logging.getLogger("mcmot-adapter-test")
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


def _build_fake_mcmot_module(state: dict[str, object]) -> ModuleType:
    module = ModuleType("mcmot")

    class FakeMCMOT:
        def __init__(self, tracking_config, camera_config):  # noqa: ANN001
            state["tracking_config_path"] = tracking_config
            state["camera_config_path"] = camera_config
            self.config = SimpleNamespace(
                cameras=[
                    SimpleNamespace(
                        camera_id="camera_1",
                        edge_id="cam01",
                        source_id="cam01",
                        name="Camera 1",
                        enabled=True,
                        color_hex="#228B22",
                    ),
                ],
            )
            self._last_timestamp = None

        def process_detected_objects(self, camera_id, timestamp, detected_objects):  # noqa: ANN001
            state["process_call"] = {
                "camera_id": camera_id,
                "timestamp": timestamp,
                "detected_objects": list(detected_objects),
            }
            return [
                {
                    "camera_id": camera_id,
                    "class_name": detected_objects[0]["class_name"],
                    "local_id": detected_objects[0]["local_id"],
                    "global_id": "g-1",
                    "bbox": detected_objects[0]["bbox"],
                    "score": detected_objects[0]["score"],
                    "timestamp": timestamp,
                    "global_trajectory": [(timestamp, 12.0, 34.0)],
                }
            ]

        def finalize_global_updates(self, timestamp):  # noqa: ANN001
            self._last_timestamp = timestamp
            state["finalize_timestamp"] = timestamp

        def get_all_global_objects(self):
            timestamp = self._last_timestamp
            if timestamp is None:
                return []
            return [
                SimpleNamespace(
                    global_id="g-1",
                    class_name="person",
                    camera_id="camera_1",
                    trajectory=[(timestamp, 12.0, 34.0)],
                    update_time=timestamp,
                )
            ]

    module.MCMOT = FakeMCMOT
    return module


def test_mcmot_task_skips_without_external_package_when_disabled() -> None:
    context = DummyContext(
        config=SimpleNamespace(
            mcmot_enabled=False,
            mcmot_tracking_config_path=None,
            mcmot_camera_config_path=None,
            global_map_visualization_enabled=False,
        ),
        resources={
            "edge_events": [
                {
                    "camera_id": "cam01",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "detections": [],
                },
            ]
        },
    )

    task = MCMOTTask()
    result = task.execute(context)

    assert result.status == "mc_mot_skipped"
    assert context.get_resource("mc_mot_tracked") == []
    assert context.get_resource("mc_mot_global_objects") == []
    assert context.reported_success == ["mc_mot"]


def test_mcmot_engine_uses_external_mcmot_module(monkeypatch) -> None:
    fake_state: dict[str, object] = {}
    monkeypatch.setitem(sys.modules, "mcmot", _build_fake_mcmot_module(fake_state))

    tracking_config_path = Path("/tmp/mcmot-tracking-config.yaml")
    camera_config_path = Path("/tmp/mcmot-camera-config.yaml")
    engine = MCMOTEngine(
        tracking_config=str(tracking_config_path),
        camera_config=str(camera_config_path),
    )

    timestamp = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    result = engine.process_events(
        [
            {
                "camera_id": "cam01",
                "timestamp": timestamp.isoformat(),
                "detections": [
                    {
                        "local_id": 7,
                        "class_name": "person",
                        "score": 0.95,
                        "bbox": [10, 20, 30, 40],
                    }
                ],
            }
        ]
    )

    assert fake_state["tracking_config_path"] == str(tracking_config_path)
    assert fake_state["camera_config_path"] == str(camera_config_path)
    assert fake_state["process_call"]["camera_id"] == "cam01"
    assert fake_state["finalize_timestamp"] == timestamp
    assert result.tracked_objects == [
        {
            "camera_id": "cam01",
            "class_name": "person",
            "local_id": 7,
            "global_id": "g-1",
            "bbox": [10, 20, 30, 40],
            "score": 0.95,
            "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
            "global_position": {"x": 12.0, "y": 34.0},
        }
    ]
    assert result.global_objects == [
        {
            "global_id": "g-1",
            "class_name": "person",
            "camera_id": "camera_1",
            "trajectory": [
                {
                    "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
                    "x": 12.0,
                    "y": 34.0,
                }
            ],
                    "updated_at": timestamp.isoformat().replace("+00:00", "Z"),
        }
    ]


@pytest.mark.parametrize(
    ("tracking_config", "camera_config", "missing_env"),
    [
        (None, "/tmp/mcmot-camera-config.yaml", "MCMOT_TRACKING_CONFIG_PATH"),
        ("/tmp/mcmot-tracking-config.yaml", None, "MCMOT_CAMERA_CONFIG_PATH"),
    ],
)
def test_mcmot_engine_requires_both_config_paths(
    tracking_config: str | None,
    camera_config: str | None,
    missing_env: str,
) -> None:
    with pytest.raises(ValueError, match=missing_env):
        MCMOTEngine(
            tracking_config=tracking_config,
            camera_config=camera_config,
        )


def test_mcmot_task_initializes_engine_from_external_module(monkeypatch) -> None:
    fake_state: dict[str, object] = {}
    monkeypatch.setitem(sys.modules, "mcmot", _build_fake_mcmot_module(fake_state))

    context = DummyContext(
        config=SimpleNamespace(
            mcmot_enabled=True,
            mcmot_tracking_config_path="/tmp/mcmot-tracking-config.yaml",
            mcmot_camera_config_path="/tmp/mcmot-camera-config.yaml",
            global_map_visualization_enabled=False,
        ),
        resources={
            "edge_events": [
                {
                    "camera_id": "cam01",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "detections": [
                        {
                            "local_id": 7,
                            "class_name": "person",
                            "score": 0.95,
                            "bbox": [10, 20, 30, 40],
                        }
                    ],
                }
            ]
        },
    )

    task = MCMOTTask()
    result = task.execute(context)

    assert result.status == "mc_mot_done"
    assert result.payload == {
        "events": 1,
        "snapshot_objects": 1,
        "tracked": 1,
        "global_objects": 1,
    }
    assert context.get_resource("mcmot_engine") is not None
    assert context.get_resource("global_map_renderer") is None
    assert len(context.get_resource("mc_mot_tracked")) == 1
    assert len(context.get_resource("mc_mot_global_objects")) == 1
    assert fake_state["tracking_config_path"] == "/tmp/mcmot-tracking-config.yaml"
    assert fake_state["camera_config_path"] == "/tmp/mcmot-camera-config.yaml"


def test_mcmot_task_keeps_attempt_report_opaque_and_does_not_render_detail(
    caplog,
) -> None:
    attempt_report = object()

    class TypedEngine:
        last_successful_watermark = 0

        def is_matching_due(self):
            return True

        def process_trajectory_snapshot(self, snapshot):
            return MCMOTResult(
                tracked_objects=[],
                global_objects=[],
                success=True,
                matching_performed=True,
                committed_watermark=snapshot.high_watermark,
                attempt_report=attempt_report,
            )

        def get_all_global_objects(self):
            return []

    class Store:
        def snapshot_since(self, watermark):
            return SimpleNamespace(objects=(), high_watermark=7)

        def ack(self, watermark):
            _ = watermark

    context = DummyContext(
        config=SimpleNamespace(
            mcmot_enabled=True,
            mcmot_tracking_config_path=None,
            mcmot_camera_config_path=None,
            global_map_visualization_enabled=False,
        ),
        resources={"trajectory_store": Store(), "edge_events": []},
    )
    task = MCMOTTask()
    task._engine = TypedEngine()

    with caplog.at_level(logging.INFO, logger=context.logger.name):
        result = task.run(context)

    assert result.status == "mc_mot_done"
    assert "matching_attempt" not in caplog.text


def test_mcmot_task_init_engine_uses_default_engine_class(monkeypatch) -> None:
    fake_state: dict[str, object] = {}
    monkeypatch.setitem(sys.modules, "mcmot", _build_fake_mcmot_module(fake_state))

    context = DummyContext(
        config=SimpleNamespace(
            mcmot_enabled=True,
            mcmot_tracking_config_path="/tmp/mcmot-tracking-config.yaml",
            mcmot_camera_config_path="/tmp/mcmot-camera-config.yaml",
            global_map_visualization_enabled=False,
        ),
        resources={},
    )

    task = MCMOTTask()
    engine = task._init_engine(context)

    assert isinstance(engine, MCMOTEngine)
    assert fake_state["tracking_config_path"] == "/tmp/mcmot-tracking-config.yaml"
    assert fake_state["camera_config_path"] == "/tmp/mcmot-camera-config.yaml"


def test_mcmot_task_builds_global_map_renderer_from_visual_config(monkeypatch) -> None:
    fake_state: dict[str, object] = {}
    monkeypatch.setitem(sys.modules, "mcmot", _build_fake_mcmot_module(fake_state))

    vis_cfg = GlobalMapVisualizationConfig(
        map={
            "image_path": "/tmp/global-map.png",
            "width_meters": 10.0,
            "height_meters": 5.0,
        },
        render={
            "mode": "write",
            "output_dir": "/tmp/global-map-output",
        },
        cameras=[
            {
                "camera_id": "camera_1",
                "display_name": "Camera 1",
                "aliases": ["cam01", "edge_cam01"],
            }
        ],
    )

    context = DummyContext(
        config=SimpleNamespace(
            mcmot_enabled=True,
            mcmot_tracking_config_path="/tmp/mcmot-tracking-config.yaml",
            mcmot_camera_config_path="/tmp/mcmot-camera-config.yaml",
            global_map_visualization_enabled=True,
            global_map_visualization=vis_cfg,
        ),
        resources={
            "edge_events": [
                {
                    "camera_id": "cam01",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "detections": [
                        {
                            "local_id": 7,
                            "class_name": "person",
                            "score": 0.95,
                            "bbox": [10, 20, 30, 40],
                        }
                    ],
                }
            ]
        },
    )

    task = MCMOTTask()
    result = task.execute(context)

    assert result.status == "mc_mot_done"
    assert context.get_resource("global_map_renderer") is not None
