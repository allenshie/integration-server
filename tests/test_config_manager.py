from __future__ import annotations

import os
from pathlib import Path

import pytest
from pydantic import BaseModel

from integration.config.manager import ConfigManager
from integration.config.settings import load_config
from integration.utils.paths import get_config_root, set_config_root


class DemoConfig(BaseModel):
    title: str
    asset_path: str


def _resolve_demo_paths(raw_config: dict[str, object], config_path: Path) -> dict[str, object]:
    processed = dict(raw_config)
    asset_path = processed.get("asset_path")
    if isinstance(asset_path, str) and asset_path.strip():
        path = Path(asset_path.strip())
        if not path.is_absolute():
            processed["asset_path"] = str((config_path.parent / path).resolve())
    return processed


def test_config_manager_loads_yaml_and_applies_preprocessors(tmp_path) -> None:
    config_path = tmp_path / "demo.yaml"
    asset_path = tmp_path / "assets" / "example.txt"
    asset_path.parent.mkdir(parents=True, exist_ok=True)
    asset_path.write_text("demo", encoding="utf-8")
    config_path.write_text(
        "\n".join(
            [
                "title: demo",
                "asset_path: assets/example.txt",
            ],
        ),
        encoding="utf-8",
    )

    manager = ConfigManager(config_path, DemoConfig, preprocessors=(_resolve_demo_paths,))
    config = manager.load()

    assert config.title == "demo"
    assert config.asset_path == str(asset_path.resolve())


def test_load_config_attaches_global_map_visualization(tmp_path, monkeypatch) -> None:
    original_root = get_config_root()
    try:
        set_config_root(tmp_path)
        config_dir = tmp_path / "data" / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        (tmp_path / "data" / "assets").mkdir(parents=True, exist_ok=True)
        (config_dir / "global_map_visualization.yaml").write_text(
            "\n".join(
                [
                    "map:",
                    "  image_path: data/assets/global_map.png",
                    "  width_meters: 120.0",
                    "  height_meters: 60.0",
                    "",
                    "render:",
                    "  mode: write",
                    "  output_dir: output/global_map",
                    "  window_name: global-map",
                    "",
                    "cameras:",
                    "  - camera_id: cam01",
                    "    display_name: Camera 1",
                    "    aliases:",
                    "      - cam01",
                    "      - edge_cam01",
                ],
            ),
            encoding="utf-8",
        )

        monkeypatch.setenv("GLOBAL_MAP_VIS_ENABLED", "1")
        monkeypatch.setenv("GLOBAL_MAP_VIS_CONFIG_PATH", "data/config/global_map_visualization.yaml")
        monkeypatch.setenv("MCMOT_ENABLED", "0")

        config = load_config()

        assert config.global_map_visualization_enabled is True
        assert config.global_map_visualization is not None
        visual_cfg = config.global_map_visualization
        assert visual_cfg.map.image_path == str((tmp_path / "data" / "assets" / "global_map.png").resolve())
        assert visual_cfg.render.output_dir == str((tmp_path / "output" / "global_map").resolve())
        assert visual_cfg.cameras[0].aliases == ["cam01", "edge_cam01"]
    finally:
        set_config_root(original_root)
        os.environ.pop("GLOBAL_MAP_VIS_ENABLED", None)
        os.environ.pop("GLOBAL_MAP_VIS_CONFIG_PATH", None)


def test_load_config_requires_visualization_path_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("MCMOT_ENABLED", "0")
    monkeypatch.setenv("GLOBAL_MAP_VIS_ENABLED", "1")
    monkeypatch.delenv("GLOBAL_MAP_VIS_CONFIG_PATH", raising=False)

    with pytest.raises(RuntimeError, match="GLOBAL_MAP_VIS_CONFIG_PATH"):
        load_config()


def test_load_config_includes_matching_broadcast_settings(monkeypatch) -> None:
    monkeypatch.setenv("MCMOT_ENABLED", "0")
    monkeypatch.setenv("MATCHING_BROADCAST_ENABLED", "1")
    monkeypatch.setenv("MATCHING_BROADCAST_BACKEND", "mqtt")
    monkeypatch.setenv("MATCHING_BROADCAST_TOPIC", "integration/matching")

    config = load_config()

    assert config.matching_broadcast.enabled is True
    assert config.matching_broadcast.backend == "mqtt"
    assert config.matching_broadcast.channel == "integration/matching"


def test_load_config_includes_matching_broadcast_recording_settings(monkeypatch) -> None:
    monkeypatch.setenv("MCMOT_ENABLED", "0")
    monkeypatch.setenv("MATCHING_BROADCAST_RECORDING_ENABLED", "1")
    monkeypatch.setenv("MATCHING_BROADCAST_RECORDING_PATH", "output/matching/custom.jsonl")

    config = load_config()

    assert config.matching_broadcast.recording.enabled is True
    assert config.matching_broadcast.recording.path == "output/matching/custom.jsonl"


def test_load_config_requires_both_mcmot_config_paths_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("MCMOT_ENABLED", "1")
    monkeypatch.delenv("MCMOT_TRACKING_CONFIG_PATH", raising=False)
    monkeypatch.delenv("MCMOT_CAMERA_CONFIG_PATH", raising=False)

    with pytest.raises(RuntimeError, match="MCMOT_TRACKING_CONFIG_PATH"):
        load_config()
