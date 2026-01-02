"""Application configuration and scheduling windows."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time, timezone, timedelta
import os
from typing import Dict, List, Optional, Tuple

try:  # Python 3.9+ has zoneinfo
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback for Python 3.8
    ZoneInfo = None  # type: ignore

from integration.mcmot.config.loader import load_mcmot_config
from integration.mcmot.config.schema import BaseConfig as MCMOTConfig


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _env_csv(name: str) -> tuple[str, ...]:
    raw = os.getenv(name)
    if not raw:
        return tuple()
    parts = [part.strip() for part in raw.split(",")]
    return tuple(part for part in parts if part)


def _parse_hex_color(value: str | None) -> tuple[int, int, int] | None:
    if not value:
        return None
    try:
        hex_value = value.strip().lstrip("#")
        if len(hex_value) != 6:
            return None
        r = int(hex_value[0:2], 16)
        g = int(hex_value[2:4], 16)
        b = int(hex_value[4:6], 16)
        return (b, g, r)
    except ValueError:
        return None


def _env_color(name: str) -> tuple[int, int, int] | None:
    raw = os.getenv(name)
    return _parse_hex_color(raw) if raw else None


def _env_class_colors() -> Dict[str, tuple[int, int, int]]:
    raw = os.getenv("GLOBAL_MAP_VIS_CLASS_COLORS")
    if not raw:
        return {}
    result: Dict[str, tuple[int, int, int]] = {}
    for entry in raw.split(","):
        if not entry.strip():
            continue
        if ":" not in entry:
            continue
        key, color_str = entry.split(":", 1)
        color = _parse_hex_color(color_str)
        if color is None:
            continue
        result[key.strip()] = color
    return result


@dataclass
class GlobalMapVisualizationConfig:
    enabled: bool = _env_bool("GLOBAL_MAP_VIS_ENABLED", False)
    mode: str = os.getenv("GLOBAL_MAP_VIS_MODE", "write").strip().lower()
    output_dir: str = os.getenv("GLOBAL_MAP_VIS_OUTPUT", "output/global_map")
    window_name: str = os.getenv("GLOBAL_MAP_VIS_WINDOW", "global-map")
    marker_radius: int = int(os.getenv("GLOBAL_MAP_VIS_RADIUS", "6"))
    label_font_scale: float = float(os.getenv("GLOBAL_MAP_VIS_LABEL_SCALE", "0.5"))
    label_thickness: int = int(os.getenv("GLOBAL_MAP_VIS_LABEL_THICKNESS", "1"))
    show_global_id: bool = _env_bool("GLOBAL_MAP_VIS_SHOW_ID", True)
    show_class_name: bool = _env_bool("GLOBAL_MAP_VIS_SHOW_CLASS", False)
    local_camera_ids: tuple[str, ...] = _env_csv("GLOBAL_MAP_VIS_CAMERAS")
    show_legend: bool = _env_bool("GLOBAL_MAP_VIS_SHOW_LEGEND", True)
    global_color: tuple[int, int, int] | None = field(default_factory=lambda: _env_color("GLOBAL_MAP_VIS_GLOBAL_COLOR"))
    class_colors: Dict[str, tuple[int, int, int]] = field(default_factory=_env_class_colors)
    global_radius_ratio: float = float(os.getenv("GLOBAL_MAP_VIS_GLOBAL_RADIUS_RATIO", "0.008"))
    local_radius_ratio: float = float(os.getenv("GLOBAL_MAP_VIS_LOCAL_RADIUS_RATIO", "0.004"))


@dataclass
class FormatTaskConfig:
    enabled: bool = _env_bool("FORMAT_TASK_ENABLED", True)
    strategy_class: str | None = os.getenv("FORMAT_STRATEGY_CLASS")


@dataclass
class IngestionTaskConfig:
    handler_class: str | None = os.getenv("INGESTION_HANDLER_CLASS")


@dataclass
class TrackingTaskConfig:
    engine_class: str | None = os.getenv("TRACKING_ENGINE_CLASS")


@dataclass
class RulesConfig:
    """Rules stage customization options."""

    engine_class: str | None = os.getenv("RULES_ENGINE_CLASS")
    detail: str | None = os.getenv("RULES_DETAIL")


@dataclass(frozen=True)
class ScheduleWindow:
    """Represents a working-hour window in local time."""

    start: time
    end: time

    def contains(self, current: time) -> bool:
        return self.start <= current < self.end


@dataclass
class AppConfig:
    """Centralized configuration for the integration daemon."""

    working_windows: List[ScheduleWindow] = field(
        default_factory=lambda: [
            ScheduleWindow(start=time(0, 0), end=time(23, 59)),
        ]
    )
    timezone: timezone = timezone.utc
    loop_interval_seconds: float = float(os.getenv("LOOP_INTERVAL_SECONDS", "5"))
    non_working_idle_seconds: float = float(os.getenv("NON_WORKING_IDLE_SECONDS", "30"))
    retry_backoff_seconds: float = float(os.getenv("RETRY_BACKOFF_SECONDS", "10"))
    edge_event_host: str = os.getenv("EDGE_EVENT_HOST", "0.0.0.0")
    edge_event_port: int = int(os.getenv("EDGE_EVENT_PORT", "9000"))
    edge_event_max_age_seconds: float = float(os.getenv("EDGE_EVENT_MAX_AGE", "5"))
    monitor_endpoint: str | None = os.getenv("MONITOR_ENDPOINT")
    monitor_service_name: str = (
        os.getenv("INTEGRATION_MONITOR_SERVICE_NAME")
        or os.getenv("MONITOR_SERVICE_NAME")
        or "integration-daemon"
    )
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    mcmot_enabled: bool = _env_bool("MCMOT_ENABLED", True)
    mcmot_config_path: str = os.getenv(
        "MCMOT_CONFIG_PATH",
        "data/config/mcmot.config.yaml",
    )
    mcmot: Optional[MCMOTConfig] = field(default=None, repr=False)
    global_map_visualization: GlobalMapVisualizationConfig = field(default_factory=GlobalMapVisualizationConfig)
    ingestion_task: IngestionTaskConfig = field(default_factory=IngestionTaskConfig)
    tracking_task: TrackingTaskConfig = field(default_factory=TrackingTaskConfig)
    format_task: FormatTaskConfig = field(default_factory=FormatTaskConfig)
    rules: RulesConfig = field(default_factory=RulesConfig)


def load_config() -> AppConfig:
    """Load configuration, including timezone, from environment."""

    tz_name = os.getenv("APP_TIMEZONE", "Asia/Taipei")

    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:  # pragma: no cover - fallback path
            tz = timezone.utc
    else:  # Python 3.8 fallback using fixed offset heuristics
        if tz_name in {"Asia/Taipei", "Asia/Shanghai", "Asia/Hong_Kong"}:
            tz = timezone(timedelta(hours=8))
        else:
            tz = timezone.utc

    config = AppConfig(timezone=tz)
    config.log_level = config.log_level.upper()
    if config.mcmot_enabled:
        config.mcmot = load_mcmot_config(config.mcmot_config_path)
    return config
