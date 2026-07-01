"""Application configuration and scheduling windows."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time, timezone, timedelta
import os
from typing import Dict, List

from .visualization import (
    GlobalMapVisualizationConfig,
    load_global_map_visualization_config,
)

try:  # Python 3.9+ has zoneinfo
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback for Python 3.8
    ZoneInfo = None  # type: ignore


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _env_path(name: str) -> str | None:
    raw = os.getenv(name)
    return raw.strip() if raw and raw.strip() else None


def _phase_publish_backend() -> str:
    override = (os.getenv("PHASE_PUBLISH_BACKEND") or "").strip().lower()
    if override:
        return override
    return (os.getenv("EDGE_EVENT_BACKEND") or "mqtt").strip().lower()


def _env_pipeline_tasks() -> Dict[str, str]:
    raw = os.getenv("PIPELINE_TASK_CLASSES")
    if not raw:
        return {}
    result: Dict[str, str] = {}
    for entry in raw.split(","):
        if not entry.strip():
            continue
        if "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        result[key] = value
    return result


def _env_pipeline_sleep() -> Dict[str, float]:
    raw = os.getenv("PIPELINE_SLEEP_SECONDS")
    if not raw:
        return {}
    result: Dict[str, float] = {}
    for entry in raw.split(","):
        if not entry.strip():
            continue
        if "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        try:
            result[key] = float(value)
        except ValueError:
            continue
    return result


def _edge_events_topic() -> str:
    return (
        os.getenv("EDGE_EVENTS_TOPIC")
        or os.getenv("EDGE_EVENTS_MQTT_TOPIC")
        or "edge/events"
    ).strip()


def _phase_topic() -> str:
    return (
        os.getenv("PHASE_TOPIC")
        or os.getenv("PHASE_MQTT_TOPIC")
        or "integration/phase"
    ).strip()


def _matching_broadcast_backend() -> str:
    override = (os.getenv("MATCHING_BROADCAST_BACKEND") or "").strip().lower()
    if override:
        return override
    return _phase_publish_backend()


def _matching_broadcast_topic() -> str:
    return (
        os.getenv("MATCHING_BROADCAST_TOPIC")
        or os.getenv("MATCHING_BROADCAST_CHANNEL")
        or "integration/matching"
    ).strip()


def _matching_broadcast_record_path() -> str:
    return (
        os.getenv("MATCHING_BROADCAST_RECORDING_PATH")
        or os.getenv("MATCHING_BROADCAST_RECORD_PATH")
        or os.getenv("MATCHING_BROADCAST_OBSERVATION_PATH")
        or "output/matching/matching_observations.jsonl"
    ).strip()


@dataclass
class FormatTaskConfig:
    enabled: bool = _env_bool("FORMAT_TASK_ENABLED", True)
    strategy_class: str | None = os.getenv("FORMAT_STRATEGY_CLASS")


@dataclass
class IngestionTaskConfig:
    engine_class: str | None = os.getenv("INGESTION_ENGINE_CLASS") or os.getenv("INGESTION_HANDLER_CLASS")


@dataclass
class PhaseTaskConfig:
    engine_class: str | None = os.getenv("PHASE_ENGINE_CLASS")


@dataclass
class SchedulerConfig:
    engine_class: str | None = os.getenv("SCHEDULER_ENGINE_CLASS")


@dataclass
class MqttConfig:
    enabled: bool = _env_bool("MQTT_ENABLED", False)
    host: str = os.getenv("MQTT_HOST", "localhost")
    port: int = int(os.getenv("MQTT_PORT", "1883"))
    qos: int = int(os.getenv("MQTT_QOS", "1"))
    retain: bool = _env_bool("MQTT_RETAIN", True)
    auth_enabled: bool = _env_bool("MQTT_AUTH_ENABLED", False)
    username: str | None = os.getenv("MQTT_USERNAME")
    password: str | None = os.getenv("MQTT_PASSWORD")
    heartbeat_seconds: int = int(os.getenv("PHASE_HEARTBEAT_SECONDS", "600"))
    client_id: str | None = os.getenv("MQTT_CLIENT_ID")


@dataclass
class EdgeEventMessagingConfig:
    backend: str = os.getenv("EDGE_EVENT_BACKEND", "http").strip().lower()
    channel: str = _edge_events_topic()
    host: str = os.getenv("EDGE_EVENT_HOST", "0.0.0.0")
    port: int = int(os.getenv("EDGE_EVENT_PORT", "9000"))
    max_age_seconds: float = float(os.getenv("EDGE_EVENT_MAX_AGE", "5"))


@dataclass
class PhaseMessagingConfig:
    enabled: bool = _env_bool("PHASE_BROADCAST_ENABLED", True)
    backend: str = _phase_publish_backend()
    channel: str = _phase_topic()
    heartbeat_seconds: int = int(os.getenv("PHASE_HEARTBEAT_SECONDS", "600"))


@dataclass
class MatchingBroadcastRecordingConfig:
    enabled: bool = field(default_factory=lambda: _env_bool("MATCHING_BROADCAST_RECORDING_ENABLED", False))
    path: str = field(default_factory=_matching_broadcast_record_path)


@dataclass
class MatchingBroadcastConfig:
    enabled: bool = field(default_factory=lambda: _env_bool("MATCHING_BROADCAST_ENABLED", False))
    backend: str = field(default_factory=_matching_broadcast_backend)
    channel: str = field(default_factory=_matching_broadcast_topic)
    recording: MatchingBroadcastRecordingConfig = field(default_factory=MatchingBroadcastRecordingConfig)


@dataclass
class PhaseHttpConfig:
    base_url: str = (os.getenv("PHASE_HTTP_BASE_URL") or "").strip()
    timeout_seconds: float = float(os.getenv("PHASE_HTTP_TIMEOUT_SECONDS", "5"))


@dataclass
class RulesConfig:
    """Rules stage customization options."""

    engine_class: str | None = os.getenv("RULES_ENGINE_CLASS")
    detail: str | None = os.getenv("RULES_DETAIL")


@dataclass
class EventDispatchConfig:
    engine_class: str | None = os.getenv("EVENT_DISPATCH_ENGINE_CLASS")


@dataclass
class PhaseChangeConfig:
    engine_class: str | None = os.getenv("PHASE_CHANGE_ENGINE_CLASS")


@dataclass
class PipelineManagerConfig:
    selector_class: str | None = os.getenv("PIPELINE_SELECTOR_CLASS")
    task_classes: Dict[str, str] = field(default_factory=_env_pipeline_tasks)
    sleep_seconds: Dict[str, float] = field(default_factory=_env_pipeline_sleep)


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
    pipeline_summary_interval_seconds: float = float(os.getenv("PIPELINE_SUMMARY_INTERVAL_SECONDS", "60"))
    non_working_idle_seconds: float = float(os.getenv("NON_WORKING_IDLE_SECONDS", "30"))
    retry_backoff_seconds: float = float(os.getenv("RETRY_BACKOFF_SECONDS", "10"))
    edge_event_host: str = os.getenv("EDGE_EVENT_HOST", "0.0.0.0")
    edge_event_port: int = int(os.getenv("EDGE_EVENT_PORT", "9000"))
    edge_event_max_age_seconds: float = float(os.getenv("EDGE_EVENT_MAX_AGE", "5"))
    pipeline_schedule_path: str | None = _env_path("PIPELINE_SCHEDULE_PATH")
    monitor_endpoint: str | None = os.getenv("MONITOR_ENDPOINT")
    monitor_service_name: str = (
        os.getenv("INTEGRATION_MONITOR_SERVICE_NAME")
        or os.getenv("MONITOR_SERVICE_NAME")
        or "integration-daemon"
    )
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    mcmot_enabled: bool = _env_bool("MCMOT_ENABLED", True)
    global_map_visualization_enabled: bool = _env_bool("GLOBAL_MAP_VIS_ENABLED", False)
    global_map_visualization_config_path: str | None = _env_path("GLOBAL_MAP_VIS_CONFIG_PATH")
    global_map_visualization: GlobalMapVisualizationConfig | None = None
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    edge_events: EdgeEventMessagingConfig = field(default_factory=EdgeEventMessagingConfig)
    phase_messaging: PhaseMessagingConfig = field(default_factory=PhaseMessagingConfig)
    matching_broadcast: MatchingBroadcastConfig = field(default_factory=MatchingBroadcastConfig)
    phase_http: PhaseHttpConfig = field(default_factory=PhaseHttpConfig)
    mcmot_config_path: str = _env_path("MCMOT_CONFIG_PATH") or "../MCMOT/configs/road_config.yaml"
    ingestion_task: IngestionTaskConfig = field(default_factory=IngestionTaskConfig)
    phase_task: PhaseTaskConfig = field(default_factory=PhaseTaskConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    format_task: FormatTaskConfig = field(default_factory=FormatTaskConfig)
    rules: RulesConfig = field(default_factory=RulesConfig)
    event_dispatch: EventDispatchConfig = field(default_factory=EventDispatchConfig)
    phase_change: PhaseChangeConfig = field(default_factory=PhaseChangeConfig)
    pipeline: PipelineManagerConfig = field(default_factory=PipelineManagerConfig)

    @property
    def edge_event_backend(self) -> str:
        return self.edge_events.backend

    @property
    def edge_event_topic(self) -> str:
        return self.edge_events.channel

    @property
    def phase_publish(self) -> PhaseMessagingConfig:
        return self.phase_messaging


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
    config.mcmot_enabled = _env_bool("MCMOT_ENABLED", config.mcmot_enabled)
    config.mcmot_config_path = _env_path("MCMOT_CONFIG_PATH") or config.mcmot_config_path
    config.global_map_visualization_enabled = _env_bool(
        "GLOBAL_MAP_VIS_ENABLED",
        config.global_map_visualization_enabled,
    )
    config.global_map_visualization_config_path = _env_path("GLOBAL_MAP_VIS_CONFIG_PATH")
    config.log_level = config.log_level.upper()
    config.edge_event_host = config.edge_events.host
    config.edge_event_port = config.edge_events.port
    config.edge_event_max_age_seconds = config.edge_events.max_age_seconds
    if config.global_map_visualization_enabled:
        if not config.global_map_visualization_config_path:
            raise RuntimeError(
                "已啟用全局地圖視覺化但未設定 GLOBAL_MAP_VIS_CONFIG_PATH"
            )
        try:
            config.global_map_visualization = load_global_map_visualization_config(
                config.global_map_visualization_config_path,
            )
        except Exception as exc:  # pragma: no cover - propagated as startup error
            raise RuntimeError(
                f"無法載入全局地圖視覺化設定：{config.global_map_visualization_config_path}"
            ) from exc
    return config
