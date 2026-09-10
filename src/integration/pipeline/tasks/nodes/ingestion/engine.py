"""Ingestion engine implementations."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Type

from smart_workflow import TaskContext

from integration.pipeline.tasks.plugin_loader import load_plugin_class


@dataclass
class IngestionResult:
    """Normalized ingestion output."""

    events: List[Dict[str, Any]]
    raw_count: int
    dropped: int
    duplicate_count: int = 0
    has_new_data: bool = False
    dirty_camera_ids: List[str] = field(default_factory=list)
    drop_reasons: Dict[str, int] = field(default_factory=dict)


class BaseIngestionEngine(ABC):
    """Base interface for ingestion engines."""

    def __init__(self, context: TaskContext | None = None) -> None:
        self._context = context

    @abstractmethod
    def process(self, context: TaskContext, raw_events: List[Dict[str, Any]]) -> IngestionResult:
        """Return deduplicated events and stats."""


class DefaultIngestionEngine(BaseIngestionEngine):
    """Default normalization/dedup logic."""

    def __init__(self, context: TaskContext | None = None) -> None:
        super().__init__(context)
        self._seen_event_identities: OrderedDict[
            tuple[str, tuple[str, str, str]], datetime
        ] = OrderedDict()
        if context is None:
            self._max_age_seconds = None
            self._dedup_ttl_seconds = 60.0
            self._dedup_max_entries = 4096
        else:
            edge_cfg = getattr(context.config, "edge_events", None)
            self._max_age_seconds = getattr(edge_cfg, "max_age_seconds", None)
            if self._max_age_seconds is None:
                self._max_age_seconds = getattr(context.config, "edge_event_max_age_seconds", None)
            self._dedup_ttl_seconds = float(
                getattr(edge_cfg, "dedup_ttl_seconds", self._max_age_seconds or 60.0)
            )
            self._dedup_max_entries = int(
                getattr(edge_cfg, "dedup_max_entries", 4096)
            )
        if self._dedup_ttl_seconds <= 0 or self._dedup_max_entries <= 0:
            raise ValueError("dedup TTL and max entries must be positive")

    def process(self, context: TaskContext, raw_events: List[Dict[str, Any]]) -> IngestionResult:
        edge_cfg = getattr(context.config, "edge_events", None)
        configured_max_age = getattr(edge_cfg, "max_age_seconds", None)
        if configured_max_age is None:
            configured_max_age = getattr(context.config, "edge_event_max_age_seconds", 5)
        max_age_seconds = (
            self._max_age_seconds
            if self._max_age_seconds is not None
            else configured_max_age
        )
        max_age = timedelta(seconds=max_age_seconds)
        now = datetime.now(timezone.utc)
        self._prune_event_identities(now)

        dropped = 0
        duplicate_count = 0
        deduped_events: List[Dict[str, Any]] = []
        dirty_camera_ids: List[str] = []
        drop_reasons: Dict[str, int] = {}
        for item in raw_events:
            parsed, drop_reason = self._normalize_event_with_reason(item, now, max_age)
            if parsed is None:
                dropped += 1
                if drop_reason is not None:
                    drop_reasons[drop_reason] = drop_reasons.get(drop_reason, 0) + 1
                continue
            camera_id = parsed["camera_id"]
            frame_identity = self._build_event_identity(parsed)
            identity_key = (camera_id, frame_identity)
            if identity_key in self._seen_event_identities:
                duplicate_count += 1
                self._seen_event_identities.move_to_end(identity_key)
                self._seen_event_identities[identity_key] = now
                continue
            self._seen_event_identities[identity_key] = now
            while len(self._seen_event_identities) > self._dedup_max_entries:
                self._seen_event_identities.popitem(last=False)
            if camera_id not in dirty_camera_ids:
                dirty_camera_ids.append(camera_id)
            deduped_events.append(parsed)

        return IngestionResult(
            events=deduped_events,
            raw_count=len(raw_events),
            dropped=dropped,
            duplicate_count=duplicate_count,
            has_new_data=bool(deduped_events),
            dirty_camera_ids=dirty_camera_ids,
            drop_reasons=drop_reasons,
        )

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
            except Exception:  # pylint: disable=broad-except
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed
        return None

    @staticmethod
    def _normalize_event(
        item: Dict[str, Any],
        now: datetime,
        max_age: timedelta,
    ) -> Dict[str, Any] | None:
        normalized, _reason = DefaultIngestionEngine._normalize_event_with_reason(
            item,
            now,
            max_age,
        )
        return normalized

    @staticmethod
    def _normalize_event_with_reason(
        item: Any,
        now: datetime,
        max_age: timedelta,
    ) -> tuple[Dict[str, Any] | None, str | None]:
        """Normalize one event and retain the reason when policy drops it."""

        if not isinstance(item, Mapping):
            return None, "invalid_event_schema"

        camera_id = item.get("camera_id")
        timestamp_str = item.get("timestamp")
        if not camera_id:
            return None, "missing_camera_id"
        if not timestamp_str:
            return None, "missing_timestamp"
        event_time = DefaultIngestionEngine._parse_timestamp(timestamp_str)
        if event_time is None:
            return None, "invalid_timestamp"
        if now - event_time > max_age:
            return None, "stale_event"
        capture_ts = DefaultIngestionEngine._parse_timestamp(item.get("capture_ts")) or event_time
        session_id = item.get("session_id")
        if session_id is not None and not isinstance(session_id, str):
            session_id = str(session_id)
        frame_seq = item.get("frame_seq")
        if isinstance(frame_seq, str):
            try:
                frame_seq = int(frame_seq)
            except ValueError:
                frame_seq = None
        elif not isinstance(frame_seq, int) or frame_seq <= 0:
            frame_seq = None
        detections = item.get("detections") or []
        models = item.get("models") or []
        return {
            "camera_id": camera_id,
            "timestamp": event_time,
            "capture_ts": capture_ts,
            "session_id": session_id,
            "frame_seq": frame_seq,
            "detections": detections,
            "models": models,
        }, None

    @staticmethod
    def _is_more_recent(candidate: Dict[str, Any], current: Dict[str, Any]) -> bool:
        candidate_session = candidate.get("session_id")
        current_session = current.get("session_id")
        candidate_seq = candidate.get("frame_seq")
        current_seq = current.get("frame_seq")
        if (
            isinstance(candidate_session, str)
            and isinstance(current_session, str)
            and candidate_session == current_session
            and isinstance(candidate_seq, int)
            and isinstance(current_seq, int)
        ):
            return candidate_seq > current_seq

        candidate_time = candidate.get("capture_ts") or candidate["timestamp"]
        current_time = current.get("capture_ts") or current["timestamp"]
        if candidate_time != current_time:
            return candidate_time > current_time
        if isinstance(candidate_seq, int) and isinstance(current_seq, int):
            return candidate_seq > current_seq
        return False

    @staticmethod
    def _build_event_identity(event: Dict[str, Any]) -> tuple[str, str, str]:
        session_id = str(event.get("session_id") or "")
        frame_seq = event.get("frame_seq")
        event_time = event.get("capture_ts") or event["timestamp"]
        event_time_key = event_time.isoformat()
        if session_id and isinstance(frame_seq, int):
            return ("frame", session_id, f"{frame_seq}:{event_time_key}")
        return ("legacy", str(event["camera_id"]), event_time_key)

    def _prune_event_identities(self, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self._dedup_ttl_seconds)
        while self._seen_event_identities:
            _identity, seen_at = next(iter(self._seen_event_identities.items()))
            if seen_at >= cutoff:
                break
            self._seen_event_identities.popitem(last=False)


def load_ingestion_engine(path: str) -> Type[BaseIngestionEngine]:
    return load_plugin_class(path, BaseIngestionEngine, "Ingestion Engine")
