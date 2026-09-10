"""Thread-safe in-memory trajectory storage for integration tasks.

The store uses a monotonically increasing watermark for each detection.  A
trajectory is identified by ``(camera_id, local_id)`` and its revision is
incremented for every detection belonging to that key.
"""

from __future__ import annotations

from collections import deque
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import math
import numbers
from threading import RLock
from typing import Any, Iterable, Literal, Mapping, Sequence, TypeAlias

from loguru import logger


TrajectoryPoint: TypeAlias = tuple[Any, float, float]
Point: TypeAlias = tuple[float, float]
BBox: TypeAlias = tuple[float, float, float, float]
ObjectKey: TypeAlias = tuple[str, str]
LifecycleState: TypeAlias = Literal["active", "idle", "grace", "expired", "removed"]

LOGGER = logger.bind(component=__name__)


class TimestampContractError(ValueError):
    """Raised when an App timestamp cannot satisfy the snapshot contract."""

    error_type = "timestamp_contract_error"

    def __init__(self, reason: str, *, path: str) -> None:
        self.reason = reason
        self.path = path
        super().__init__(f"{self.error_type}: {reason} at {path}")


def normalize_utc_timestamp(
    value: Any,
    *,
    path: str,
    required: bool = True,
) -> datetime | None:
    """Normalize a timestamp to UTC, rejecting ambiguous local time."""

    if value is None:
        if required:
            raise TimestampContractError("timestamp is required", path=path)
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise TimestampContractError(
                "naive datetime is not allowed; timezone-aware UTC input is required",
                path=path,
            )
        return value.astimezone(timezone.utc)

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise TimestampContractError("timestamp must be ISO-8601", path=path) from exc
        return normalize_utc_timestamp(parsed, path=path)

    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise TimestampContractError(
            "timestamp must be datetime, ISO-8601 string, or Unix seconds",
            path=path,
        )
    numeric = float(value)
    if not math.isfinite(numeric):
        raise TimestampContractError("timestamp must be finite", path=path)
    try:
        return datetime.fromtimestamp(numeric, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise TimestampContractError("timestamp is outside supported range", path=path) from exc


@dataclass(frozen=True, slots=True)
class DetectionEvent:
    """A detection accepted by :class:`TrajectoryStore`.

    ``bbox`` is ``(left, top, right, bottom)``.  The stored point is the
    bottom-center of the box, which is the useful contact point for scene
    trajectories.  Mappings with the same field names are accepted too.
    """

    camera_id: str
    local_id: str | int
    bbox: Sequence[float]
    timestamp: Any = None
    class_name: str | None = None
    score: float | None = None
    feature: Any = None
    updated_at: Any = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TrajectoryObject:
    """Immutable snapshot DTO for one camera/local-id pair."""

    camera_id: str
    local_id: str
    class_name: str | None
    first_seen_at: Any
    last_observed_at: Any
    last_updated_at: Any
    lifecycle_state: LifecycleState
    latest_timestamp: Any
    revision: int
    local_trajectory: tuple[TrajectoryPoint, ...]
    latest_metadata: Mapping[str, Any]
    latest_bbox: BBox
    latest_point: Point
    ema_point: Point
    latest_score: float | None
    latest_feature: Any
    last_watermark: int
    watermark: int

    @property
    def state(self) -> LifecycleState:
        """Compatibility alias for callers that use a short lifecycle name."""
        return self.lifecycle_state

    @property
    def raw_local_trajectory(self) -> tuple[TrajectoryPoint, ...]:
        """The unmodified local trajectory source of truth."""
        return self.local_trajectory


@dataclass(frozen=True, slots=True)
class TrajectorySnapshot:
    """A consistent view of objects changed after a supplied watermark."""

    objects: tuple[TrajectoryObject, ...]
    high_watermark: int


@dataclass(frozen=True, slots=True)
class LifecycleTransition:
    """A lifecycle transition emitted by :meth:`TrajectoryStore.sweep`."""

    camera_id: str
    local_id: str
    previous_state: LifecycleState
    state: LifecycleState
    at: Any


@dataclass(frozen=True, slots=True)
class TrajectorySweepResult:
    """Summary of a lifecycle sweep."""

    transitions: tuple[LifecycleTransition, ...]
    removed: tuple[ObjectKey, ...]


@dataclass(slots=True)
class _TrajectoryState:
    camera_id: str
    local_id: str
    revision: int
    class_name: str | None
    first_seen_at: Any
    last_observed_at: Any
    last_updated_at: Any
    lifecycle_state: LifecycleState
    latest_timestamp: Any
    local_trajectory: deque[TrajectoryPoint]
    latest_metadata: dict[str, Any]
    latest_bbox: BBox
    latest_point: Point
    ema_point: Point
    latest_score: float | None
    latest_feature: Any
    last_watermark: int


class TrajectoryStore:
    """An in-memory, thread-safe trajectory store.

    ``retention_seconds`` is the semantic trajectory retention window.
    ``max_points`` is only a safety cap.  Object record retention is managed
    independently by :meth:`sweep`; acknowledgement advances the consumer
    watermark and never removes object state.
    """

    def __init__(
        self,
        retention: int | None = None,
        ema_alpha: float | None = None,
        retention_seconds: float | None = None,
        max_points: int = 4096,
        idle_after_seconds: float = 5.0,
        grace_after_seconds: float = 15.0,
        expire_after_seconds: float = 30.0,
        remove_after_seconds: float = 60.0,
        max_removed_records: int = 1024,
    ) -> None:
        if retention is not None:
            # ``retention`` was the old fixed-point API.  Keep it as a
            # compatibility alias for the safety cap, while the time window
            # remains the primary retention semantic.
            if isinstance(retention, bool) or not isinstance(retention, int):
                raise TypeError("retention must be an integer")
            if retention <= 0:
                raise ValueError("retention must be a positive integer")
            max_points = retention
        if isinstance(max_points, bool) or not isinstance(max_points, int) or max_points <= 0:
            raise ValueError("max_points must be a positive integer")
        if retention_seconds is None:
            retention_seconds = 30.0
        if not math.isfinite(float(retention_seconds)) or float(retention_seconds) <= 0:
            raise ValueError("retention_seconds must be positive")
        thresholds = (
            idle_after_seconds,
            grace_after_seconds,
            expire_after_seconds,
            remove_after_seconds,
        )
        if any(not math.isfinite(float(value)) or float(value) < 0 for value in thresholds):
            raise ValueError("lifecycle thresholds must be finite and non-negative")
        if not idle_after_seconds <= grace_after_seconds <= expire_after_seconds <= remove_after_seconds:
            raise ValueError("lifecycle thresholds must be non-decreasing")
        if isinstance(max_removed_records, bool) or not isinstance(max_removed_records, int) or max_removed_records <= 0:
            raise ValueError("max_removed_records must be a positive integer")

        self._max_points = max_points
        self._retention_seconds = float(retention_seconds)
        # ``ema_alpha`` is accepted for API compatibility only.  Raw points
        # are never replaced by a smoothed value in this store.
        _ = ema_alpha
        self._idle_after_seconds = float(idle_after_seconds)
        self._grace_after_seconds = float(grace_after_seconds)
        self._expire_after_seconds = float(expire_after_seconds)
        self._remove_after_seconds = float(remove_after_seconds)
        self._max_removed_records = max_removed_records
        self._lock = RLock()
        self._objects: dict[ObjectKey, _TrajectoryState] = {}
        self._removed: deque[tuple[ObjectKey, Any]] = deque(maxlen=max_removed_records)
        self._watermark = 0
        self._acknowledged_watermark = 0

    def append_events(self, events: Iterable[DetectionEvent | Mapping[str, Any]]) -> int:
        """Append detections and return the resulting high watermark.

        Events are normalized before mutating the store, so malformed input
        cannot leave a partially appended batch behind.
        """

        normalized = [self._normalize_event(event) for event in events]
        if not normalized:
            with self._lock:
                return self._watermark

        with self._lock:
            for event in normalized:
                self._watermark += 1
                key = (event.camera_id, event.local_id)
                state = self._objects.get(key)
                if state is None:
                    point = self.point_from_bbox(event.bbox)
                    state = _TrajectoryState(
                        camera_id=event.camera_id,
                        local_id=event.local_id,
                        revision=0,
                        class_name=event.class_name,
                        first_seen_at=event.timestamp,
                        last_observed_at=event.timestamp,
                        last_updated_at=event.updated_at or self._now(),
                        lifecycle_state="active",
                        latest_timestamp=event.timestamp,
                        local_trajectory=deque(maxlen=self._max_points),
                        latest_metadata={},
                        latest_bbox=event.bbox,
                        latest_point=point,
                        ema_point=point,
                        latest_score=event.score,
                        latest_feature=deepcopy(event.feature),
                        last_watermark=0,
                    )
                    self._objects[key] = state

                point = self.point_from_bbox(event.bbox)
                state.revision += 1
                state.local_trajectory.append((event.timestamp, point[0], point[1]))
                self._prune_trajectory(state, event.timestamp)
                state.class_name = event.class_name or state.class_name
                if state.first_seen_at is None:
                    state.first_seen_at = event.timestamp
                state.last_observed_at = event.timestamp
                state.last_updated_at = event.updated_at or self._now()
                state.lifecycle_state = "active"
                state.latest_timestamp = event.timestamp
                state.latest_metadata = deepcopy(event.metadata)
                state.latest_bbox = event.bbox
                state.latest_point = point
                # Keep the legacy field aligned to the latest raw point.  No
                # EMA is applied, so raw trajectory remains lossless.
                state.ema_point = point
                state.latest_score = event.score
                state.latest_feature = deepcopy(event.feature)
                state.last_watermark = self._watermark

            return self._watermark

    def sweep(
        self,
        now: Any | None = None,
        *,
        idle_after_seconds: float | None = None,
        grace_after_seconds: float | None = None,
        expire_after_seconds: float | None = None,
        remove_after_seconds: float | None = None,
    ) -> TrajectorySweepResult:
        """Advance lifecycle state and reclaim records past record retention.

        This is explicit store maintenance, independent of MCMOT cadence.
        Trajectory point retention and object record retention are separate:
        points are pruned on append, while records are removed only here.
        """
        now = normalize_utc_timestamp(now or self._now(), path="sweep.now")
        assert now is not None
        thresholds = (
            self._idle_after_seconds if idle_after_seconds is None else float(idle_after_seconds),
            self._grace_after_seconds if grace_after_seconds is None else float(grace_after_seconds),
            self._expire_after_seconds if expire_after_seconds is None else float(expire_after_seconds),
            self._remove_after_seconds if remove_after_seconds is None else float(remove_after_seconds),
        )
        if any(value < 0 or not math.isfinite(value) for value in thresholds):
            raise ValueError("lifecycle thresholds must be finite and non-negative")
        if not thresholds[0] <= thresholds[1] <= thresholds[2] <= thresholds[3]:
            raise ValueError("lifecycle thresholds must be non-decreasing")

        transitions: list[LifecycleTransition] = []
        removed: list[ObjectKey] = []
        with self._lock:
            for key, state in list(self._objects.items()):
                # Point retention is evaluated independently from object
                # record retention, so a no-event maintenance cycle still
                # bounds trajectory memory.
                self._prune_trajectory(state, now)
                age = self._age_seconds(state.last_observed_at, now)
                if age <= thresholds[0]:
                    next_state: LifecycleState = "active"
                elif age <= thresholds[1]:
                    next_state = "idle"
                elif age <= thresholds[2]:
                    next_state = "grace"
                elif age <= thresholds[3]:
                    next_state = "expired"
                elif state.last_watermark <= self._acknowledged_watermark:
                    next_state = "removed"
                else:
                    # An expired record with an unacknowledged revision is
                    # retained for retry.  It can be reclaimed only after the
                    # consumer acknowledges that revision.
                    next_state = "expired"

                previous_state = state.lifecycle_state
                if next_state == "removed":
                    transitions.append(
                        LifecycleTransition(
                            camera_id=state.camera_id,
                            local_id=state.local_id,
                            previous_state=previous_state,
                            state="removed",
                            at=now,
                        )
                    )
                    state.lifecycle_state = "removed"
                    removed.append(key)
                    self._removed.append((key, now))
                    del self._objects[key]
                    continue

                if next_state != previous_state:
                    state.lifecycle_state = next_state
                    transitions.append(
                        LifecycleTransition(
                            camera_id=state.camera_id,
                            local_id=state.local_id,
                            previous_state=previous_state,
                            state=next_state,
                            at=now,
                        )
                    )
        return TrajectorySweepResult(tuple(transitions), tuple(removed))

    def maintenance(self, now: Any | None = None) -> TrajectorySweepResult:
        """Run lifecycle and retention maintenance independently of ingestion.

        The store intentionally has no knowledge of MCMOT cadence.  Its only
        deletion gate is the consumer acknowledgement watermark maintained by
        :meth:`ack`.
        """
        return self.sweep(now)

    def lifecycle_state(self, camera_id: str, local_id: str | int) -> LifecycleState:
        """Return current lifecycle state, including bounded removed tombstones."""
        key = (str(camera_id), str(local_id))
        with self._lock:
            state = self._objects.get(key)
            if state is not None:
                return state.lifecycle_state
            if any(removed_key == key for removed_key, _at in self._removed):
                return "removed"
            raise KeyError(key)

    def object_count(self) -> int:
        """Return live object-record count, excluding removed tombstones."""
        with self._lock:
            return len(self._objects)

    def snapshot_since(self, watermark: int) -> TrajectorySnapshot:
        """Return the latest state for objects revised after ``watermark``.

        The returned DTOs own their trajectory and metadata copies, so callers
        may inspect or modify them without racing with future appends.
        """

        watermark = self._validate_watermark(watermark)
        with self._lock:
            objects = tuple(
                self._snapshot_object(state)
                for state in self._objects.values()
                if state.last_watermark > watermark
            )
            return TrajectorySnapshot(objects=objects, high_watermark=self._watermark)

    def ack(self, watermark: int) -> int:
        """Advance the acknowledged watermark and return newly acked events.

        The watermark is clamped to the current high watermark.  Object state
        is deliberately not deleted, and a newer revision remains visible even
        when an older snapshot is acknowledged.
        """

        watermark = self._validate_watermark(watermark)
        with self._lock:
            target = min(watermark, self._watermark)
            if target <= self._acknowledged_watermark:
                return 0
            acknowledged = target - self._acknowledged_watermark
            self._acknowledged_watermark = target
            return acknowledged

    @property
    def acknowledged_watermark(self) -> int:
        """Return the highest revision acknowledged by the consumer."""
        with self._lock:
            return self._acknowledged_watermark

    @staticmethod
    def point_from_bbox(bbox: Sequence[float]) -> Point:
        """Calculate a bottom-center point from ``(left, top, right, bottom)``."""

        if len(bbox) != 4:
            raise ValueError("bbox must contain four coordinates")
        values = tuple(float(value) for value in bbox)
        if not all(math.isfinite(value) for value in values):
            raise ValueError("bbox coordinates must be finite")
        left, _top, right, bottom = values
        return ((left + right) / 2.0, bottom)

    @staticmethod
    def _validate_watermark(watermark: int) -> int:
        if isinstance(watermark, bool) or not isinstance(watermark, int):
            raise TypeError("watermark must be an integer")
        if watermark < 0:
            raise ValueError("watermark must be non-negative")
        return watermark

    def _prune_trajectory(self, state: _TrajectoryState, current_timestamp: Any) -> None:
        if not isinstance(current_timestamp, datetime):
            return
        cutoff = current_timestamp - timedelta(seconds=self._retention_seconds)
        retained = (
            point
            for point in state.local_trajectory
            if not isinstance(point[0], datetime) or point[0] >= cutoff
        )
        state.local_trajectory = deque(retained, maxlen=self._max_points)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _age_seconds(observed_at: Any, now: Any) -> float:
        if not isinstance(observed_at, datetime) or not isinstance(now, datetime):
            return 0.0
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise TimestampContractError("naive datetime is not allowed", path="last_observed_at")
        if now.tzinfo is None or now.utcoffset() is None:
            raise TimestampContractError("naive datetime is not allowed", path="sweep.now")
        return max(
            0.0,
            (now.astimezone(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds(),
        )

    @classmethod
    def _normalize_event(
        cls, event: DetectionEvent | Mapping[str, Any]
    ) -> DetectionEvent:
        if isinstance(event, DetectionEvent):
            camera_id = event.camera_id
            local_id = event.local_id
            bbox = event.bbox
            timestamp = event.timestamp
            class_name = event.class_name
            score = event.score
            feature = event.feature
            updated_at = event.updated_at
            metadata: dict[str, Any] = dict(event.metadata)
        elif isinstance(event, Mapping):
            data = dict(event)
            camera_id = data.pop("camera_id", data.pop("camera", None))
            local_id = data.pop("local_id", data.pop("track_id", data.pop("id", None)))
            bbox = data.pop("bbox", data.pop("box", None))
            timestamp = data.pop(
                "timestamp", data.pop("ts", data.pop("time", None))
            )
            class_name = data.pop("class_name", data.pop("label", None))
            score = data.pop("score", data.pop("confidence", None))
            feature = data.pop("feature", None)
            updated_at = data.pop("updated_at", data.pop("ingested_at", None))
            supplied_metadata = data.pop("metadata", data.pop("meta", None))
            metadata = dict(supplied_metadata) if isinstance(supplied_metadata, Mapping) else {}
            metadata.update(data)
        else:
            raise TypeError("events must contain DetectionEvent or mapping values")

        if camera_id is None or str(camera_id) == "":
            raise ValueError("camera_id is required")
        if local_id is None or str(local_id) == "":
            raise ValueError("local_id is required")
        if bbox is None:
            raise ValueError("bbox is required")

        normalized_bbox = cls._normalize_bbox(bbox)
        timestamp = cls._normalize_timestamp(timestamp, path="timestamp", required=True)
        updated_at = cls._normalize_timestamp(updated_at, path="updated_at", required=False)
        # An edge-provided global_id is an external annotation, never part of
        # the App's formal local-trajectory record.  Formal IDs are assigned by
        # Gallery after matching; dropping the annotation here also prevents a
        # legacy event from overriding a Gallery binding through metadata.
        metadata.pop("global_id", None)
        if timestamp is not None:
            metadata.setdefault("timestamp", deepcopy(timestamp))
        return DetectionEvent(
            camera_id=str(camera_id),
            local_id=str(local_id),
            bbox=normalized_bbox,
            timestamp=timestamp,
            class_name=str(class_name) if class_name is not None else None,
            score=float(score) if score is not None else None,
            feature=deepcopy(feature),
            updated_at=updated_at,
            metadata=deepcopy(metadata),
        )

    def append_edge_events(self, events: Iterable[Mapping[str, Any]]) -> int:
        """Append trackable detections from normalized edge events.

        Edge can legitimately emit detections without a tracker identity.  A
        detection without ``local_id``/``track_id`` cannot form a trajectory,
        so it is excluded at this edge-to-trajectory adapter boundary instead
        of being allowed to fail the whole ingestion batch.
        """

        detection_events: list[dict[str, Any]] = []
        skipped_by_camera: dict[str, int] = {}
        for event in events:
            event_data = dict(event)
            detections = event_data.pop("detections", None)
            if not detections:
                continue
            for detection_index, detection in enumerate(detections):
                if not isinstance(detection, Mapping):
                    camera_id = str(event.get("camera_id") or "<unknown>")
                    skipped_by_camera[camera_id] = skipped_by_camera.get(camera_id, 0) + 1
                    LOGGER.warning(
                        "skip edge detection without mapping shape "
                        f"camera_id={camera_id} session_id={event.get('session_id')} "
                        f"frame_seq={event.get('frame_seq')} detection_index={detection_index}"
                    )
                    continue

                local_id = self._resolve_edge_local_id(detection)
                if local_id is None:
                    camera_id = str(event.get("camera_id") or "<unknown>")
                    skipped_by_camera[camera_id] = skipped_by_camera.get(camera_id, 0) + 1
                    LOGGER.warning(
                        "skip untracked edge detection "
                        f"camera_id={camera_id} session_id={event.get('session_id')} "
                        f"frame_seq={event.get('frame_seq')} detection_index={detection_index} "
                        f"class_name={detection.get('class_name') or detection.get('label') or '<unknown>'}"
                    )
                    continue

                merged = dict(event_data)
                merged.update(detection)
                # Store the resolved identity explicitly.  This also handles
                # payloads that contain ``local_id=None`` alongside a valid
                # ``track_id`` alias.
                merged["local_id"] = local_id
                merged["timestamp"] = event.get("capture_ts") or event.get("timestamp")
                merged["camera_id"] = event.get("camera_id")
                merged["metadata"] = {
                    "session_id": event.get("session_id"),
                    "frame_seq": event.get("frame_seq"),
                    "models": deepcopy(event.get("models") or []),
                }
                detection_events.append(merged)

        if skipped_by_camera:
            LOGGER.warning(f"skipped untrackable edge detections by camera={skipped_by_camera}")
        return self.append_events(detection_events)

    @staticmethod
    def _resolve_edge_local_id(detection: Mapping[str, Any]) -> Any:
        """Return the first non-empty identity alias from an edge detection."""

        for field_name in ("local_id", "track_id", "id"):
            value = detection.get(field_name)
            if value is not None and str(value).strip():
                return value
        return None

    @staticmethod
    def _normalize_bbox(bbox: Sequence[float]) -> BBox:
        try:
            values = tuple(float(value) for value in bbox)
        except (TypeError, ValueError) as exc:
            raise ValueError("bbox must contain numeric coordinates") from exc
        if len(values) != 4:
            raise ValueError("bbox must contain four coordinates")
        if not all(math.isfinite(value) for value in values):
            raise ValueError("bbox coordinates must be finite")
        return values  # type: ignore[return-value]

    @staticmethod
    def _normalize_timestamp(
        value: Any,
        *,
        path: str,
        required: bool,
    ) -> datetime | None:
        return normalize_utc_timestamp(value, path=path, required=required)

    @staticmethod
    def _snapshot_object(state: _TrajectoryState) -> TrajectoryObject:
        return TrajectoryObject(
            camera_id=state.camera_id,
            local_id=state.local_id,
            class_name=state.class_name,
            first_seen_at=deepcopy(state.first_seen_at),
            last_observed_at=deepcopy(state.last_observed_at),
            last_updated_at=deepcopy(state.last_updated_at),
            lifecycle_state=state.lifecycle_state,
            latest_timestamp=deepcopy(state.latest_timestamp),
            revision=state.revision,
            local_trajectory=tuple(state.local_trajectory),
            latest_metadata=deepcopy(state.latest_metadata),
            latest_bbox=state.latest_bbox,
            latest_point=state.latest_point,
            ema_point=state.ema_point,
            latest_score=state.latest_score,
            latest_feature=deepcopy(state.latest_feature),
            last_watermark=state.last_watermark,
            watermark=state.last_watermark,
        )


__all__ = [
    "BBox",
    "DetectionEvent",
    "ObjectKey",
    "Point",
    "LifecycleState",
    "LifecycleTransition",
    "TrajectoryPoint",
    "TrajectoryObject",
    "TrajectorySnapshot",
    "TrajectorySweepResult",
    "TrajectoryStore",
    "TimestampContractError",
    "normalize_utc_timestamp",
]
