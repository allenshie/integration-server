"""Payload schema for matching broadcast v2."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar, Iterable, Mapping

from .constants import MATCHING_BROADCAST_MESSAGE_TYPE, MATCHING_BROADCAST_SCHEMA_VERSION


@dataclass
class MatchingBroadcastPoint:
    x: float
    y: float

    def to_dict(self) -> dict[str, float]:
        return {"x": self.x, "y": self.y}


@dataclass
class MatchingBroadcastLocalObject:
    camera_id: str
    local_id: int
    global_id: int | str | None
    global_position: MatchingBroadcastPoint
    class_name: str = "unknown"
    bbox: list[int] | None = None
    score: float | None = None
    timestamp: str | None = None

    @classmethod
    def from_mapping(cls, item: Mapping[str, Any]) -> MatchingBroadcastLocalObject | None:
        camera_id = _normalize_text(item.get("camera_id"))
        local_id = _coerce_non_negative_int(item.get("local_id"))
        global_position = _extract_point(item.get("global_position"))
        if camera_id is None or local_id is None or global_position is None:
            return None

        return cls(
            camera_id=camera_id,
            local_id=local_id,
            global_id=_coerce_global_id(item.get("global_id"), allow_none=True),
            global_position=global_position,
            class_name=_normalize_text(item.get("class_name")) or "unknown",
            bbox=_coerce_bbox(item.get("bbox")),
            score=_coerce_float(item.get("score")),
            timestamp=_format_timestamp(item.get("timestamp")),
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "camera_id": self.camera_id,
            "local_id": self.local_id,
            "global_id": self.global_id,
            "global_position": self.global_position.to_dict(),
            "class_name": self.class_name,
        }
        if self.bbox is not None:
            payload["bbox"] = list(self.bbox)
        if self.score is not None:
            payload["score"] = self.score
        if self.timestamp is not None:
            payload["timestamp"] = self.timestamp
        return payload


@dataclass
class MatchingBroadcastGlobalObject:
    global_id: int | str
    global_position: MatchingBroadcastPoint
    class_name: str = "unknown"
    matched_locals: list[MatchingBroadcastLocalObject] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "global_id": self.global_id,
            "global_position": self.global_position.to_dict(),
            "class_name": self.class_name,
            "matched_locals": [item.to_dict() for item in self.matched_locals],
        }


@dataclass
class MatchingBroadcastBatch:
    session_id: str
    frame_seq: int
    capture_ts: str | None
    tracked_objects: list[dict[str, Any]] = field(default_factory=list)
    global_objects: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class MatchingBroadcastSnapshot:
    schema_version: ClassVar[int] = MATCHING_BROADCAST_SCHEMA_VERSION
    message_type: ClassVar[str] = MATCHING_BROADCAST_MESSAGE_TYPE

    generated_at: str
    session_id: str
    frame_seq: int
    capture_ts: str | None
    objects: list[MatchingBroadcastGlobalObject] = field(default_factory=list)

    @classmethod
    def from_batch(
        cls,
        batch: MatchingBroadcastBatch,
        generated_at: datetime | None = None,
    ) -> MatchingBroadcastSnapshot:
        tracked_objects = _index_locals_by_global_id(batch.tracked_objects)
        objects: list[MatchingBroadcastGlobalObject] = []

        for item in batch.global_objects:
            global_id = _coerce_global_id(item.get("global_id"), allow_none=False)
            global_position = _extract_global_position(item)
            if global_id is None or global_position is None:
                continue

            matched_locals = sorted(
                tracked_objects.get(_global_id_token(global_id), []),
                key=lambda local: (local.camera_id, local.local_id),
            )
            objects.append(
                MatchingBroadcastGlobalObject(
                    global_id=global_id,
                    global_position=global_position,
                    class_name=_normalize_text(item.get("class_name")) or "unknown",
                    matched_locals=matched_locals,
                ),
            )

        objects.sort(key=lambda item: _global_id_sort_key(item.global_id))
        return cls(
            generated_at=_format_timestamp(generated_at or datetime.now(timezone.utc)) or "",
            session_id=batch.session_id,
            frame_seq=batch.frame_seq,
            capture_ts=batch.capture_ts,
            objects=objects,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "message_type": self.message_type,
            "generated_at": self.generated_at,
            "session_id": self.session_id,
            "frame_seq": self.frame_seq,
            "capture_ts": self.capture_ts,
            "objects": [item.to_dict() for item in self.objects],
        }


def _index_locals_by_global_id(
    tracked_objects: Iterable[Mapping[str, Any]],
) -> dict[tuple[str, str], list[MatchingBroadcastLocalObject]]:
    grouped: dict[tuple[str, str], list[MatchingBroadcastLocalObject]] = {}
    for item in tracked_objects:
        global_id = _coerce_global_id(item.get("global_id"), allow_none=True)
        if global_id is None:
            continue
        local_object = MatchingBroadcastLocalObject.from_mapping(item)
        if local_object is None:
            continue
        grouped.setdefault(_global_id_token(global_id), []).append(local_object)
    return grouped


def _extract_global_position(item: Mapping[str, Any]) -> MatchingBroadcastPoint | None:
    point = _extract_point(item.get("global_position"))
    if point is not None:
        return point

    trajectory = item.get("trajectory")
    if isinstance(trajectory, list) and trajectory:
        last_point = trajectory[-1]
        if isinstance(last_point, Mapping):
            return _extract_point(last_point)
    return None


def _extract_point(value: Any) -> MatchingBroadcastPoint | None:
    if not isinstance(value, Mapping):
        return None

    x = _coerce_float(value.get("x"))
    y = _coerce_float(value.get("y"))
    if x is None or y is None:
        return None
    return MatchingBroadcastPoint(x=x, y=y)


def _coerce_bbox(value: Any) -> list[int] | None:
    if not isinstance(value, list):
        return None

    result: list[int] = []
    for item in value:
        converted = _coerce_non_negative_int(item)
        if converted is None:
            return None
        result.append(converted)
    return result


def _coerce_non_negative_int(value: Any) -> int | None:
    try:
        converted = int(value)
    except (TypeError, ValueError):
        return None
    return converted if converted >= 0 else None


def _coerce_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_global_id(value: Any, *, allow_none: bool) -> int | str | None:
    if value is None:
        return None if allow_none else None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return value
    text = _normalize_text(value)
    if text is None:
        return None
    return text


def _global_id_sort_key(value: int | str) -> tuple[int, Any]:
    if isinstance(value, int):
        return (0, value)
    return (1, value)


def _global_id_token(value: int | str) -> tuple[str, str]:
    if isinstance(value, int):
        return ("int", str(value))
    return ("str", value)


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _format_timestamp(value: Any) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None
