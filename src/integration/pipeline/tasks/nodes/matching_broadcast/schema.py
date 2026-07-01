"""Payload schema for broadcasting MC-MOT matching results."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .constants import MATCHING_BROADCAST_MESSAGE_TYPE, MATCHING_BROADCAST_SCHEMA_VERSION


@dataclass(slots=True)
class MatchingBroadcastTrack:
    """Single camera-local matching entry."""

    local_id: int
    global_id: Any = None
    class_name: str = "unknown"
    matched: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "local_id": self.local_id,
            "global_id": self.global_id,
        }
        if self.class_name:
            payload["class_name"] = self.class_name
        payload["matched"] = self.matched
        return payload


@dataclass(slots=True)
class MatchingBroadcastPayload:
    """Envelope for a single broadcast containing all camera match tables."""

    schema_version: int = MATCHING_BROADCAST_SCHEMA_VERSION
    message_type: str = MATCHING_BROADCAST_MESSAGE_TYPE
    generated_at: str = ""
    camera_matches: dict[str, list[MatchingBroadcastTrack]] = field(default_factory=dict)

    @classmethod
    def from_tracked_objects(
        cls,
        tracked_objects: Iterable[Mapping[str, Any]],
        generated_at: datetime | None = None,
    ) -> "MatchingBroadcastPayload":
        grouped: dict[str, list[MatchingBroadcastTrack]] = {}
        for item in tracked_objects:
            camera_id = str(item.get("camera_id") or "").strip()
            if not camera_id:
                continue
            local_id = _coerce_local_id(item.get("local_id"))
            if local_id is None:
                continue
            global_id = item.get("global_id")
            grouped.setdefault(camera_id, []).append(
                MatchingBroadcastTrack(
                    local_id=local_id,
                    global_id=global_id,
                    class_name=str(item.get("class_name") or "unknown"),
                    matched=_has_value(global_id),
                ),
            )

        for tracks in grouped.values():
            tracks.sort(key=lambda track: track.local_id)

        return cls(
            generated_at=_format_timestamp(generated_at or datetime.now(timezone.utc)),
            camera_matches=dict(sorted(grouped.items(), key=lambda item: item[0])),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "message_type": self.message_type,
            "generated_at": self.generated_at,
            "camera_matches": {
                camera_id: [track.to_dict() for track in tracks]
                for camera_id, tracks in self.camera_matches.items()
            },
        }


def _coerce_local_id(value: Any) -> int | None:
    try:
        local_id = int(value)
    except (TypeError, ValueError):
        return None
    return local_id if local_id >= 0 else None


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True
