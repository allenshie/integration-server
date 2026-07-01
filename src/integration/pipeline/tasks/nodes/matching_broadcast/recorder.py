"""JSONL observation recording for matching broadcast results."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from integration.pipeline.tasks.nodes.matching_broadcast.schema import MatchingBroadcastPayload
from integration.utils.paths import get_config_root

MATCHING_OBSERVATION_MESSAGE_TYPE = "matching_observation"


@dataclass(slots=True)
class MatchingBroadcastObservationWriter:
    """Append frame-level matching observations to a JSONL file."""

    path: Path

    def __init__(self, raw_path: str | Path) -> None:
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = (get_config_root() / path).resolve()
        self.path = path

    def write_records(self, records: Iterable[Mapping[str, Any]]) -> int:
        """Append records and return how many lines were written."""

        record_list = list(records)
        if not record_list:
            return 0

        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            for record in record_list:
                json.dump(record, handle, ensure_ascii=False, separators=(",", ":"))
                handle.write("\n")
        return len(record_list)


def build_matching_broadcast_records(
    edge_events: Iterable[Mapping[str, Any]],
    tracked_objects: Iterable[Mapping[str, Any]],
    global_objects: Iterable[Mapping[str, Any]],
    *,
    generated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Build one observation record per camera frame in the batch."""

    edge_event_list = list(edge_events)
    tracked_list = [dict(item) for item in tracked_objects]
    global_list = [dict(item) for item in global_objects]
    for item in tracked_list:
        item["matched"] = _has_value(item.get("global_id"))
    broadcast_payload = MatchingBroadcastPayload.from_tracked_objects(
        tracked_list,
        generated_at=generated_at,
    ).to_dict()
    tracked_by_camera = _group_tracked_by_camera(tracked_list)
    recorded_at = broadcast_payload["generated_at"]

    records: list[dict[str, Any]] = []
    for event in edge_event_list:
        camera_id = _normalize_text(event.get("camera_id"))
        if not camera_id:
            continue

        records.append(
            {
                "schema_version": broadcast_payload["schema_version"],
                "message_type": MATCHING_OBSERVATION_MESSAGE_TYPE,
                "generated_at": recorded_at,
                "session_id": _normalize_text(event.get("session_id")),
                "frame_seq": _coerce_frame_seq(event.get("frame_seq")),
                "capture_ts": _format_timestamp(event.get("capture_ts") or event.get("timestamp")),
                "camera_id": camera_id,
                "matching_result": broadcast_payload,
                "tracked_objects": tracked_by_camera.get(camera_id, []),
                "global_objects": global_list,
            },
        )
    return records


def _group_tracked_by_camera(tracked_objects: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in tracked_objects:
        camera_id = _normalize_text(item.get("camera_id"))
        if not camera_id:
            continue
        grouped.setdefault(camera_id, []).append(dict(item))

    for records in grouped.values():
        records.sort(key=_tracked_sort_key)
    return grouped


def _tracked_sort_key(item: Mapping[str, Any]) -> tuple[int, str]:
    local_id = _coerce_frame_seq(item.get("local_id"))
    global_id = _normalize_text(item.get("global_id"))
    return (local_id if local_id is not None else 10**9, global_id or "")


def _coerce_frame_seq(value: Any) -> int | None:
    try:
        frame_seq = int(value)
    except (TypeError, ValueError):
        return None
    return frame_seq if frame_seq >= 0 else None


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _format_timestamp(value: Any) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat()
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None
