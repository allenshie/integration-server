"""JSONL observation recording for matching broadcast snapshots."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from integration.utils.paths import get_config_root

from .constants import MATCHING_OBSERVATION_MESSAGE_TYPE
from .schema import MatchingBroadcastBatch


@dataclass
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
    batch: MatchingBroadcastBatch,
    matching_snapshot: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Build one observation record per camera frame in the batch."""

    tracked_by_camera = _group_tracked_by_camera(batch.tracked_objects)
    global_objects = [dict(item) for item in batch.global_objects]
    records: list[dict[str, Any]] = []

    for event in edge_events:
        camera_id = _normalize_text(event.get("camera_id"))
        if camera_id is None:
            continue

        records.append(
            {
                "schema_version": matching_snapshot.get("schema_version"),
                "message_type": MATCHING_OBSERVATION_MESSAGE_TYPE,
                "generated_at": matching_snapshot.get("generated_at"),
                "session_id": batch.session_id,
                "frame_seq": batch.frame_seq,
                "capture_ts": batch.capture_ts,
                "camera_id": camera_id,
                "matching_snapshot": dict(matching_snapshot),
                "tracked_objects": tracked_by_camera.get(camera_id, []),
                "global_objects": global_objects,
            },
        )
    return records


def _group_tracked_by_camera(tracked_objects: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in tracked_objects:
        camera_id = _normalize_text(item.get("camera_id"))
        if camera_id is None:
            continue
        grouped.setdefault(camera_id, []).append(dict(item))

    for records in grouped.values():
        records.sort(key=_tracked_sort_key)
    return grouped


def _tracked_sort_key(item: Mapping[str, Any]) -> tuple[int, str]:
    local_id = _coerce_non_negative_int(item.get("local_id"))
    return (local_id if local_id is not None else 10**9, _normalize_text(item.get("global_id")) or "")


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_non_negative_int(value: Any) -> int | None:
    try:
        converted = int(value)
    except (TypeError, ValueError):
        return None
    return converted if converted >= 0 else None
