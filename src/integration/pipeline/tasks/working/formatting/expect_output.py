"""Utilities for converting MC-MOT output into expect_output_v1 schema."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List


def _is_valid_global_id(value: Any) -> bool:
    return isinstance(value, str) and value.isdigit()


def _convert_value(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    return value


@dataclass
class ExpectOutputTransformer:
    """Convert MC-MOT tracked/global objects into expect_output_v1 structure.

    邏輯分為三段：
    1. 依 camera_id 匯總 tracked objects，組出 `camera_data` -> `object_metadata`（key 為
       `class_localId`，內容含 class/bbox/confidence）。
    2. 將同一 global id 的 local 物件映射到 `object_id_mapping`，因此 `class_globalId`
       會對應到 `{camera_id: class_localId}`。
    3. 針對 global_objects 取最後一個座標點，轉成 `mcmot_data[class_globalId]` 的實際
       坐標；整份資料再附上當前 timestamp 作為 `overall_metadata`。
    """

    tz: timezone = timezone.utc

    def transform(
        self,
        tracked_objects: Iterable[Dict[str, Any]],
        global_objects: Iterable[Dict[str, Any]],
    ) -> Dict[str, Any]:
        camera_data = self._build_camera_data(tracked_objects)
        object_mapping = self._build_object_mapping(tracked_objects)
        global_payload = self._build_global_objects(global_objects)
        return {
            "overall_metadata": {
                "timestamp": datetime.now(self.tz).isoformat(),
            },
            "camera_data": camera_data,
            "mcmot_data": global_payload,
            "object_id_mapping": object_mapping,
        }

    def _build_camera_data(self, tracked_objects: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        per_camera: Dict[str, Dict[str, Any]] = {}
        for obj in tracked_objects:
            camera_id = obj.get("camera_id") or "unknown"
            class_name = obj.get("class_name") or "unknown"
            local_id = obj.get("local_id")
            bbox = obj.get("bbox")
            score = obj.get("score")

            obj_key = f"{class_name}_{local_id}"
            camera_entry = per_camera.setdefault(camera_id, {"object_metadata": {}})
            camera_entry["object_metadata"][obj_key] = {
                "class_name": class_name,
                "bbox": bbox,
                "confidence_score": _convert_value(score),
            }
        return per_camera

    def _build_object_mapping(self, tracked_objects: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
        mapping: Dict[str, Dict[str, str]] = {}
        for obj in tracked_objects:
            global_id = obj.get("global_id")
            if not _is_valid_global_id(global_id):
                continue
            class_name = obj.get("class_name") or "unknown"
            local_id = obj.get("local_id")
            camera_id = obj.get("camera_id") or "unknown"
            obj_key = f"{class_name}_{local_id}"
            global_key = f"{class_name}_{global_id}"
            camera_map = mapping.setdefault(global_key, {})
            camera_map[camera_id] = obj_key
        return mapping

    def _build_global_objects(self, global_objects: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        for obj in global_objects:
            global_id = obj.get("global_id")
            if not _is_valid_global_id(global_id):
                continue
            class_name = obj.get("class_name") or "unknown"
            trajectory = obj.get("trajectory") or []
            coordinate = self._extract_latest_coordinate(trajectory)
            global_key = f"{class_name}_{global_id}"
            payload[global_key] = {
                "class_name": class_name,
                "coordinate_location": coordinate,
            }
        return payload

    @staticmethod
    def _extract_latest_coordinate(trajectory: Iterable[Any]) -> List[Any]:
        if not trajectory:
            return [None, None]
        try:
            last = trajectory[-1]
        except (TypeError, IndexError):
            return [None, None]

        if isinstance(last, dict):
            return [
                _convert_value(last.get("x")),
                _convert_value(last.get("y")),
            ]
        if isinstance(last, (list, tuple)) and len(last) >= 3:
            return [
                _convert_value(last[1]),
                _convert_value(last[2]),
            ]
        return [None, None]
