"""Adapter around the external MCMOT package."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from loguru import logger as loguru_logger

from integration.api.trajectory_store import (
    TimestampContractError,
    normalize_utc_timestamp,
)
from integration.utils.paths import get_config_root


@dataclass
class MCMOTResult:
    tracked_objects: List[Dict[str, Any]]
    global_objects: List[Dict[str, Any]]
    success: bool = True
    matching_performed: bool = True
    committed_watermark: int | None = None
    reason: str | None = None
    attempt_report: Any | None = None


class MCMOTEngine:
    """Adapter that feeds integration events into the external MCMOT engine."""

    def __init__(
        self,
        tracking_config: str | None = None,
        camera_config: str | None = None,
        logger: Any | None = None,
    ) -> None:
        self._log = logger or loguru_logger.bind(component="integration.mcmot.engine")
        self._tracking_config_path = self._resolve_config_path(
            tracking_config,
            "MCMOT_TRACKING_CONFIG_PATH",
        )
        self._camera_config_path = self._resolve_config_path(
            camera_config,
            "MCMOT_CAMERA_CONFIG_PATH",
        )
        self._engine = self._initialize_engine(
            self._tracking_config_path,
            self._camera_config_path,
        )
        self.config = self._engine.config
        self._last_successful_watermark = int(
            getattr(self._engine, "last_successful_watermark", 0) or 0
        )
        self._last_attempt_report: Any | None = None
        camera_count = len(getattr(self.config, "cameras", []) or [])
        self._log.info(f"MC-MOT engine ready with {camera_count} cameras")

    def process_events(self, events: Iterable[Dict[str, Any]]) -> MCMOTResult:
        self._last_attempt_report = None
        events_list = list(events)
        tracked_payload: List[Dict[str, Any]] = []
        latest_timestamp: datetime | None = None

        for event in events_list:
            timestamp = self._ensure_timestamp(event.get("timestamp"))
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp

            camera_id = event.get("camera_id")
            detections = self._build_detections(event.get("detections") or [])
            if not camera_id or not detections:
                continue

            tracked = self._engine.process_detected_objects(
                camera_id=camera_id,
                timestamp=timestamp,
                detected_objects=detections,
            )
            if tracked:
                tracked_payload.extend(self._serialize_tracked(camera_id, tracked))

        finalize_timestamp = latest_timestamp or datetime.now(timezone.utc)
        self._engine.finalize_global_updates(finalize_timestamp)
        global_objects = [self._serialize_global(obj) for obj in self._engine.get_all_global_objects()]
        return MCMOTResult(tracked_objects=tracked_payload, global_objects=global_objects)

    @property
    def last_successful_watermark(self) -> int:
        return int(
            getattr(self._engine, "last_successful_watermark", self._last_successful_watermark)
            or self._last_successful_watermark
        )

    def is_matching_due(self) -> bool:
        """Ask MCMOT whether a matching attempt should be started."""
        checker = getattr(self._engine, "is_matching_due", None)
        if not callable(checker):
            # Keep compatibility with older external MCMOT builds; they still
            # perform their own cadence decision inside process_trajectory_snapshot.
            return True
        return bool(checker())

    def maintain(self) -> None:
        """Run MCMOT lifecycle maintenance without reading a snapshot."""
        maintainer = getattr(self._engine, "maintain", None)
        if callable(maintainer):
            maintainer()

    def get_all_global_objects(self) -> List[Dict[str, Any]]:
        """Return the current global objects for a cadence-skipped cycle."""
        getter = getattr(self._engine, "get_all_global_objects", None)
        if not callable(getter):
            return []
        return self._serialize_batch_global(getter() or [])

    @property
    def last_attempt_report(self) -> Any | None:
        """Return the latest typed attempt report for exception reporting."""
        return self._last_attempt_report

    def process_trajectory_snapshot(self, snapshot: Any) -> MCMOTResult:
        """將完整軌跡快照交給 MCMOT library 的協調流程。

        ``self._engine`` 是 MCMOT library 的 ``MCMOT`` facade；此處呼叫
        ``process_trajectory_snapshot()`` 後，會由 facade 轉交給
        ``MCMOTCoordinator.process_trajectory_snapshot()``。App adapter
        只負責資料格式轉換與結果反序列化，不在此處拆 camera 或執行 matching。
        """
        self._last_attempt_report = None
        objects = [self._serialize_trajectory_object(obj) for obj in snapshot.objects]
        high_watermark = int(snapshot.high_watermark)
        # 這是 App → MCMOT facade → MCMOTCoordinator 的正式 typed snapshot 入口。
        processor = getattr(self._engine, "process_trajectory_snapshot", None)
        if processor is None:
            raise RuntimeError("MCMOT library does not expose process_trajectory_snapshot")

        raw_result = processor(objects=objects, high_watermark=high_watermark)
        if isinstance(raw_result, MCMOTResult):
            self._last_attempt_report = raw_result.attempt_report
            return raw_result

        if isinstance(raw_result, Mapping):
            result = MCMOTResult(
                tracked_objects=self._serialize_batch_tracked(raw_result.get("tracked_objects") or []),
                global_objects=self._serialize_batch_global(raw_result.get("global_objects") or []),
                success=bool(raw_result.get("success", False)),
                matching_performed=bool(raw_result.get("matching_performed", False)),
                committed_watermark=raw_result.get("committed_watermark"),
                reason=raw_result.get("reason"),
                attempt_report=raw_result.get(
                    "attempt_report",
                    raw_result.get("matching_attempt"),
                ),
            )
        else:
            result = MCMOTResult(
                tracked_objects=self._serialize_batch_tracked(getattr(raw_result, "tracked_objects", []) or []),
                global_objects=self._serialize_batch_global(getattr(raw_result, "global_objects", []) or []),
                success=bool(getattr(raw_result, "success", False)),
                matching_performed=bool(getattr(raw_result, "matching_performed", False)),
                committed_watermark=getattr(raw_result, "committed_watermark", None),
                reason=getattr(raw_result, "reason", None),
                attempt_report=getattr(
                    raw_result,
                    "attempt_report",
                    getattr(raw_result, "matching_attempt", None),
                ),
            )

        self._last_attempt_report = result.attempt_report

        if result.success and result.committed_watermark is not None:
            self._last_successful_watermark = max(
                self._last_successful_watermark,
                int(result.committed_watermark),
            )
        return result

    def _serialize_batch_tracked(self, objects: Iterable[Any]) -> List[Dict[str, Any]]:
        serialized: List[Dict[str, Any]] = []
        for item in objects:
            if isinstance(item, Mapping) and "global_position" in item:
                serialized.append(dict(item))
                continue
            if isinstance(item, Mapping):
                camera_id = str(item.get("camera_id") or "")
                serialized.extend(self._serialize_tracked(camera_id, [dict(item)]))
            else:
                camera_id = str(getattr(item, "camera_id", ""))
                serialized.extend(self._serialize_tracked(camera_id, [item]))
        return serialized

    def _serialize_batch_global(self, objects: Iterable[Any]) -> List[Dict[str, Any]]:
        serialized: List[Dict[str, Any]] = []
        for item in objects:
            if isinstance(item, Mapping):
                serialized.append(dict(item))
            else:
                serialized.append(self._serialize_global(item))
        return serialized

    @staticmethod
    def _serialize_trajectory_object(obj: Any) -> Dict[str, Any]:
        metadata = dict(getattr(obj, "latest_metadata", {}) or {})
        # The trajectory snapshot is a local-input DTO.  A legacy edge event
        # may carry a global_id annotation in metadata, but it must not cross
        # the App/MCMOT boundary as a formal Gallery ID.
        metadata.pop("global_id", None)

        local_trajectory = []
        for index, point in enumerate(getattr(obj, "local_trajectory", ())):
            if not isinstance(point, Sequence) or len(point) != 3:
                raise TimestampContractError(
                    "trajectory point must be [timestamp, x, y]",
                    path=f"snapshot.local_trajectory[{index}]",
                )
            local_trajectory.append(
                [
                    normalize_utc_timestamp(
                        point[0],
                        path=f"snapshot.local_trajectory[{index}][0]",
                    ),
                    point[1],
                    point[2],
                ]
            )
        if not local_trajectory:
            raise TimestampContractError(
                "trajectory must be non-empty",
                path="snapshot.local_trajectory",
            )

        def optional_timestamp(field_name: str) -> datetime | None:
            return normalize_utc_timestamp(
                getattr(obj, field_name, None),
                path=f"snapshot.{field_name}",
                required=False,
            )

        return {
            "camera_id": getattr(obj, "camera_id", metadata.get("camera_id")),
            "local_id": getattr(obj, "local_id", metadata.get("local_id")),
            "class_name": getattr(obj, "class_name", None) or metadata.get("class_name"),
            "bbox": list(getattr(obj, "latest_bbox", metadata.get("bbox", [])) or []),
            "score": getattr(obj, "latest_score", None),
            "feature": getattr(obj, "latest_feature", None),
            "first_seen_at": optional_timestamp("first_seen_at"),
            "last_observed_at": optional_timestamp("last_observed_at"),
            "last_updated_at": optional_timestamp("last_updated_at"),
            "lifecycle_state": getattr(obj, "lifecycle_state", "active"),
            "timestamp": normalize_utc_timestamp(
                getattr(obj, "latest_timestamp", metadata.get("timestamp")),
                path="snapshot.timestamp",
            ),
            "local_trajectory": local_trajectory,
            "revision": int(getattr(obj, "revision", 0)),
            "watermark": int(getattr(obj, "watermark", getattr(obj, "last_watermark", 0)) or 0),
            "metadata": metadata,
        }

    def _initialize_engine(self, tracking_config_path: str, camera_config_path: str):
        try:
            from mcmot import MCMOT as EngineClass
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "MCMOT package is required when MC-MOT is enabled. "
                "Install the MCMOT submodule before enabling tracking.",
            ) from exc

        return EngineClass(
            tracking_config=tracking_config_path,
            camera_config=camera_config_path,
        )

    @staticmethod
    def _resolve_config_path(raw: str | None, env_name: str) -> str:
        if raw is None or not str(raw).strip():
            raise ValueError(f"{env_name} must be set when MC-MOT is enabled")
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (get_config_root() / path).resolve()
        return str(path)

    def _build_detections(self, detections: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        formatted: List[Dict[str, Any]] = []
        for det in detections:
            bbox = det.get("bbox") or det.get("box")
            if not bbox or len(bbox) != 4:
                continue
            local_id = det.get("local_id", det.get("track_id"))
            if local_id is None:
                continue
            class_name = det.get("class_name") or det.get("label")
            if class_name is None:
                continue
            score = det.get("score")
            if score is None:
                score = det.get("confidence")
            if score is None:
                score = 0.0
            formatted.append(
                {
                    "class_name": class_name,
                    "local_id": int(local_id),
                    "global_id": det.get("global_id"),
                    "bbox": [int(x) for x in bbox],
                    "score": float(score),
                    "feature": det.get("feature"),
                }
            )
        return formatted

    def _serialize_tracked(self, camera_id: str, tracked: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        payload: List[Dict[str, Any]] = []
        for item in tracked:
            global_position = self._extract_latest_xy(item.get("global_trajectory"))
            payload.append(
                {
                    "camera_id": camera_id,
                    "class_name": item.get("class_name"),
                    "local_id": item.get("local_id"),
                    "global_id": item.get("global_id"),
                    "bbox": item.get("bbox"),
                    "score": item.get("score"),
                    "timestamp": self._to_iso(item.get("timestamp")),
                    "global_position": global_position,
                }
            )
        return payload

    def _serialize_global(self, obj: Any) -> Dict[str, Any]:
        trajectory = []
        for entry in getattr(obj, "trajectory", []) or []:
            ts, x, y = entry
            trajectory.append(
                {
                    "timestamp": self._to_iso(ts),
                    "x": float(x),
                    "y": float(y),
                }
            )
        update_time = getattr(obj, "update_time", None)
        return {
            "global_id": getattr(obj, "global_id", None),
            "class_name": getattr(obj, "class_name", None),
            "camera_id": getattr(obj, "camera_id", None),
            "trajectory": trajectory,
            "updated_at": self._to_iso(update_time),
        }

    @staticmethod
    def _ensure_timestamp(value: Any) -> datetime:
        timestamp = normalize_utc_timestamp(value, path="event.timestamp")
        assert timestamp is not None
        return timestamp

    @staticmethod
    def _to_iso(value: Any) -> str | None:
        if value is None:
            return None
        timestamp = normalize_utc_timestamp(value, path="output.timestamp")
        assert timestamp is not None
        return timestamp.isoformat().replace("+00:00", "Z")

    @staticmethod
    def _extract_latest_xy(trajectory: Any) -> Dict[str, float] | None:
        if not trajectory:
            return None
        try:
            last = trajectory[-1]
        except (TypeError, IndexError):
            return None
        x: float | None
        y: float | None
        if isinstance(last, Mapping):
            x = last.get("x")
            y = last.get("y")
        elif isinstance(last, Sequence) and len(last) >= 3:
            x = last[1]
            y = last[2]
        else:
            return None
        if x is None or y is None:
            return None
        try:
            return {"x": float(x), "y": float(y)}
        except (TypeError, ValueError):
            return None
