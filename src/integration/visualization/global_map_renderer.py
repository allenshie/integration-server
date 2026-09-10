"""Rendering helpers for drawing MC-MOT results on the global warehouse map."""
from __future__ import annotations

import colorsys
import hashlib
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Mapping, Tuple

import cv2  # type: ignore[import]
import numpy as np

from integration.config.visualization import GlobalMapVisualizationConfig


@dataclass
class OverlayResult:
    image_path: Path | None
    rendered: np.ndarray | None


_DEFAULT_CLASS_PALETTE: dict[str, tuple[int, int, int]] = {
    "person": (0, 255, 0),
    "stacker": (0, 165, 255),
    "forklift": (0, 128, 255),
}


class GlobalMapRenderer:
    """Centralized renderer that overlays global/local objects onto the warehouse map."""

    def __init__(
        self,
        vis_cfg: GlobalMapVisualizationConfig,
        *,
        logger,
    ) -> None:
        self._vis_cfg = vis_cfg
        self._logger = logger
        self._map_cfg = vis_cfg.map
        self._render_cfg = vis_cfg.render
        self._camera_cfgs = list(vis_cfg.cameras)
        self._allowed_cameras = {camera.camera_id for camera in self._camera_cfgs}
        self._base_canvas: np.ndarray | None = None
        self._image_mtime: float | None = None
        self._meters_per_pixel_x = 1.0
        self._meters_per_pixel_y = 1.0
        self._global_radius = 4
        self._local_radius = 2
        self._global_font_scale = 0.5
        self._global_label_thickness = 1
        self._local_font_scale = 0.5
        self._local_label_thickness = 1
        self._camera_colors: dict[str, tuple[int, int, int]] = {}
        self._camera_lookup: dict[str, str] = {}
        self._legend_entries: list[tuple[str, str, tuple[int, int, int]]] = []
        self._legend_ids: set[str] = set()
        self._seen_global_classes: list[str] = []
        self._seen_global_class_set: set[str] = set()
        self._build_camera_color_map()

    def render(
        self,
        global_objects: Iterable[Mapping],
        local_objects: Iterable[Mapping],
    ) -> OverlayResult | None:
        canvas = self._load_base_canvas()
        if canvas is None:
            return None
        self._configure_canvas(canvas.shape[:2])

        rendered = canvas.copy()
        global_list = list(global_objects)
        local_list = list(local_objects)

        global_count = self._draw_global_objects(rendered, global_list)
        local_payload = self._prepare_local_overlay_objects(local_list, global_list)
        local_count, used_cameras = self._draw_local_objects(rendered, local_payload)

        if self._render_cfg.show_legend:
            self._draw_legend(rendered, focus_cameras=used_cameras)

        saved_path = self._finalize(rendered)
        if saved_path is None and global_count == 0 and local_count == 0:
            self._logger.debug("全局地圖沒有可視化的物件")
        return OverlayResult(image_path=saved_path, rendered=rendered)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_base_canvas(self) -> np.ndarray | None:
        image_path = self._map_cfg.image_path
        if not image_path:
            self._logger.debug("全局地圖未設定影像路徑，略過可視化")
            return None
        path = Path(image_path)
        if not path.exists():
            self._logger.warning(f"找不到全局地圖影像：{path}")
            return None
        mtime = path.stat().st_mtime
        if self._base_canvas is None or self._image_mtime != mtime:
            canvas = cv2.imread(str(path))
            if canvas is None:
                self._logger.warning(f"無法載入全局地圖影像：{path}")
                return None
            self._base_canvas = canvas
            self._image_mtime = mtime
        return self._base_canvas

    def _configure_canvas(self, shape: tuple[int, int]) -> None:
        height, width = shape
        min_dim = max(1, min(height, width))
        global_dynamic = int(min_dim * max(0.0, self._render_cfg.global_radius_ratio))
        self._global_radius = max(self._render_cfg.marker_radius, global_dynamic, 4)
        local_dynamic = int(min_dim * max(0.0, self._render_cfg.local_radius_ratio))
        self._local_radius = max(2, min(self._global_radius - 2, local_dynamic or self._global_radius // 2))
        self._global_font_scale, self._global_label_thickness = self._compute_font_params(
            self._global_radius,
        )
        self._local_font_scale, self._local_label_thickness = self._compute_font_params(
            self._local_radius,
            scale_bias=0.85,
        )
        self._meters_per_pixel_x = self._map_cfg.width_meters / float(max(1, width))
        self._meters_per_pixel_y = self._map_cfg.height_meters / float(max(1, height))

    def _draw_global_objects(
        self,
        canvas: np.ndarray,
        global_objects: Iterable[Mapping],
    ) -> int:
        count = 0
        for obj in global_objects:
            coords = self._extract_global_xy(obj)
            if coords is None:
                continue
            x, y = int(round(coords[0])), int(round(coords[1]))
            class_name = obj.get("class_name")
            color = self._color_for_global(class_name)
            cv2.circle(canvas, (x, y), self._global_radius, color, thickness=-1)
            label_parts: List[str] = []
            if class_name and class_name not in self._seen_global_class_set:
                self._seen_global_class_set.add(str(class_name))
                self._seen_global_classes.append(str(class_name))
            if self._render_cfg.show_global_id and obj.get("global_id") is not None:
                label_parts.append(str(obj.get("global_id")))
            if self._render_cfg.show_class_name and class_name:
                label_parts.append(str(class_name))
            if label_parts:
                cv2.putText(
                    canvas,
                    "|".join(label_parts),
                    (x + self._global_radius + 4, y - 4),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    self._global_font_scale,
                    color,
                    self._global_label_thickness,
                    lineType=cv2.LINE_AA,
                )
            count += 1
        return count

    def _prepare_local_overlay_objects(
        self,
        local_objects: Iterable[Mapping],
        global_objects: Iterable[Mapping],
    ) -> List[dict]:
        global_lookup = {
            obj.get("global_id"): obj for obj in global_objects if obj.get("global_id") is not None
        }
        prepared: List[dict] = []
        for item in local_objects:
            camera_id = item.get("camera_id")
            if not camera_id:
                continue
            canonical_id = self._camera_lookup.get(str(camera_id), str(camera_id))
            if self._allowed_cameras and camera_id not in self._allowed_cameras and canonical_id not in self._allowed_cameras:
                continue
            coords = self._coerce_point(item.get("global_position"))
            if coords is None:
                continue
            global_id = item.get("global_id")
            distance_m = None
            if global_id is not None:
                ref_obj = global_lookup.get(global_id)
                if ref_obj is not None:
                    ref_xy = self._extract_global_xy(ref_obj)
                    if ref_xy is not None:
                        dx = coords[0] - ref_xy[0]
                        dy = coords[1] - ref_xy[1]
                        distance_m = self._distance_in_meters(dx, dy)
            prepared.append(
                {
                    "camera_id": str(camera_id),
                    "canonical_id": canonical_id,
                    "local_id": item.get("local_id"),
                    "global_id": global_id,
                    "point": coords,
                    "distance_m": distance_m,
                }
            )
        return prepared

    def _draw_local_objects(
        self,
        canvas: np.ndarray,
        local_objects: List[dict],
    ) -> tuple[int, set[str]]:
        rendered = 0
        used_cameras: set[str] = set()
        for obj in local_objects:
            point = obj["point"]
            color = self._color_for_camera(obj["camera_id"], obj["canonical_id"])
            x = int(round(point[0]))
            y = int(round(point[1]))
            cv2.circle(canvas, (x, y), self._local_radius, color, thickness=-1)
            label_parts: List[str] = []
            if obj.get("local_id") is not None:
                label_parts.append(f"l_{obj.get('local_id')}")
            if obj.get("global_id") is not None:
                label_parts.append(f"g_{obj.get('global_id')}")
            distance = obj.get("distance_m")
            if distance is not None:
                label_parts.append(f"{distance:.2f}M")
            if label_parts:
                cv2.putText(
                    canvas,
                    " | ".join(label_parts),
                    (x + self._local_radius + 4, y - 4),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    self._local_font_scale,
                    color,
                    self._local_label_thickness,
                    lineType=cv2.LINE_AA,
                )
            rendered += 1
            used_cameras.add(obj["canonical_id"])
        if rendered == 0:
            self._logger.debug("沒有符合條件的 local 物件可視化")
        return rendered, used_cameras

    def _draw_legend(
        self,
        canvas: np.ndarray,
        *,
        focus_cameras: set[str],
    ) -> None:
        sections: List[Tuple[str, str | None, tuple[int, int, int] | None]] = []
        global_palette = self._build_global_legend()
        if global_palette:
            sections.append(("title", "Global Objects", None))
            for class_name, color in global_palette:
                sections.append(("item", class_name, color))
        camera_entries = self._build_camera_legend(focus_cameras)
        if camera_entries:
            if sections:
                sections.append(("spacer", None, None))
            sections.append(("title", "Cameras", None))
            sections.extend(camera_entries)
        if not sections:
            return
        padding = 10
        line_height = 22
        item_count = len([entry for entry in sections if entry[0] != "spacer"])
        width = 240
        height = padding * 2 + line_height * item_count
        overlay = canvas.copy()
        top_left = (padding, padding)
        bottom_right = (padding + width, padding + height)
        cv2.rectangle(overlay, top_left, bottom_right, (30, 30, 30), thickness=-1)
        cv2.addWeighted(overlay, 0.4, canvas, 0.6, 0, canvas)
        y = padding + line_height - 6
        for kind, display_name, color in sections:
            if kind == "spacer":
                y += line_height // 2
                continue
            if kind == "title":
                cv2.putText(
                    canvas,
                    display_name or "",
                    (padding + 8, y),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    (255, 255, 255),
                    1,
                    lineType=cv2.LINE_AA,
                )
                y += line_height
                continue
            if color is None or display_name is None:
                continue
            cv2.rectangle(
                canvas,
                (padding + 8, y - 12),
                (padding + 28, y + 4),
                color,
                thickness=-1,
            )
            cv2.putText(
                canvas,
                display_name,
                (padding + 34, y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.55,
                (255, 255, 255),
                1,
                lineType=cv2.LINE_AA,
            )
            y += line_height

    def _finalize(self, rendered: np.ndarray) -> Path | None:
        saved_path: Path | None = None
        mode = self._render_cfg.mode
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if mode in {"write", "both"}:
            output_dir = Path(self._render_cfg.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            saved_path = output_dir / f"global_map_{timestamp}.png"
            cv2.imwrite(str(saved_path), rendered)
            self._logger.debug(f"已輸出全局地圖快照：{saved_path}")

        if mode in {"show", "both"}:
            try:
                cv2.imshow(self._render_cfg.window_name, rendered)
                cv2.waitKey(1)
            except cv2.error as exc:  # pragma: no cover
                self._logger.warning(f"無法顯示全局地圖視窗：{exc}")
        return saved_path

    def _build_camera_color_map(self) -> None:
        for camera in self._camera_cfgs:
            color = self._stable_color_from_key(f"camera:{camera.camera_id}")
            display_name = camera.display_name or camera.camera_id
            self._register_camera_entry(camera.camera_id, display_name, color)
            self._camera_colors[camera.camera_id] = color
            self._camera_lookup[camera.camera_id] = camera.camera_id
            for alias in camera.aliases:
                self._camera_lookup[alias] = camera.camera_id
                self._camera_colors[alias] = color

    def _register_camera_entry(
        self,
        camera_id: str,
        display_name: str,
        color: tuple[int, int, int],
    ) -> None:
        if camera_id in self._legend_ids:
            return
        self._legend_ids.add(camera_id)
        self._legend_entries.append((camera_id, display_name, color))

    def _color_for_camera(self, raw_camera_id: str, canonical_camera_id: str | None = None) -> tuple[int, int, int]:
        canonical_id = canonical_camera_id or self._camera_lookup.get(raw_camera_id, raw_camera_id)
        color = self._camera_colors.get(raw_camera_id) or self._camera_colors.get(canonical_id)
        if color is not None:
            return color
        color = self._stable_color_from_key(f"camera:{canonical_id}")
        self._camera_colors[canonical_id] = color
        self._camera_colors[raw_camera_id] = color
        self._camera_lookup[raw_camera_id] = canonical_id
        self._register_camera_entry(canonical_id, canonical_id, color)
        return color

    def _color_for_global(self, class_name: str | None) -> tuple[int, int, int]:
        if class_name and class_name in _DEFAULT_CLASS_PALETTE:
            return _DEFAULT_CLASS_PALETTE[class_name]
        if class_name:
            return self._stable_color_from_key(f"class:{class_name}")
        return (255, 255, 255)

    def _distance_in_meters(self, dx_pixels: float, dy_pixels: float) -> float:
        return math.hypot(dx_pixels * self._meters_per_pixel_x, dy_pixels * self._meters_per_pixel_y)

    def _build_global_legend(self) -> List[Tuple[str, tuple[int, int, int]]]:
        if self._seen_global_classes:
            return [
                (class_name, self._color_for_global(class_name))
                for class_name in self._seen_global_classes
            ]
        return list(_DEFAULT_CLASS_PALETTE.items())

    def _build_camera_legend(self, focus_cameras: set[str]) -> List[Tuple[str, str, tuple[int, int, int]]]:
        entries: List[Tuple[str, str, tuple[int, int, int]]] = []
        ordered_entries = list(self._legend_entries)
        if focus_cameras:
            ordered_entries = sorted(
                ordered_entries,
                key=lambda entry: (entry[0] not in focus_cameras, entry[0]),
            )
        for camera_id, display_name, color in ordered_entries:
            entries.append(("item", display_name, color))
        return entries

    @staticmethod
    def _coerce_point(value: Mapping | None) -> tuple[float, float] | None:
        if not value:
            return None
        x = value.get("x")
        y = value.get("y")
        if x is None or y is None:
            return None
        try:
            return float(x), float(y)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_global_xy(obj: Mapping) -> tuple[float, float] | None:
        trajectory = obj.get("trajectory")
        if not trajectory:
            return None
        last = trajectory[-1]
        if isinstance(last, Mapping):
            x = last.get("x")
            y = last.get("y")
        elif isinstance(last, (tuple, list)) and len(last) >= 3:
            x = last[1]
            y = last[2]
        else:
            return None
        if x is None or y is None:
            return None
        try:
            return float(x), float(y)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _stable_color_from_key(key: str) -> tuple[int, int, int]:
        digest = hashlib.sha1(key.encode("utf-8")).digest()
        hue = digest[0] / 255.0
        saturation = 0.65 + (digest[1] / 255.0) * 0.2
        value = 0.85 + (digest[2] / 255.0) * 0.1
        red, green, blue = colorsys.hsv_to_rgb(hue, min(saturation, 1.0), min(value, 1.0))
        return (
            int(round(blue * 255)),
            int(round(green * 255)),
            int(round(red * 255)),
        )

    def _compute_font_params(
        self,
        radius: int,
        *,
        scale_bias: float = 1.0,
    ) -> tuple[float, int]:
        base_scale = max(0.3, self._render_cfg.label_font_scale)
        dynamic_scale = max(0.3, radius / 14.0)
        scale = max(base_scale, dynamic_scale * scale_bias)
        base_thickness = max(1, self._render_cfg.label_thickness)
        dynamic_thickness = max(1, int(round(radius / 8)))
        return scale, max(base_thickness, dynamic_thickness)


__all__ = ["OverlayResult", "GlobalMapRenderer"]
