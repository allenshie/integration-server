import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional
from integration.mcmot.config.schema import BaseConfig
from integration.utils.paths import get_core_root

class ConfigManager:
    def __init__(self, config: Optional[str] = None):
        if config:
            self.config_path = Path(config)
        else:
            core_root = get_core_root()
            self.config_path = core_root / 'data' / 'config' / 'mcmot.config.yaml'

        if not self.config_path:
            raise ValueError("Config 路徑未設定或無效")

        self._base_dir = get_core_root()
        raw_config = self._load_config(self.config_path)
        parsed_config = self._parse_cameras_config(raw_config)
        normalized_config = self._resolve_relative_paths(parsed_config)
        self.config = BaseConfig(**normalized_config)

    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        """
        載入配置文件，支援 YAML 和 JSON 格式。
        """
        if not config_path.is_file():
            raise FileNotFoundError(f"配置檔案： {config_path} 不存在或無效")
        ext = config_path.suffix.lower()
        with open(config_path, 'r', encoding='utf-8') as file:
            if ext in ['.yaml', '.yml']:
                config = yaml.safe_load(file)
            elif ext == '.json':
                config = json.load(file)
            else:
                raise ValueError("不支援的配置檔案格式，僅支援 YAML 和 JSON")
        return config

    def _parse_cameras_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        相機配置前處理
        """
        cameras = config.get("cameras")
        if isinstance(cameras, dict):
            camera_list = []
            for camera_id, camera_cfg in cameras.items():
                camera_cfg = dict(camera_cfg)
                camera_cfg["camera_id"] = camera_id
                camera_list.append(camera_cfg)
            config["cameras"] = camera_list
        return config

    def _resolve_relative_paths(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        將配置中的路徑轉換為絕對路徑，避免受工作目錄影響。
        """
        map_cfg = config.get("map")
        if isinstance(map_cfg, dict):
            image_path = map_cfg.get("image_path")
            if image_path:
                map_cfg["image_path"] = self._absolute_path(image_path)

        cameras = config.get("cameras") or []
        for camera in cameras:
            if not isinstance(camera, dict):
                continue
            for key in ("coordinate_matrix_ckpt", "ignore_polygons"):
                value = camera.get(key)
                if value:
                    camera[key] = self._absolute_path(value)
        return config

    def _absolute_path(self, value: str) -> str:
        path = Path(value)
        if not path.is_absolute():
            path = (self._base_dir / path).resolve()
        return str(path)
