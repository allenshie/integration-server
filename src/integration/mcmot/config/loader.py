"""Helpers for loading MC-MOT configuration files."""
from __future__ import annotations

from pathlib import Path

from integration.mcmot.config.manager import ConfigManager
from integration.mcmot.config.schema import BaseConfig
from integration.utils.paths import get_core_root


DEFAULT_CONFIG_RELATIVE_PATH = "data/config/mcmot.config.yaml"


def resolve_config_path(raw_path: str | None) -> Path:
    """Resolve a config path relative to the integration core root."""
    path = Path(raw_path or DEFAULT_CONFIG_RELATIVE_PATH)
    if not path.is_absolute():
        core_root = get_core_root()
        path = (core_root / path).resolve()
    return path


def load_mcmot_config(raw_path: str | None = None) -> BaseConfig:
    """Load MC-MOT configuration using the vendored ConfigManager."""
    path = resolve_config_path(raw_path)
    manager = ConfigManager(config=str(path))
    return manager.config


__all__ = ["load_mcmot_config", "resolve_config_path", "DEFAULT_CONFIG_RELATIVE_PATH"]
