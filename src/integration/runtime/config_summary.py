"""Helpers for printing startup configuration summary."""
from __future__ import annotations

import os


def should_print_config_summary() -> bool:
    return os.getenv("CONFIG_SUMMARY", "").strip().lower() in {"1", "true", "yes"}


def log_config_summary(config, context, logger) -> None:
    scheduler = context.get_resource("scheduler")
    scheduler_engine = getattr(scheduler, "_engine", None)
    scheduler_engine_name = scheduler_engine.__class__.__name__ if scheduler_engine else "unknown"
    phase_engine_path = getattr(getattr(config, "phase_task", None), "engine_class", None)
    phase_engine_name = phase_engine_path or "TimeBasedPhaseEngine"
    phase_broadcast_enabled = getattr(getattr(config, "phase_messaging", None), "enabled", True)
    matching_broadcast_enabled = getattr(getattr(config, "matching_broadcast", None), "enabled", False)
    matching_broadcast_recording_enabled = getattr(
        getattr(getattr(config, "matching_broadcast", None), "recording", None),
        "enabled",
        False,
    )
    pipeline_summary_interval_seconds = getattr(config, "pipeline_summary_interval_seconds", 60.0)
    logger.info(
        (
            "config summary:\n"
            "- scheduler_engine: %s\n"
            "- phase_engine: %s\n"
            "- phase_broadcast_enabled: %s\n"
            "- matching_broadcast_enabled: %s\n"
            "- matching_broadcast_recording_enabled: %s\n"
            "- pipeline_summary_interval_seconds: %s\n"
            "- pipeline_schedule: %s"
        ),
        scheduler_engine_name,
        phase_engine_name,
        phase_broadcast_enabled,
        matching_broadcast_enabled,
        matching_broadcast_recording_enabled,
        pipeline_summary_interval_seconds,
        config.pipeline_schedule_path,
    )
