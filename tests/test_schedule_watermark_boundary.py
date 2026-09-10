from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from main import safe_loop_interval_seconds
from integration.pipeline.schedule import PhasePolicy


def test_mcmot_schedule_disables_phase_throttle():
    schedule_path = Path(__file__).parents[2] / "pipeline_schedule.json"
    schedule = json.loads(schedule_path.read_text())
    policy = PhasePolicy(float(schedule["phases"]["working"]["interval_seconds"]))

    assert policy.interval == 0.0
    assert policy.enabled is False
    assert policy.should_run(last_run_time=999.0, now=999.0) is True


def test_outer_loop_keeps_safe_sleep_when_interval_is_zero_or_invalid():
    assert safe_loop_interval_seconds(0) == 0.01
    assert safe_loop_interval_seconds(-2) == 0.01
    assert safe_loop_interval_seconds("invalid") == 0.01
    assert safe_loop_interval_seconds(0.25) == 0.25
