from __future__ import annotations

from pathlib import Path

import mcmot
from mcmot import MCMOT


def test_app_uv_loads_local_mcmot_batch_api():
    module_path = Path(mcmot.__file__).resolve()
    expected_root = Path(__file__).parents[3] / "MCMOT" / "mcmot"

    assert module_path.is_relative_to(expected_root)
    assert hasattr(MCMOT, "process_trajectory_snapshot")
