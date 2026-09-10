from __future__ import annotations

from datetime import datetime, timezone
import io
from pathlib import Path
import sys

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import setup_logging
from mcmot.core.contracts import (
    MatchingAttemptReport,
    MatchingCameraResult,
    MatchingObjectResult,
)
from mcmot.services.matching_report import MatchingSummaryReporter
from mcmot.utils.logger import get_logger


def _report() -> MatchingAttemptReport:
    return MatchingAttemptReport(
        attempt_id="app-log-attempt",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        status="matched_and_committed",
        high_watermark=1,
        camera_reports=(
            MatchingCameraResult(
                "camera-1",
                (
                    MatchingObjectResult(
                        local_id="1",
                        class_name="car",
                        decision="matched",
                        reason="test",
                    ),
                ),
            ),
        ),
    )


def _remove_handlers(handler_ids: set[int]) -> None:
    for handler_id in handler_ids:
        logger.remove(handler_id)


def test_setup_logging_creates_terminal_sink_with_unified_format(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr(sys, "stderr", output)

    setup_logging("INFO")
    handler_ids = set(logger._core.handlers)
    try:
        assert len(handler_ids) == 1

        logger.info("integration-important")
        rendered = output.getvalue()
        assert "INFO" in rendered
        assert "integration-important" in rendered
        assert "test_logging" in rendered
    finally:
        _remove_handlers(handler_ids)


def test_setup_logging_replaces_sink_without_duplicate_output(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr(sys, "stderr", output)

    setup_logging("INFO")
    setup_logging("INFO")
    handler_ids = set(logger._core.handlers)
    try:
        assert len(handler_ids) == 1

        logger.info("one-app-message")
        assert output.getvalue().count("one-app-message") == 1
    finally:
        _remove_handlers(handler_ids)


def test_mcmot_matching_summary_info_reaches_app_sink(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr(sys, "stderr", output)
    monkeypatch.setenv("MCMOT_MATCHING_SUMMARY", "1")

    setup_logging("INFO")
    handler_ids = set(logger._core.handlers)
    try:
        mcmot_logger = get_logger("integration.matching-summary")

        assert MatchingSummaryReporter(mcmot_logger).emit(_report()) is True
        rendered = output.getvalue()
        assert "matching_attempt id=app-log-attempt" in rendered
        assert "INFO" in rendered
    finally:
        _remove_handlers(handler_ids)
