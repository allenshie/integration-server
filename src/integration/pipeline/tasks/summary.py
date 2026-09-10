"""Helpers for periodic pipeline summary logs.

The task resources contain the latest per-cycle facts.  The summary window is
kept separately so a cadence-skipped MC-MOT cycle cannot overwrite the
counts accumulated by an earlier matching attempt.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


INGESTION_STATS_RESOURCE = "ingestion_stats"
MC_MOT_STATS_RESOURCE = "mc_mot_stats"
MATCHING_BROADCAST_STATS_RESOURCE = "matching_broadcast_stats"
FORMAT_STATS_RESOURCE = "format_stats"
RULE_STATS_RESOURCE = "rule_stats"
EVENT_DISPATCH_STATS_RESOURCE = "event_dispatch_stats"
SUMMARY_WINDOW_STATS_RESOURCE = "pipeline_summary_window_stats"
SUMMARY_STATUS_RESOURCE = "pipeline_summary_status"
MCMOT_STATE_RESOURCE = "mcmot_state"

SUMMARY_INTERVAL_SECONDS = 60.0

# Keep the table narrow. Details such as ingestion drops are folded into the
# result cell, while matching cadence facts get their own columns.
SUMMARY_COLUMNS = (
    "input",
    "result",
    "attempts",
    "skipped",
    "failed",
)

SUMMARY_STAGE_RESOURCES = (
    ("ingestion", INGESTION_STATS_RESOURCE),
    ("mc_mot", MC_MOT_STATS_RESOURCE),
    ("matching_broadcast", MATCHING_BROADCAST_STATS_RESOURCE),
    ("format_conversion", FORMAT_STATS_RESOURCE),
    ("rule_evaluation", RULE_STATS_RESOURCE),
    ("event_dispatch", EVENT_DISPATCH_STATS_RESOURCE),
)

_COUNTER_FIELDS = frozenset(
    {
        "raw",
        "events",
        "dropped",
        "duplicates",
        "tracked",
        "signal_groups",
        "warnings",
        "dispatched",
        "skipped",
        "failed",
        "recorded",
        "recording_failed",
        "input_count",
        "result_count",
        "attempts",
    }
)
_GAUGE_FIELDS = frozenset({"global", "active_global"})


def reset_pipeline_cycle_stats(context) -> None:
    """Clear only current-cycle stage facts, preserving the summary window."""

    for _, resource_key in SUMMARY_STAGE_RESOURCES:
        context.set_resource(resource_key, {})


def reset_pipeline_summary(context) -> None:
    """Reset the completed summary window and current-cycle stage facts."""

    reset_pipeline_cycle_stats(context)
    context.set_resource(SUMMARY_WINDOW_STATS_RESOURCE, {})
    context.set_resource(SUMMARY_STATUS_RESOURCE, "ok")


def mark_pipeline_summary_error(context) -> None:
    """Remember a window error even when the log interval is not due yet."""

    context.set_resource(SUMMARY_STATUS_RESOURCE, "error")


def store_stage_stats(context, resource_key: str, values: dict[str, Any]) -> None:
    """Store per-cycle facts and accumulate their numeric window counters."""

    context.set_resource(resource_key, dict(values))

    window = context.get_resource(SUMMARY_WINDOW_STATS_RESOURCE)
    window = dict(window) if isinstance(window, Mapping) else {}
    aggregate = window.get(resource_key)
    aggregate = dict(aggregate) if isinstance(aggregate, Mapping) else {}

    for key, value in values.items():
        if key in _GAUGE_FIELDS:
            # A gauge describes current state; it must never be added to the
            # window counter (especially active_global).
            aggregate[key] = value
            continue

        if key in _COUNTER_FIELDS:
            if value is None:
                continue
            number = _as_non_negative_int(value)
            aggregate[key] = _as_non_negative_int(aggregate.get(key)) + number
            if key in {"input_count", "result_count"}:
                aggregate[f"_{key}_seen"] = True
            continue

        # Units, reasons and booleans are latest per-cycle metadata. They are
        # not used as window counters but remain available to renderers/tests.
        aggregate[key] = value

    window[resource_key] = aggregate
    context.set_resource(SUMMARY_WINDOW_STATS_RESOURCE, window)


def render_pipeline_summary(
    context,
    phase_name: str,
    window_seconds: float,
    status: str = "ok",
    throughput: dict[str, Any] | None = None,
    latency: dict[str, Any] | None = None,
) -> str:
    window = context.get_resource(SUMMARY_WINDOW_STATS_RESOURCE)
    window = window if isinstance(window, Mapping) else {}

    rows = []
    for stage_name, resource_key in SUMMARY_STAGE_RESOURCES:
        stage_stats = window.get(resource_key)
        if not isinstance(stage_stats, Mapping):
            # Keep direct callers and older task integrations compatible while
            # the pipeline migrates to the window resource.
            stage_stats = context.get_resource(resource_key)
        stage_stats = stage_stats if isinstance(stage_stats, Mapping) else {}
        rows.append(_build_stage_row(stage_name, stage_stats))

    window_status = context.get_resource(SUMMARY_STATUS_RESOURCE)
    if window_status == "error" or status == "error" or _has_failures(rows):
        effective_status = "error"
    elif status == "ok" and _is_idle(rows):
        effective_status = "idle"
    else:
        effective_status = status

    return _render_table(
        rows,
        phase_name,
        window_seconds,
        effective_status,
        mcmot_state=context.get_resource(MCMOT_STATE_RESOURCE),
        throughput=throughput,
        latency=latency,
    )


def _build_stage_row(stage_name: str, stats: Mapping[str, Any]) -> dict[str, str]:
    if not stats:
        return {"stage": stage_name, **{column: "-" for column in SUMMARY_COLUMNS}}

    input_count, input_unit = _stage_input(stage_name, stats)
    result_count, result_unit = _stage_result(stage_name, stats)
    result_text = _format_count(result_count, result_unit)

    if stage_name == "ingestion" and result_text != "-":
        details = []
        if "dropped" in stats:
            details.append(f"drop={_format_value(stats.get('dropped'))}")
        if "duplicates" in stats:
            details.append(f"dup={_format_value(stats.get('duplicates'))}")
        if details:
            result_text = f"{result_text} ({' '.join(details)})"
    elif stage_name == "matching_broadcast" and result_text != "-":
        details = []
        if "recorded" in stats:
            details.append(f"recorded={_format_value(stats.get('recorded'))}")
        if "recording_failed" in stats and _as_non_negative_int(stats.get('recording_failed')):
            details.append(f"recording_failed={_format_value(stats.get('recording_failed'))}")
        if details:
            result_text = f"{result_text} ({' '.join(details)})"

    return {
        "stage": stage_name,
        "input": _format_count(input_count, input_unit),
        "result": result_text,
        "attempts": _format_optional_count(stats, "attempts"),
        "skipped": _format_optional_count(stats, "skipped"),
        "failed": _format_value(stats.get("failed", 0)),
    }


def _stage_input(stage_name: str, stats: Mapping[str, Any]) -> tuple[Any, str]:
    input_count = _read_count(stats, "input_count")
    input_unit = str(stats.get("input_unit") or "")
    if input_count is not None or input_unit:
        return input_count, input_unit or "items"

    # Compatibility fallback for callers that still use the original stats
    # names. New typed MC-MOT facts use input_unit="snapshots".
    if stage_name == "ingestion":
        return stats.get("raw"), "raw"
    if stage_name == "mc_mot":
        return stats.get("events"), "events"
    if stage_name in {"format_conversion", "matching_broadcast"}:
        return stats.get("tracked"), "tracked"
    if stage_name == "rule_evaluation":
        return stats.get("global"), "objects"
    if stage_name == "event_dispatch":
        return stats.get("events"), "events"
    return None, "items"


def _stage_result(stage_name: str, stats: Mapping[str, Any]) -> tuple[Any, str]:
    result_count = _read_count(stats, "result_count")
    result_unit = str(stats.get("result_unit") or "")
    if result_count is not None or result_unit:
        return result_count, result_unit or "items"

    if stage_name == "ingestion":
        return stats.get("events"), "accepted"
    if stage_name == "mc_mot":
        return stats.get("tracked"), "tracked"
    if stage_name == "matching_broadcast":
        return stats.get("dispatched"), "dispatched"
    if stage_name == "format_conversion":
        return stats.get("signal_groups"), "groups"
    if stage_name == "rule_evaluation":
        return stats.get("warnings"), "warnings"
    if stage_name == "event_dispatch":
        return stats.get("dispatched"), "dispatched"
    return None, "items"


def _read_count(stats: Mapping[str, Any], key: str) -> Any:
    if key not in stats:
        return None
    if stats.get(f"_{key}_seen") is False:
        return None
    return stats.get(key)


def _format_count(value: Any, unit: str) -> str:
    if value is None:
        return "-"
    unit = unit.strip()
    return f"{_format_value(value)} {unit}" if unit else _format_value(value)


def _format_optional_count(stats: Mapping[str, Any], key: str) -> str:
    if key not in stats:
        return "-"
    value = stats.get(key)
    return "-" if value is None else _format_value(value)


def _has_failures(rows: Iterable[Mapping[str, str]]) -> bool:
    return any(_as_non_negative_int(row.get("failed")) > 0 for row in rows)


def _is_idle(rows: Iterable[Mapping[str, str]]) -> bool:
    for row in rows:
        for column in SUMMARY_COLUMNS:
            value = row.get(column, "-")
            if value != "-" and value != "0":
                return False
    return True


def _format_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}"
    text = str(value).strip()
    return text if text else "-"


def _as_non_negative_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def _render_table(
    rows: list[dict[str, str]],
    phase_name: str,
    window_seconds: float,
    status: str,
    *,
    mcmot_state: Mapping[str, Any] | None = None,
    throughput: dict[str, Any] | None = None,
    latency: dict[str, Any] | None = None,
) -> str:
    header = f"pipeline_summary window={_format_value(window_seconds)}s phase={phase_name or '-'} status={status}"
    columns = ("stage",) + SUMMARY_COLUMNS
    widths: dict[str, int] = {column: len(column) for column in columns}

    for row in rows:
        widths["stage"] = max(widths["stage"], len(row["stage"]))
        for column in SUMMARY_COLUMNS:
            widths[column] = max(widths[column], len(row[column]))

    lines = [header]
    throughput_line = _render_throughput_line(throughput)
    if throughput_line is not None:
        lines.append(throughput_line)
    latency_line = _render_latency_line(latency)
    if latency_line is not None:
        lines.append(latency_line)
    lines.append(_format_row(columns, widths, is_header=True))
    lines.append(_format_separator(columns, widths))
    for row in rows:
        lines.append(_format_row((row["stage"], *[row[column] for column in SUMMARY_COLUMNS]), widths))
    lines.append(_render_mcmot_state(mcmot_state))
    return "\n".join(lines)


def _format_row(values: tuple[str, ...], widths: dict[str, int], is_header: bool = False) -> str:
    stage = values[0]
    columns = values[1:]
    cells = [f"{stage:<{widths['stage']}}"]
    for column_name, value in zip(SUMMARY_COLUMNS, columns):
        if value == "-" or is_header:
            cells.append(f"{value:<{widths[column_name]}}")
        else:
            cells.append(f"{value:>{widths[column_name]}}")
    return " | ".join(cells)


def _format_separator(columns: tuple[str, ...], widths: dict[str, int]) -> str:
    return " | ".join("-" * widths[column] for column in columns)


def _render_mcmot_state(state: Mapping[str, Any] | None) -> str:
    state = state if isinstance(state, Mapping) else {}
    fields = (
        ("active_global", _format_value(state.get("active_global"))),
        ("last_matching_at", _format_value(state.get("last_matching_at"))),
        ("last_status", _format_value(state.get("last_matching_status"))),
        ("reason", _format_value(state.get("last_reason"))),
        ("watermark", _format_value(state.get("last_successful_watermark"))),
    )
    return "mcmot_state | " + " | ".join(f"{key}={value}" for key, value in fields)


def _render_throughput_line(throughput: dict[str, Any] | None) -> str | None:
    if not throughput:
        return None

    elapsed_seconds = throughput.get("elapsed_seconds")
    elapsed_text = "-"
    if elapsed_seconds is not None:
        elapsed_text = f"{_format_value(elapsed_seconds)}s"

    fields = (
        ("elapsed", elapsed_text),
        ("source_fps", _format_value(throughput.get("source_fps"))),
        ("processed_fps", _format_value(throughput.get("processed_fps"))),
        ("duplicate_skip_fps", _format_value(throughput.get("duplicate_skip_fps"))),
        ("active_batches", _format_value(throughput.get("active_batches"))),
        ("idle_batches", _format_value(throughput.get("idle_batches"))),
    )
    return _render_key_value_line("throughput", fields)


def _render_latency_line(latency: dict[str, Any] | None) -> str | None:
    if not latency:
        return None

    elapsed_seconds = latency.get("elapsed_seconds")
    elapsed_text = "-"
    if elapsed_seconds is not None:
        elapsed_text = f"{_format_value(elapsed_seconds)}s"

    fields = (
        ("elapsed", elapsed_text),
        ("avg_active_ms", _format_value(latency.get("avg_active_ms"))),
    )
    return _render_key_value_line("latency", fields)


def _render_key_value_line(prefix: str, fields: tuple[tuple[str, str], ...]) -> str:
    parts = [prefix]
    for key, value in fields:
        parts.append(f"{key}={value}")
    return " | ".join(parts)
