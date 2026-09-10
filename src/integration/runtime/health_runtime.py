"""Health server lifecycle helpers."""
from __future__ import annotations

import os

from smart_workflow import HealthServer, HealthState, ProbeConfig


def is_health_enabled() -> bool:
    value = os.getenv("INTEGRATION_HEALTH_SERVER_ENABLED")
    if value is None:
        return False
    return value.strip().lower() not in {"0", "false", "no", "off"}


def start_health_server(context, logger) -> tuple[HealthServer | None, HealthState | None]:
    if not is_health_enabled():
        return None, None

    health_state = HealthState()
    context.set_resource("health_state", health_state)

    host = os.environ.get("INTEGRATION_HEALTH_SERVER_HOST", "0.0.0.0")
    port = int(os.environ.get("INTEGRATION_HEALTH_SERVER_PORT", "8081"))

    server = HealthServer(
        health_state=health_state,
        host=host,
        port=port,
        probe_config=ProbeConfig(
            liveness_timeout_seconds=float(
                os.environ.get("INTEGRATION_HEALTH_LIVENESS_TIMEOUT_SECONDS", "30")
            ),
            readiness_timeout_seconds=float(
                os.environ.get("INTEGRATION_HEALTH_READINESS_TIMEOUT_SECONDS", "30")
            ),
            startup_grace_seconds=float(
                os.environ.get("INTEGRATION_HEALTH_STARTUP_GRACE_SECONDS", "10")
            ),
        ),
    )
    server.start()
    logger.info(f"health server started at {host}:{port}")
    return server, health_state


def stop_health_server(server: HealthServer | None) -> None:
    if server is None:
        return
    server.stop()
