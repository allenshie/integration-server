"""Minimal HTTP server receiving edge inference events."""
from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from loguru import logger

from integration.api.event_store import EdgeEventStore

LOGGER = logger.bind(component=__name__)


class EdgeEventHandler(BaseHTTPRequestHandler):
    store: EdgeEventStore | None = None

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/edge/events":
            self.send_error(HTTPStatus.NOT_FOUND, "unknown path")
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("payload must be JSON object")
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning(f"invalid edge event payload: {exc}")
            self.send_error(HTTPStatus.BAD_REQUEST, f"invalid payload: {exc}")
            return

        if self.store is None:
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE, "store not available")
            return

        self.store.add_event(payload)
        self.send_response(HTTPStatus.ACCEPTED)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        request_message = format % args if args else format
        LOGGER.info(f"edge-http {self.address_string()} - {request_message}")


def start_edge_event_server(host: str, port: int, store: EdgeEventStore) -> ThreadingHTTPServer:
    EdgeEventHandler.store = store
    server = ThreadingHTTPServer((host, port), EdgeEventHandler)
    LOGGER.info(f"edge event server listening on {host}:{port}")
    return server
