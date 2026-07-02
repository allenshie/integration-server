"""Matching broadcast engine."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from smart_workflow import TaskContext

from .constants import MATCHING_BROADCAST_ROUTE
from .schema import MatchingBroadcastBatch, MatchingBroadcastSnapshot


@dataclass
class MatchingBroadcastResult:
    """Summary returned by matching broadcast engines."""

    session_id: str | None = None
    frame_seq: int | None = None
    dispatched: int = 0
    skipped: int = 0
    failed: int = 0
    task_payload: dict[str, Any] | None = None
    message_payload: dict[str, Any] | None = None
    reason: str | None = None


class BaseMatchingBroadcastEngine(ABC):
    """Base interface for matching broadcast engines."""

    def __init__(self, context: TaskContext | None = None) -> None:
        self._context = context

    @abstractmethod
    def broadcast(
        self,
        batch: MatchingBroadcastBatch,
        context: TaskContext,
    ) -> MatchingBroadcastResult:
        """Build and publish matching snapshots."""


class DefaultMatchingBroadcastEngine(BaseMatchingBroadcastEngine):
    """Fallback engine that publishes matching snapshots."""

    def broadcast(
        self,
        batch: MatchingBroadcastBatch,
        context: TaskContext,
    ) -> MatchingBroadcastResult:
        broadcast_cfg = getattr(context.config, "matching_broadcast", None)
        enabled = bool(getattr(broadcast_cfg, "enabled", False)) if broadcast_cfg is not None else False

        if not enabled:
            context.logger.debug("matching broadcast disabled for %s/%s", batch.session_id, batch.frame_seq)
            return MatchingBroadcastResult(
                session_id=batch.session_id,
                frame_seq=batch.frame_seq,
                skipped=1,
                reason="disabled",
            )

        if not batch.session_id or batch.frame_seq is None:
            context.logger.warning("matching broadcast failed: invalid batch identity")
            return MatchingBroadcastResult(
                session_id=batch.session_id or None,
                frame_seq=batch.frame_seq,
                failed=1,
                reason="invalid_batch_identity",
            )

        payload = MatchingBroadcastSnapshot.from_batch(batch).to_dict()
        messaging = context.get_resource("messaging_client")
        if messaging is None:
            context.logger.warning("matching broadcast skipped: messaging_client not ready")
            return MatchingBroadcastResult(
                session_id=batch.session_id,
                frame_seq=batch.frame_seq,
                skipped=1,
                reason="messaging_client_not_ready",
                message_payload=payload,
            )

        try:
            published = messaging.publish(MATCHING_BROADCAST_ROUTE, payload)
        except Exception as exc:  # pylint: disable=broad-except
            context.logger.warning("matching broadcast failed: %s", exc)
            return MatchingBroadcastResult(
                session_id=batch.session_id,
                frame_seq=batch.frame_seq,
                failed=1,
                reason="publish_exception",
                task_payload={"error": str(exc)},
                message_payload=payload,
            )

        if not published:
            context.logger.warning("matching broadcast failed: backend rejected publish")
            return MatchingBroadcastResult(
                session_id=batch.session_id,
                frame_seq=batch.frame_seq,
                failed=1,
                reason="publish_rejected",
                message_payload=payload,
            )

        context.logger.debug(
            "matching broadcast completed: session=%s frame=%s objects=%d",
            batch.session_id,
            batch.frame_seq,
            len(payload.get("objects") or []),
        )
        return MatchingBroadcastResult(
            session_id=batch.session_id,
            frame_seq=batch.frame_seq,
            dispatched=1,
            message_payload=payload,
        )
