"""Shared event library: catalogue, strict registry client, outbox write helper."""

from libs.events.catalog import EVENT_CATALOG
from libs.events.outbox import enqueue_event

__all__ = ["EVENT_CATALOG", "enqueue_event"]
