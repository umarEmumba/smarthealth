"""Strict Schema Registry access for the event library (plan 03-01, D-05).

Services only READ schema ids: every serializer is built with
`auto.register.schemas=False` and `record_subject_name_strategy`, so a service
that serializes an event whose schema was not registered by the deploy-time
CLI (`python -m libs.events.register`, plan 03-03) fails instead of silently
writing a new subject (T-3-57). `register_all` is the only writer and is used
by that CLI and by tests against the `mock://` client.

Schema Registry is a cached dependency of the write path (T-3-04): `warm_up`
resolves every schema id once at process start, with retries, so a request
never pays (or blocks on) the first registry round trip.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import threading
import uuid
from collections.abc import Iterable
from importlib import resources
from typing import Any

from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    record_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

from libs.config import shared_settings
from libs.events.catalog import EVENT_CATALOG

_lock = threading.Lock()
_clients: dict[str, SchemaRegistryClient] = {}
_serializers: dict[tuple[str, str], AvroSerializer] = {}
_deserializers: dict[str, AvroDeserializer] = {}
_warmed: set[tuple[str, str]] = set()


def get_registry_client() -> SchemaRegistryClient:
    """One process-wide client per registry URL (`mock://...` yields the mock)."""
    url = shared_settings.schema_registry_url
    with _lock:
        client = _clients.get(url)
        if client is None:
            client = SchemaRegistryClient.new_client({"url": url})
            _clients[url] = client
        return client


def load_schema_str(event_type: str) -> str:
    """The `.avsc` text for `event_type`, read from the package data.

    Stripped, because confluent-kafka's serializer strips the schema text it is
    given; the text registered by `register_all` must be byte-identical to the
    text the serializer looks up (the mock registry compares strings).
    """
    spec = EVENT_CATALOG[event_type]
    text = resources.files("libs.events").joinpath("schemas", spec.schema_file).read_text("utf-8")
    return text.strip()


def serializer_for(event_type: str) -> AvroSerializer:
    """The cached STRICT serializer for `event_type` (never registers schemas)."""
    url = shared_settings.schema_registry_url
    key = (url, event_type)
    with _lock:
        serializer = _serializers.get(key)
    if serializer is not None:
        return serializer
    client = get_registry_client()
    built = AvroSerializer(
        client,
        load_schema_str(event_type),
        conf={
            "auto.register.schemas": False,
            "subject.name.strategy": record_subject_name_strategy,
        },
    )
    with _lock:
        return _serializers.setdefault(key, built)


def deserializer() -> AvroDeserializer:
    """One generic deserializer; the writer schema id comes from each message."""
    url = shared_settings.schema_registry_url
    client = get_registry_client()
    with _lock:
        existing = _deserializers.get(url)
        if existing is None:
            existing = AvroDeserializer(client)
            _deserializers[url] = existing
        return existing


def register_all(
    client: SchemaRegistryClient, event_types: Iterable[str] | None = None
) -> dict[str, int]:
    """Register the catalogue schemas (or a subset) and return {subject: schema_id}.

    The only writer to the registry: the deploy-time CLI and tests call it;
    service code never does.
    """
    registered: dict[str, int] = {}
    for event_type in event_types if event_types is not None else tuple(EVENT_CATALOG):
        spec = EVENT_CATALOG[event_type]
        schema_id = client.register_schema(
            spec.subject, Schema(load_schema_str(event_type), "AVRO")
        )
        registered[spec.subject] = schema_id
    return registered


def is_warm(event_type: str) -> bool:
    """True once a serialization for `event_type` has resolved its schema id."""
    return (shared_settings.schema_registry_url, event_type) in _warmed


def mark_warm(event_type: str) -> None:
    _warmed.add((shared_settings.schema_registry_url, event_type))


def _sample_value(avro_type: Any) -> Any:
    """A minimal valid value for an Avro type (used only to resolve schema ids)."""
    if isinstance(avro_type, list):  # union: prefer null, else the first branch
        return None if "null" in avro_type else _sample_value(avro_type[0])
    if isinstance(avro_type, dict):
        kind = avro_type["type"]
        logical = avro_type.get("logicalType")
        if kind == "record":
            return {f["name"]: _sample_value(f["type"]) for f in avro_type["fields"]}
        if kind == "array":
            return []
        if kind == "map":
            return {}
        if kind == "enum":
            return avro_type["symbols"][0]
        if logical == "uuid":
            return str(uuid.UUID(int=0))
        if logical in ("timestamp-millis", "timestamp-micros"):
            return datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
        return _sample_value(kind)
    return {
        "string": "",
        "int": 0,
        "long": 0,
        "float": 0.0,
        "double": 0.0,
        "boolean": False,
        "bytes": b"",
        "null": None,
    }[avro_type]


def _warm_one(event_type: str) -> None:
    from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: PLC0415

    from libs.events.catalog import topic_for  # noqa: PLC0415

    sample = _sample_value(json.loads(load_schema_str(event_type)))
    serializer_for(event_type)(
        sample, SerializationContext(topic_for(event_type), MessageField.VALUE)
    )
    mark_warm(event_type)


async def warm_up(
    event_types: Iterable[str] | None = None,
    attempts: int = 10,
    delay_seconds: float = 2.0,
) -> None:
    """Resolve each serializer's schema id once, retrying while the registry starts.

    Runs the blocking registry calls in a thread. Raises the last error after
    `attempts` failures, so a service cold-started during a registry outage
    cannot become ready (T-3-04; residual risk documented in plan 03-18).
    """
    for event_type in event_types if event_types is not None else tuple(EVENT_CATALOG):
        for attempt in range(1, attempts + 1):
            try:
                await asyncio.to_thread(_warm_one, event_type)
                break
            except Exception:
                if attempt == attempts:
                    raise
                await asyncio.sleep(delay_seconds)


def reset_registry_for_tests() -> None:
    """Drop every cached client, serializer and warm-up mark."""
    with _lock:
        _clients.clear()
        _serializers.clear()
        _deserializers.clear()
        _warmed.clear()
