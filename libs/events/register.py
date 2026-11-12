"""Deploy-time Schema Registry registration: the ONLY writer of the registry (D-05).

Services never register schemas at runtime: their serializers are strict
(`auto.register.schemas=False`, see `libs.events.registry`), so an unregistered
or edited schema fails loudly instead of silently creating a subject (T-3-02,
T-3-57). This module is run once per deploy, as the Compose `schema-init` job
of plan 03-05:

    python -m libs.events.register                      # register everything
    python -m libs.events.register --url http://schema-registry:8081
    python -m libs.events.register --check              # compatibility only (CI)

For every catalogue entry (one subject per event type, named by the Avro record)
it (1) tests the local schema against the subject's latest version and stops with
exit 1, naming the subject, when it is incompatible; (2) pins the subject's
compatibility to BACKWARD; (3) registers the schema (idempotent: an unchanged
schema returns its existing id). Every check runs before the first write, so an
incompatible schema never leaves a half-registered catalogue behind.

Output is one `subject -> schema id` line per subject on stdout and nothing else.
Schema bodies, registry URLs and tracebacks are never printed; any registry error
exits 1 with the exception class name only (the URL may carry credentials).

Exit codes: 0 success, 1 incompatible schema or any registry error.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    SchemaRegistryError,
    ServerConfig,
)
from confluent_kafka.schema_registry.common.schema_registry_client import (
    ConfigCompatibilityLevel,
)

from libs.config import shared_settings
from libs.events.catalog import EVENT_CATALOG, EventSpec
from libs.events.registry import load_schema_str

_NOT_FOUND = 404


def _latest_schema_str(client: SchemaRegistryClient, subject: str) -> str | None:
    """The subject's latest registered schema text, or None for a new subject."""
    try:
        latest = client.get_latest_version(subject)
    except SchemaRegistryError as exc:
        if exc.http_status_code == _NOT_FOUND:
            return None
        raise
    return latest.schema.schema_str if latest.schema is not None else None


def _is_compatible(client: SchemaRegistryClient, spec: EventSpec, schema: Schema) -> bool:
    """True for a new subject, an unchanged schema, or a BACKWARD-compatible change."""
    latest = _latest_schema_str(client, spec.subject)
    if latest is None or latest.strip() == (schema.schema_str or "").strip():
        return True  # nothing to be incompatible with, or a no-op re-registration
    return bool(client.test_compatibility(spec.subject, schema))


def main(argv: Sequence[str] | None = None, *, client: SchemaRegistryClient | None = None) -> int:
    """Register (or with --check, only verify) every catalogue schema.

    `client` is a test seam; the CLI always builds its own from `--url`.
    """
    parser = argparse.ArgumentParser(
        prog="python -m libs.events.register",
        description="Register the event schemas with BACKWARD compatibility (deploy time).",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Schema Registry URL (default: SCHEMA_REGISTRY_URL / shared settings)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="test compatibility only; register nothing and change no subject config",
    )
    args = parser.parse_args(argv)

    try:
        if client is None:
            url = args.url or shared_settings.schema_registry_url
            client = SchemaRegistryClient.new_client({"url": url})

        schemas = {
            spec.subject: Schema(load_schema_str(spec.event_type), "AVRO")
            for spec in EVENT_CATALOG.values()
        }

        incompatible = [
            spec.subject
            for spec in EVENT_CATALOG.values()
            if not _is_compatible(client, spec, schemas[spec.subject])
        ]
        if incompatible:
            for subject in incompatible:
                print(f"ERROR: incompatible schema for subject {subject}", file=sys.stderr)
            return 1
        if args.check:
            return 0

        for spec in EVENT_CATALOG.values():
            client.set_config(
                spec.subject, ServerConfig(compatibility=ConfigCompatibilityLevel.BACKWARD)
            )
            schema_id = client.register_schema(spec.subject, schemas[spec.subject])
            print(f"{spec.subject} -> {schema_id}")
    except Exception as exc:  # noqa: BLE001 - exit with the class name only (no URL, no body)
        print(f"ERROR: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
