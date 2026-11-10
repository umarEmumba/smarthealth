"""Event schema contract and deploy-time registration tests (plan 03-03, D-05, D-07).

The registration CLI is the only writer of the Schema Registry. Tests drive it
against in-memory `mock://` clients (plus small fakes for the failure paths) and
check the schema files against the catalogue.
"""

from __future__ import annotations

import datetime
import json
import re
import uuid
from typing import Any

import pytest
from confluent_kafka.schema_registry import (
    RegisteredSchema,
    Schema,
    SchemaRegistryClient,
    SchemaRegistryError,
    ServerConfig,
)
from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (
    MockSchemaRegistryClient,
)
from confluent_kafka.serialization import MessageField, SerializationContext

from libs.events import register
from libs.events.catalog import EVENT_CATALOG, topic_for
from libs.events.registry import deserializer, load_schema_str, serializer_for

_LINE = re.compile(r"^com\.smarthealth\.events\.[A-Za-z]+ -> \d+$")


class SpyClient(MockSchemaRegistryClient):
    """Mock registry that records the compatibility and registration writes."""

    def __init__(self) -> None:
        super().__init__({"url": "mock://spy"})
        self.compat_set: dict[str, str] = {}
        self.registered: list[str] = []

    def set_config(self, subject_name=None, config: ServerConfig | None = None):  # noqa: ANN001
        assert config is not None and subject_name is not None
        self.compat_set[subject_name] = config.compatibility.value
        return super().set_config(subject_name, config)

    def register_schema(self, subject_name: str, schema: Schema, normalize_schemas: bool = False):
        self.registered.append(subject_name)
        return super().register_schema(subject_name, schema, normalize_schemas)


class IncompatibleClient(SpyClient):
    """A registry that already holds a different schema and refuses the new one."""

    def __init__(self, subject: str) -> None:
        super().__init__()
        self._subject = subject

    def get_latest_version(self, subject_name: str, fmt: str | None = None) -> RegisteredSchema:
        if subject_name != self._subject:
            raise SchemaRegistryError(404, 40401, "Subject not found")
        return RegisteredSchema(
            schema_id=1,
            guid=str(uuid.uuid4()),
            schema=Schema('{"type": "string"}', "AVRO"),
            subject=subject_name,
            version=1,
        )

    def test_compatibility(self, subject_name: str, schema: Schema, *args: Any, **kwargs: Any):
        return subject_name != self._subject


def _sample(event_type: str) -> dict[str, Any]:
    """A valid payload built from the schema itself (optional fields are filled)."""
    schema = json.loads(load_schema_str(event_type))
    now = datetime.datetime(2026, 11, 10, 9, 0, tzinfo=datetime.UTC)
    sample: dict[str, Any] = {}
    for field in schema["fields"]:
        kind = field["type"]
        if isinstance(kind, list):
            kind = next(branch for branch in kind if branch != "null")
        logical = kind.get("logicalType") if isinstance(kind, dict) else None
        base = kind["type"] if isinstance(kind, dict) else kind
        if logical == "uuid":
            sample[field["name"]] = str(uuid.uuid4())
        elif logical == "timestamp-millis":
            sample[field["name"]] = now
        elif base == "string":
            sample[field["name"]] = f"{event_type}"
        elif base == "int":
            sample[field["name"]] = 1
        else:  # pragma: no cover - the catalogue uses only the kinds above
            raise AssertionError(f"unsupported type in {event_type}: {kind}")
    sample["event_type"] = event_type
    return sample


def _stdout_lines(capsys: pytest.CaptureFixture[str]) -> list[str]:
    return [line for line in capsys.readouterr().out.splitlines() if line]


def test_register_prints_one_line_per_subject_and_exits_zero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert register.main(["--url", "mock://p3-register"]) == 0
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line]
    assert len(lines) == len(EVENT_CATALOG)
    assert all(_LINE.match(line) for line in lines)
    assert {line.split(" -> ")[0] for line in lines} == {
        spec.subject for spec in EVENT_CATALOG.values()
    }
    assert captured.err == ""


def test_register_sets_backward_on_every_subject() -> None:
    client = SpyClient()
    assert register.main([], client=client) == 0
    assert client.compat_set == {spec.subject: "BACKWARD" for spec in EVENT_CATALOG.values()}
    assert sorted(client.registered) == sorted(client.compat_set)


def test_second_registration_run_returns_the_same_ids(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = SpyClient()
    assert register.main([], client=client) == 0
    first = _stdout_lines(capsys)
    assert register.main([], client=client) == 0
    second = _stdout_lines(capsys)
    assert first == second
    assert len(set(line.split(" -> ")[1] for line in first)) == len(first)  # distinct ids


def test_check_mode_registers_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    client = SpyClient()
    assert register.main(["--check"], client=client) == 0
    assert client.registered == []
    assert client.compat_set == {}
    assert _stdout_lines(capsys) == []
    assert client.get_subjects() == []


def test_check_mode_exits_one_and_names_the_incompatible_subject(
    capsys: pytest.CaptureFixture[str],
) -> None:
    subject = EVENT_CATALOG["appointment.cancelled"].subject
    client = IncompatibleClient(subject)
    assert register.main(["--check"], client=client) == 1
    captured = capsys.readouterr()
    assert subject in captured.err
    assert client.registered == []
    assert client.compat_set == {}


def test_incompatible_schema_is_refused_before_anything_is_written(
    capsys: pytest.CaptureFixture[str],
) -> None:
    subject = EVENT_CATALOG["appointment.cancelled"].subject
    client = IncompatibleClient(subject)
    assert register.main([], client=client) == 1
    assert subject in capsys.readouterr().err
    assert client.registered == []  # no half-registered catalogue
    assert client.compat_set == {}


def test_registry_error_prints_only_the_exception_class_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class Broken(SpyClient):
        def register_schema(self, *args: Any, **kwargs: Any):
            raise ConnectionError("http://user:hunter2@registry:8081 body={schema}")

    assert register.main([], client=Broken()) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "ERROR: ConnectionError\n"
    assert "hunter2" not in captured.err
    assert "Traceback" not in captured.err


def test_registry_error_on_a_real_url_never_leaks_the_url(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Nothing listens on port 1: the client raises a connection error.
    assert register.main(["--url", "http://user:hunter2@127.0.0.1:1"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("ERROR: ")
    assert "hunter2" not in captured.err and "127.0.0.1" not in captured.err
    assert "Traceback" not in captured.err


def test_appointment_cancelled_round_trips_through_the_strict_serializer(
    mock_registry: SchemaRegistryClient,
) -> None:
    event_type = "appointment.cancelled"
    sample = _sample(event_type)
    ctx = SerializationContext(topic_for(event_type), MessageField.VALUE)
    wire = serializer_for(event_type)(sample, ctx)
    decoded = deserializer()(wire, ctx)
    assert decoded["event_type"] == event_type
    assert decoded["appointment_id"] == uuid.UUID(sample["appointment_id"])
    assert decoded["cancelled_at"] == sample["cancelled_at"]
