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
from pathlib import Path
from typing import Any

import pytest
from fastavro import parse_schema
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


# --- catalogue / file parity, envelope, PHI scan, round trip (Task 2) -----------

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "libs" / "events" / "schemas"
ENVELOPE = ("event_id", "event_type", "schema_version", "occurred_at", "aggregate_id")
# D-07 / T-3-56: no schema field name may contain any of these fragments.
PHI_FRAGMENTS = (
    "name", "birth", "dob", "phone", "address", "email",
    "coverage", "ssn", "insurance", "diagnosis", "note",
)  # fmt: skip
EXPECTED_TYPES = {
    "appointment.booked", "appointment.cancelled", "appointment.rescheduled",
    "appointment.checked_in", "appointment.completed", "appointment.no_show",
    "visit.completed", "provider.schedule_changed",
    "patient.registered", "patient.deactivated",
}  # fmt: skip
EVENT_TYPES = sorted(EVENT_CATALOG)


def test_catalogue_is_exactly_the_ten_d07_event_types() -> None:
    assert set(EVENT_CATALOG) == EXPECTED_TYPES
    assert len(EVENT_CATALOG) == 10


def test_every_schema_file_belongs_to_a_catalogue_entry_and_vice_versa() -> None:
    files = {path.name for path in SCHEMA_DIR.glob("*.avsc")}
    assert files == {spec.schema_file for spec in EVENT_CATALOG.values()}
    assert len(files) == len(EVENT_CATALOG) == 10


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_schema_parses_and_matches_its_catalogue_entry(event_type: str) -> None:
    spec = EVENT_CATALOG[event_type]
    schema = json.loads(load_schema_str(event_type))
    parsed = parse_schema(schema)
    assert parsed["name"] == spec.record_name == f"com.smarthealth.events.{schema['name']}"
    assert schema["namespace"] == "com.smarthealth.events"
    assert spec.subject == spec.record_name


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_envelope_fields_come_first_in_order(event_type: str) -> None:
    fields = [f["name"] for f in json.loads(load_schema_str(event_type))["fields"]]
    assert tuple(fields[: len(ENVELOPE)]) == ENVELOPE
    assert len(set(fields)) == len(fields)


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_schema_event_type_doc_names_its_own_event(event_type: str) -> None:
    fields = {f["name"]: f for f in json.loads(load_schema_str(event_type))["fields"]}
    assert event_type in fields["event_type"]["doc"]


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_no_field_name_looks_like_phi(event_type: str) -> None:
    names = [f["name"] for f in json.loads(load_schema_str(event_type))["fields"]]
    offenders = [n for n in names if any(fragment in n.lower() for fragment in PHI_FRAGMENTS)]
    assert offenders == [], f"{event_type}: PHI-looking field names {offenders}"


def test_phi_scan_actually_catches_phi_field_names() -> None:
    """The scan is only worth trusting if it flags what it claims to flag."""
    for bad in ("patient_name", "date_of_birth", "phone_number", "email", "diagnosis_code"):
        assert any(fragment in bad for fragment in PHI_FRAGMENTS), bad


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_optional_fields_are_null_unions_with_a_null_default(event_type: str) -> None:
    """Later additions must stay BACKWARD-compatible: optional means ["null", T] + default null."""
    for field in json.loads(load_schema_str(event_type))["fields"]:
        if isinstance(field["type"], list):
            assert field["type"][0] == "null", field["name"]
            assert "default" in field and field["default"] is None, field["name"]


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_every_event_type_round_trips(
    event_type: str, mock_registry: SchemaRegistryClient
) -> None:
    sample = _sample(event_type)
    ctx = SerializationContext(topic_for(event_type), MessageField.VALUE)
    decoded = deserializer()(serializer_for(event_type)(sample, ctx), ctx)
    assert decoded["event_type"] == event_type
    for key, value in sample.items():
        if isinstance(value, datetime.datetime) or key == "event_type":
            assert decoded[key] == value, key
        elif key in ("schema_version", "aggregate_id") or not _is_uuid(value):
            assert decoded[key] == value, key
        else:
            assert decoded[key] == uuid.UUID(value), key


def _is_uuid(value: object) -> bool:
    try:
        uuid.UUID(str(value))
    except ValueError:
        return False
    return True


def test_optional_fields_may_be_null(mock_registry: SchemaRegistryClient) -> None:
    ctx = SerializationContext(topic_for("visit.completed"), MessageField.VALUE)
    sample = _sample("visit.completed") | {"started_at": None}
    assert deserializer()(serializer_for("visit.completed")(sample, ctx), ctx)["started_at"] is None
    ctx = SerializationContext(topic_for("provider.schedule_changed"), MessageField.VALUE)
    sample = _sample("provider.schedule_changed") | {"template_id": None}
    decoded = deserializer()(serializer_for("provider.schedule_changed")(sample, ctx), ctx)
    assert decoded["template_id"] is None


def test_a_field_outside_the_schema_never_reaches_the_wire(
    mock_registry: SchemaRegistryClient,
) -> None:
    """fastavro writes only schema fields: extra (PHI-looking) keys are dropped, not encoded.

    The PHI guarantee therefore rests on the schema files (scanned above), not on
    the serializer raising: an undeclared key cannot reach Kafka, but it is also
    not reported. Required fields that are missing do fail (outbox test, plan 03-01).
    """
    event_type = "patient.registered"
    sample = _sample(event_type) | {"patient_name": "Ada Lovelace"}
    ctx = SerializationContext(topic_for(event_type), MessageField.VALUE)
    wire = serializer_for(event_type)(sample, ctx)
    assert b"Ada" not in wire and b"patient_name" not in wire
    assert "patient_name" not in deserializer()(wire, ctx)


def test_registering_the_whole_catalogue_prints_ten_lines(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert register.main(["--url", "mock://p3-register-all"]) == 0
    lines = _stdout_lines(capsys)
    assert len(lines) == 10
    assert all(_LINE.match(line) for line in lines)
