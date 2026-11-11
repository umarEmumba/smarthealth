"""CDC relay proofs against the live stack (plan 03-05; D-01, EVENT-02).

A row committed to a service's `outbox` reaches Kafka through Debezium with
key = aggregate id, headers `id`, `eventType`, `traceparent`, `schemaVersion`, and
a registry-framed Avro value; delivery survives Connect being stopped or hard
killed. Tests that name a connector are parametrized over the connector JSON files
present, so the patient, provider and visit connectors that later plans add are
covered without editing this file (select one with `-k <name>`).

Probe rows use `aggregatetype='cdcprobe'` and land on `smarthealth.cdcprobe.events`,
which no consumer reads. They are synthetic and are left in the outbox (outbox
tables are insert-only; test hygiene never deletes application rows).
"""

from __future__ import annotations

import pytest

from libs.events.catalog import CDC_PROBE_TOPIC
from tests.e2e import phase3_stack as stack

pytestmark = pytest.mark.e2e

@pytest.fixture(scope="module", autouse=True)
def _stack_ready() -> None:
    stack.require_phase3_stack()


def _scheduling_db_url() -> str:
    from services.scheduling.config import settings  # noqa: PLC0415

    return settings.database_url


# --- Task 1: tracer ------------------------------------------------------------------


def test_tracer_probe_row_reaches_kafka_through_debezium() -> None:
    """One committed outbox row -> one Kafka message with the agreed key, headers
    and an Avro value that decodes against the LIVE registry."""
    stack.wait_connectors_running(["scheduling-outbox"])
    (row,) = stack.make_probe_rows(count=1)
    stack.insert_probe_rows(_scheduling_db_url(), [row])

    messages = stack.consume_matching(
        CDC_PROBE_TOPIC, lambda m: m["key"] == row.aggregate_id, timeout=60
    )
    assert len(messages) == 1, f"expected one probe message, got {len(messages)}"
    message = messages[0]

    assert message["key"] == row.aggregate_id
    headers = message["headers"]
    assert headers["id"] == str(row.event_id)
    assert headers["eventType"] == "appointment.booked"
    assert headers["traceparent"] == row.traceparent
    assert headers["schemaVersion"] == "1"

    value = message["value"]
    # The `uuid` logical type decodes to uuid.UUID.
    assert str(value["event_id"]) == str(row.event_id)
    assert value["event_type"] == "appointment.booked"
    assert value["aggregate_id"] == row.aggregate_id
    assert str(value["appointment_id"]) == str(row.appointment_id)
    assert value["schema_version"] == 1
