"""Event catalogue: which event types exist, their aggregate, topic and schema.

One entry per event type (D-05: one Schema Registry subject per event type,
named by the Avro record). One topic per aggregate (D-06 revised), so the
events of one appointment stay ordered relative to each other; the event type
travels in the Avro record name and the `eventType` header, never in the topic.

Plan 03-01 registered `appointment.booked`; plan 03-03 adds the other nine event
types, each together with its `.avsc` file, so the catalogue never names a schema
file that does not exist. The deploy-time CLI `libs.events.register` registers
exactly this catalogue.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

SCHEMA_NAMESPACE: Final = "com.smarthealth.events"


class EventType:
    """Event type constants (dotted, lower-case, as stored in outbox.type)."""

    APPOINTMENT_BOOKED: Final = "appointment.booked"
    APPOINTMENT_CANCELLED: Final = "appointment.cancelled"


@dataclass(frozen=True, slots=True)
class EventSpec:
    event_type: str
    aggregate_type: str
    record_name: str
    schema_file: str

    @property
    def subject(self) -> str:
        """Registry subject under `record_subject_name_strategy` (the record name)."""
        return self.record_name


def _spec(event_type: str, aggregate_type: str, pascal_name: str) -> EventSpec:
    return EventSpec(
        event_type=event_type,
        aggregate_type=aggregate_type,
        record_name=f"{SCHEMA_NAMESPACE}.{pascal_name}",
        schema_file=f"{pascal_name}.avsc",
    )


EVENT_CATALOG: dict[str, EventSpec] = {
    spec.event_type: spec
    for spec in (
        _spec(EventType.APPOINTMENT_BOOKED, "appointment", "AppointmentBooked"),
        _spec(EventType.APPOINTMENT_CANCELLED, "appointment", "AppointmentCancelled"),
    )
}

# One topic per aggregate. `cdcprobe` is a test-only topic that no consumer
# reads (used to prove the CDC path end to end without touching real topics).
AGGREGATE_TOPICS: dict[str, str] = {
    aggregate: f"smarthealth.{aggregate}.events"
    for aggregate in ("appointment", "visit", "provider", "patient", "cdcprobe")
}

CDC_PROBE_TOPIC: Final = AGGREGATE_TOPICS["cdcprobe"]

# One dead-letter topic per consumer group (D-08, EVENT-05).
CONSUMER_GROUPS: Final = ("visit-downstream", "notification-trigger", "analytics-projection")
DLQ_TOPICS: dict[str, str] = {group: f"smarthealth.dlq.{group}" for group in CONSUMER_GROUPS}


def topic_for(event_type: str) -> str:
    """Kafka topic that carries `event_type` (its aggregate's topic)."""
    return AGGREGATE_TOPICS[EVENT_CATALOG[event_type].aggregate_type]
