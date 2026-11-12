"""notification-trigger handlers (plan 03-14, Task 2; NOTIF-01..04, EVENT-03, D-09 revised).

No broker and no Temporal server: the publisher and the reminder helpers are fakes that
record their calls. The delivery log and the closed-appointment guard use the live Mongo
in a throw-away database, dropped at the end of the module.
"""

from __future__ import annotations

import asyncio
import datetime
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from kombu.exceptions import OperationalError as KombuOperationalError
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError
from temporalio.service import RPCError, RPCStatusCode

from libs.events.catalog import EventType, topic_for
from libs.events.consumer import EventEnvelope, NonRetryableEventError, TransientEventError
from services.notification import deliveries
from services.notification.config import NotificationSettings
from services.notification.tasks import QUEUE_FOR_TYPE, SendRequest
from services.notification.triggers import NotificationTriggers, dedupe_key_for

pytestmark = pytest.mark.integration

NOW = datetime.datetime(2026, 11, 11, 12, 0, tzinfo=datetime.UTC)
SLOT = NOW + datetime.timedelta(days=3)
NEW_SLOT = NOW + datetime.timedelta(days=4)
SLOT_ISO = SLOT.isoformat()


class FakeHelpers:
    """Stand-in for `services.notification.reminders`: records each call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail_with: BaseException | None = None

    async def _record(self, name: str, **kwargs: Any) -> Any:
        if self.fail_with is not None:
            raise self.fail_with
        self.calls.append((name, kwargs))
        return True

    async def start_reminder(self, client: Any, **kwargs: Any) -> bool:
        return await self._record("start_reminder", **kwargs)

    async def reschedule_reminder(self, client: Any, **kwargs: Any) -> None:
        await self._record("reschedule_reminder", **kwargs)

    async def cancel_reminder(self, client: Any, appointment_id: str) -> None:
        await self._record("cancel_reminder", appointment_id=appointment_id)

    async def start_followup(self, client: Any, **kwargs: Any) -> bool:
        return await self._record("start_followup", **kwargs)

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []
        self.fail_with: BaseException | None = None

    def __call__(self, **kwargs: str) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.calls.append(kwargs)


async def _fake_client() -> Any:
    return object()


@pytest.fixture(scope="module")
def db() -> Iterator[Database]:
    name = f"p3test_triggers_{uuid.uuid4().hex[:10]}"
    database = deliveries.get_client()[name]
    deliveries.ensure_notification_indexes(database)
    try:
        yield database
    finally:
        deliveries.get_client().drop_database(name)


@pytest.fixture
def publisher() -> FakePublisher:
    return FakePublisher()


@pytest.fixture
def helpers() -> FakeHelpers:
    return FakeHelpers()


@pytest.fixture
def triggers(db: Database, publisher: FakePublisher, helpers: FakeHelpers) -> NotificationTriggers:
    return NotificationTriggers(
        publisher=publisher,
        temporal=_fake_client,  # the fake helpers ignore the client
        deliveries_db=db,
        settings=NotificationSettings(),
        helpers=helpers,
        now=lambda: NOW,
    )


def _envelope(event_type: str, *, occurred_at: datetime.datetime = NOW, **payload: Any) -> EventEnvelope:
    event_id = uuid.uuid4()
    record = {
        "event_id": str(event_id),
        "event_type": event_type,
        "schema_version": 1,
        "occurred_at": occurred_at,
        "patient_id": str(uuid.uuid4()),
        "appointment_id": str(uuid.uuid4()),
        "clinic_id": str(uuid.uuid4()),
        "provider_id": str(uuid.uuid4()),
        **payload,
    }
    return EventEnvelope(
        event_id=event_id,
        event_type=event_type,
        aggregate_id=record["appointment_id"],
        occurred_at=occurred_at,
        schema_version=1,
        payload=record,
        topic=topic_for(event_type),
        partition=0,
        offset=0,
    )


def _booked(**kw: Any) -> EventEnvelope:
    return _envelope(EventType.APPOINTMENT_BOOKED, slot_start=SLOT, slot_end=SLOT, booked_at=NOW, **kw)


def _cancelled(**kw: Any) -> EventEnvelope:
    return _envelope(EventType.APPOINTMENT_CANCELLED, slot_start=SLOT, cancelled_at=NOW, **kw)


def _rescheduled(**kw: Any) -> EventEnvelope:
    return _envelope(
        EventType.APPOINTMENT_RESCHEDULED,
        old_slot_start=SLOT,
        new_slot_start=NEW_SLOT,
        new_slot_end=NEW_SLOT,
        rescheduled_at=NOW,
        **kw,
    )


def _delivery(db: Database, key: str) -> dict[str, Any] | None:
    return db[deliveries.DELIVERIES].find_one({"dedupe_key": key})


def test_booked_publishes_one_booking_send_and_starts_the_reminder(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers
) -> None:
    env = _booked()
    asyncio.run(triggers.on_booked(env))

    assert len(publisher.calls) == 1
    sent = publisher.calls[0]
    appointment_id = env.payload["appointment_id"]
    assert sent["notification_type"] == "appointment.booked"
    assert sent["dedupe_key"] == f"appointment.booked:{appointment_id}:{env.event_id}"
    assert sent["patient_id"] == env.payload["patient_id"]
    assert QUEUE_FOR_TYPE[sent["notification_type"]] == "notifications.booking"
    SendRequest(**sent)  # the task would accept exactly this
    assert helpers.calls == [
        (
            "start_reminder",
            {
                "appointment_id": appointment_id,
                "patient_id": env.payload["patient_id"],
                "slot_start_iso": SLOT_ISO,
                "lead_minutes": [1440, 120],
            },
        )
    ]


def test_cancelled_publishes_closes_the_appointment_and_cancels_the_reminder(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers, db: Database
) -> None:
    env = _cancelled()
    asyncio.run(triggers.on_cancelled(env))

    appointment_id = env.payload["appointment_id"]
    assert [c["dedupe_key"] for c in publisher.calls] == [f"appointment.cancelled:{appointment_id}:{env.event_id}"]
    assert helpers.calls == [("cancel_reminder", {"appointment_id": appointment_id})]
    assert deliveries.is_appointment_closed(db, appointment_id)
    assert db[deliveries.APPOINTMENT_STATE].find_one({"appointment_id": appointment_id})["closed_reason"] == "cancelled"


def test_rescheduled_publishes_and_moves_the_reminder_to_the_new_slot(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers
) -> None:
    env = _rescheduled()
    asyncio.run(triggers.on_rescheduled(env))

    appointment_id = env.payload["appointment_id"]
    assert [c["dedupe_key"] for c in publisher.calls] == [f"appointment.rescheduled:{appointment_id}:{env.event_id}"]
    assert helpers.calls == [
        (
            "reschedule_reminder",
            {
                "appointment_id": appointment_id,
                "patient_id": env.payload["patient_id"],
                "new_slot_iso": NEW_SLOT.isoformat(),
                "lead_minutes": [1440, 120],
            },
        )
    ]


def test_no_show_closes_and_cancels_without_a_notification(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers, db: Database
) -> None:
    env = _envelope(EventType.APPOINTMENT_NO_SHOW, slot_start=SLOT, marked_at=NOW)
    asyncio.run(triggers.on_no_show(env))

    appointment_id = env.payload["appointment_id"]
    assert publisher.calls == []
    assert helpers.calls == [("cancel_reminder", {"appointment_id": appointment_id})]
    assert db[deliveries.APPOINTMENT_STATE].find_one({"appointment_id": appointment_id})["closed_reason"] == "no_show"


def test_checked_in_cancels_the_reminder_and_publishes_nothing(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers, db: Database
) -> None:
    env = _envelope(EventType.APPOINTMENT_CHECKED_IN, slot_start=SLOT, checked_in_at=NOW)
    asyncio.run(triggers.on_checked_in(env))

    appointment_id = env.payload["appointment_id"]
    assert publisher.calls == []
    assert helpers.calls == [("cancel_reminder", {"appointment_id": appointment_id})]
    assert db[deliveries.APPOINTMENT_STATE].find_one({"appointment_id": appointment_id})["closed_reason"] == "checked_in"


def test_visit_completed_starts_the_followup_with_the_visits_ids(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers
) -> None:
    visit_id = str(uuid.uuid4())
    env = _envelope(
        EventType.VISIT_COMPLETED,
        visit_id=visit_id,
        checked_in_at=NOW,
        started_at=None,
        completed_at=NOW,
    )
    asyncio.run(triggers.on_visit_completed(env))

    assert publisher.calls == []
    assert helpers.calls == [
        (
            "start_followup",
            {
                "visit_id": visit_id,
                "appointment_id": env.payload["appointment_id"],
                "patient_id": env.payload["patient_id"],
                "completed_at_iso": NOW.isoformat(),
                "delay_minutes": 4320,
            },
        )
    ]


def test_only_the_six_notification_worthy_event_types_have_a_handler(triggers: NotificationTriggers) -> None:
    assert set(triggers.handlers()) == {
        "appointment.booked",
        "appointment.cancelled",
        "appointment.rescheduled",
        "appointment.no_show",
        "appointment.checked_in",
        "visit.completed",
    }
    for ignored in ("appointment.completed", "provider.schedule_changed", "patient.registered"):
        assert ignored not in triggers.handlers()


def test_the_same_envelope_twice_publishes_the_same_key_twice_and_raises_nothing(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers
) -> None:
    env = _booked()
    asyncio.run(triggers.on_booked(env))
    asyncio.run(triggers.on_booked(env))
    # The send task's delivery log and the provider's idempotency store dedupe the key;
    # the reminder helpers are idempotent too.
    assert len(publisher.calls) == 2
    assert publisher.calls[0]["dedupe_key"] == publisher.calls[1]["dedupe_key"]
    assert helpers.names() == ["start_reminder", "start_reminder"]


def test_dedupe_key_is_type_appointment_and_event_and_never_the_failure_prefix() -> None:
    appointment_id, event_id = str(uuid.uuid4()), str(uuid.uuid4())
    key = dedupe_key_for("appointment.booked", appointment_id, event_id)
    assert key == f"appointment.booked:{appointment_id}:{event_id}"
    assert not key.startswith("FAIL-")


def test_a_stale_booking_event_is_recorded_skipped_and_not_published_but_still_starts_the_reminder(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers, db: Database
) -> None:
    env = _envelope(
        EventType.APPOINTMENT_BOOKED,
        occurred_at=NOW - datetime.timedelta(minutes=1441),
        slot_start=SLOT,
        slot_end=SLOT,
        booked_at=NOW,
    )
    asyncio.run(triggers.on_booked(env))
    asyncio.run(triggers.on_booked(env))  # redelivered: the duplicate key is ignored

    key = f"appointment.booked:{env.payload['appointment_id']}:{env.event_id}"
    document = _delivery(db, key)
    assert document is not None and document["status"] == "skipped_stale"
    assert document["attempts"] == 0
    assert publisher.calls == []
    assert db[deliveries.PROVIDER_CALLS].count_documents({"dedupe_key": key}) == 0
    assert helpers.names() == ["start_reminder", "start_reminder"]


def test_an_event_just_inside_the_max_age_is_still_sent(
    triggers: NotificationTriggers, publisher: FakePublisher
) -> None:
    env = _envelope(
        EventType.APPOINTMENT_BOOKED,
        occurred_at=NOW - datetime.timedelta(minutes=1439),
        slot_start=SLOT,
        slot_end=SLOT,
        booked_at=NOW,
    )
    asyncio.run(triggers.on_booked(env))
    assert len(publisher.calls) == 1


def test_a_stale_cancellation_is_skipped_but_still_closes_and_cancels(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers, db: Database
) -> None:
    env = _envelope(
        EventType.APPOINTMENT_CANCELLED,
        occurred_at=NOW - datetime.timedelta(days=3),
        slot_start=SLOT,
        cancelled_at=NOW,
    )
    asyncio.run(triggers.on_cancelled(env))
    assert publisher.calls == []
    assert helpers.names() == ["cancel_reminder"]
    assert deliveries.is_appointment_closed(db, env.payload["appointment_id"])
    key = f"appointment.cancelled:{env.payload['appointment_id']}:{env.event_id}"
    assert _delivery(db, key)["status"] == "skipped_stale"


def test_a_skipped_record_never_overwrites_a_sent_delivery(db: Database) -> None:
    request = SendRequest(
        notification_type="appointment.booked",
        dedupe_key=f"appointment.booked:{uuid.uuid4()}:{uuid.uuid4()}",
        patient_id=str(uuid.uuid4()),
        appointment_id=str(uuid.uuid4()),
        requested_at=NOW.isoformat(),
    )
    from services.notification.deliveries import claim_delivery, mark_sent, record_skipped  # noqa: PLC0415

    claim_delivery(db, request)
    mark_sent(db, request.dedupe_key, "msg-1")
    record_skipped(db, request, "stale")
    assert _delivery(db, request.dedupe_key)["status"] == "sent"


@pytest.mark.parametrize(
    "failure",
    [KombuOperationalError("broker down"), ConnectionError("refused"), OSError("unreachable")],
)
def test_a_broker_outage_is_a_transient_event_error(
    triggers: NotificationTriggers, publisher: FakePublisher, failure: BaseException
) -> None:
    publisher.fail_with = failure
    with pytest.raises(TransientEventError):
        asyncio.run(triggers.on_booked(_booked()))


@pytest.mark.parametrize("status", [RPCStatusCode.UNAVAILABLE, RPCStatusCode.DEADLINE_EXCEEDED])
def test_a_temporal_outage_is_a_transient_event_error(
    triggers: NotificationTriggers, helpers: FakeHelpers, status: RPCStatusCode
) -> None:
    helpers.fail_with = RPCError("down", status, b"")
    with pytest.raises(TransientEventError):
        asyncio.run(triggers.on_booked(_booked()))
    with pytest.raises(TransientEventError):
        asyncio.run(triggers.on_visit_completed(_envelope(
            EventType.VISIT_COMPLETED, visit_id=str(uuid.uuid4()), completed_at=NOW)))


@pytest.mark.parametrize(
    "status", [RPCStatusCode.UNAVAILABLE, RPCStatusCode.INTERNAL, RPCStatusCode.UNKNOWN]
)
def test_unavailable_internal_and_unknown_are_kept_transient_and_bounded_by_the_cap(
    triggers: NotificationTriggers, helpers: FakeHelpers, status: RPCStatusCode
) -> None:
    """W3 (plan 03-21): a Temporal outage surfaces as several statuses, so they all stay
    `TransientEventError`; the consumer base's cap (TransientExhausted) bounds them."""
    helpers.fail_with = RPCError("down", status, b"")
    with pytest.raises(TransientEventError):
        asyncio.run(triggers.on_booked(_booked()))


@pytest.mark.parametrize(
    "status",
    [RPCStatusCode.INVALID_ARGUMENT, RPCStatusCode.NOT_FOUND, RPCStatusCode.PERMISSION_DENIED],
)
def test_temporal_request_errors_propagate_unchanged(
    triggers: NotificationTriggers, helpers: FakeHelpers, status: RPCStatusCode
) -> None:
    helpers.fail_with = RPCError("bad", status, b"")
    with pytest.raises(RPCError):
        asyncio.run(triggers.on_booked(_booked()))


def test_a_lost_temporal_connection_runtime_error_is_transient(
    triggers: NotificationTriggers, helpers: FakeHelpers
) -> None:
    helpers.fail_with = RuntimeError("Client closed")
    with pytest.raises(TransientEventError):
        asyncio.run(triggers.on_no_show(_envelope(EventType.APPOINTMENT_NO_SHOW, slot_start=SLOT, marked_at=NOW)))


def test_a_mongo_outage_is_a_transient_event_error(
    triggers: NotificationTriggers, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*args: Any, **kwargs: Any) -> None:
        raise ServerSelectionTimeoutError("mongo down")

    monkeypatch.setattr(deliveries, "mark_appointment_closed", boom)
    with pytest.raises(TransientEventError):
        asyncio.run(triggers.on_cancelled(_cancelled()))


def test_a_temporal_error_that_is_not_an_outage_is_not_transient(
    triggers: NotificationTriggers, helpers: FakeHelpers
) -> None:
    helpers.fail_with = RPCError("bad", RPCStatusCode.INVALID_ARGUMENT, b"")
    with pytest.raises(RPCError):
        asyncio.run(triggers.on_booked(_booked()))


def test_a_malformed_event_is_non_retryable_and_publishes_nothing(
    triggers: NotificationTriggers, publisher: FakePublisher, helpers: FakeHelpers
) -> None:
    missing = _booked()
    del missing.payload["patient_id"]
    with pytest.raises(NonRetryableEventError):
        asyncio.run(triggers.on_booked(missing))

    not_a_uuid = _booked(appointment_id="not-a-uuid")
    with pytest.raises(NonRetryableEventError):
        asyncio.run(triggers.on_booked(not_a_uuid))
    with pytest.raises(NonRetryableEventError):
        asyncio.run(triggers.on_cancelled(_cancelled(appointment_id="not-a-uuid")))
    assert publisher.calls == []
    assert helpers.calls == []


def test_publish_kwargs_are_strings_only_and_carry_ids_and_timestamps_only(
    triggers: NotificationTriggers, publisher: FakePublisher
) -> None:
    env = _booked()
    asyncio.run(triggers.on_booked(env))
    kwargs = publisher.calls[0]
    assert set(kwargs) == {
        "notification_type",
        "dedupe_key",
        "patient_id",
        "appointment_id",
        "event_id",
        "requested_at",
    }
    assert all(isinstance(value, str) for value in kwargs.values())
    for forbidden in ("slot_start", "clinic_id", "provider_id", "slot_end"):
        assert forbidden not in kwargs
