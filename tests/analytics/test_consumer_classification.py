"""Failure classification in the analytics-projection consumer (plan 03-21; WR-02, CR-01,
IN-01, EVENT-03, ANLY-06).

Unit level, no live database: the session factory raises before any query, and the
consumer base is driven through its real `_handle_message` with a stand-in for the
Kafka consumer (see `tests/events/standins.py`). The classification itself is the one
shared module, `libs.events.classify`; these tests prove the analytics handler uses it.
"""

from __future__ import annotations

import uuid
from types import TracebackType
from typing import Any

import asyncpg.exceptions as pg_exc
import pytest
import sqlalchemy.exc

from libs.events.catalog import EventType
from libs.events.consumer import EventEnvelope, NonRetryableEventError, Outcome, TransientEventError
from services.analytics import consumer as analytics_consumer
from services.analytics.consumer import build_consumer, handle_event
from tests.events.standins import (
    FakeMessage,
    StandInKafkaConsumer,
    event_record,
    install,
    stub_decoder,
)


class _DialectError(Exception):
    """What the SQLAlchemy asyncpg dialect puts in `DBAPIError.orig`."""

    def __init__(self, code: str) -> None:
        super().__init__("db error")
        self.sqlstate = code
        self.pgcode = code


class _RaisingSession:
    """A session whose `__aenter__` raises: a connection that cannot be opened."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    async def __aenter__(self) -> Any:
        raise self._error

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        return None


def _factory(error: BaseException) -> Any:
    return lambda: _RaisingSession(error)


def _envelope() -> EventEnvelope:
    import datetime  # noqa: PLC0415

    return EventEnvelope(
        event_id=uuid.uuid4(),
        event_type=EventType.PATIENT_REGISTERED,
        aggregate_id=str(uuid.uuid4()),
        occurred_at=datetime.datetime.now(datetime.UTC),
        schema_version=1,
        payload={},
        topic="smarthealth.patient.events",
        partition=0,
        offset=3,
    )


_TRANSIENT_ERRORS = [
    pytest.param(lambda: pg_exc.TooManyConnectionsError("too many"), id="raw-53300"),
    pytest.param(lambda: pg_exc.CannotConnectNowError("starting"), id="raw-57P03"),
    pytest.param(lambda: pg_exc.DeadlockDetectedError("deadlock"), id="raw-40P01"),
    pytest.param(
        lambda: sqlalchemy.exc.DBAPIError("SELECT 1", {}, _DialectError("53300")), id="dbapi-53300"
    ),
    pytest.param(
        lambda: sqlalchemy.exc.OperationalError("SELECT 1", {}, ConnectionRefusedError("down")),
        id="operational",
    ),
    pytest.param(lambda: ConnectionResetError("reset"), id="network"),
]


@pytest.mark.parametrize("make_error", _TRANSIENT_ERRORS)
async def test_a_database_resource_or_connection_error_is_transient(make_error: Any) -> None:
    with pytest.raises(TransientEventError):
        await handle_event(_envelope(), session_factory=_factory(make_error()))


async def test_a_unique_violation_is_reraised_unchanged() -> None:
    """Not an outage: it surfaces as itself so the base's bounded retries and the DLQ apply."""
    error = sqlalchemy.exc.DBAPIError("INSERT", {}, _DialectError("23505"))
    with pytest.raises(sqlalchemy.exc.DBAPIError) as raised:
        await handle_event(_envelope(), session_factory=_factory(error))
    assert raised.value is error


async def test_a_payload_key_error_is_still_non_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Db:
        def begin(self) -> Any:
            return _Begin()

    class _Begin:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *exc: object) -> None:
            return None

    class _Session:
        async def __aenter__(self) -> _Db:
            return _Db()

        async def __aexit__(self, *exc: object) -> None:
            return None

    async def claim(*args: Any, **kwargs: Any) -> bool:
        return True

    async def apply(*args: Any, **kwargs: Any) -> None:
        raise KeyError("registered_at")

    monkeypatch.setattr(analytics_consumer, "claim_event", claim)
    monkeypatch.setattr(analytics_consumer, "apply_event", apply)
    with pytest.raises(NonRetryableEventError, match="InvalidEvent"):
        await handle_event(_envelope(), session_factory=lambda: _Session())  # type: ignore[arg-type,return-value]


@pytest.mark.parametrize("make_error", _TRANSIENT_ERRORS[:4])
def test_the_consumer_base_holds_a_projection_event_on_a_database_resource_error(
    make_error: Any,
) -> None:
    """WR-02 at the real `_handle_message`: RETRY_LATER with one seek, no commit, no DLQ."""

    async def handler(envelope: EventEnvelope) -> None:
        await handle_event(envelope, session_factory=_factory(make_error()))

    consumer = build_consumer(
        handlers={EventType.PATIENT_REGISTERED: handler},
        metrics_port=None,
        heartbeat_path=None,
        backoff_seconds=0.01,
    )
    stand_in = StandInKafkaConsumer()
    install(consumer, stand_in)
    consumer._deserializer = stub_decoder(  # type: ignore[assignment]  # noqa: SLF001
        event_record(EventType.PATIENT_REGISTERED, patient_id=str(uuid.uuid4()))
    )
    try:
        message = FakeMessage(topic="smarthealth.patient.events", offset=12)
        assert consumer._handle_message(message) is Outcome.RETRY_LATER  # noqa: SLF001
        assert stand_in.seeks == [("smarthealth.patient.events", 0, 12)]
        assert stand_in.commits == []
        assert consumer._dlq_producer is None  # noqa: SLF001
    finally:
        consumer.close()
