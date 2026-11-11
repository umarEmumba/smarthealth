"""The one failure classifier for every Phase 3 event consumer (plan 03-21; IN-01,
CR-01, WR-02, WR-03).

The consumer base (`libs.events.consumer`) acts on a two-way signal from a handler:
`TransientEventError` ("an infrastructure outage, hold the event and retry") versus
anything else ("a poison or buggy event, bounded retries, then the DLQ"). This module
decides which side a raised exception falls on, for visit-downstream,
analytics-projection and notification-trigger alike. No consumer keeps its own copy
(an AST guard in `tests/events/test_classify.py` fails the build if one appears).

Database failures are classified by SQLSTATE class first, not by SQLAlchemy exception
class: the asyncpg dialect only translates a few asyncpg errors into
`OperationalError` / `InterfaceError` and surfaces the rest (53300 too many
connections, 57P03 cannot connect now, 40P01 deadlock) as a generic `DBAPIError`, or as
a raw asyncpg exception when it is raised while a connection opens. The transient
classes are

* 08 - connection exception (connection does not exist, failure, refused),
* 53 - insufficient resources (too many connections, out of memory, disk full),
* 57 - operator intervention (cannot connect now, admin shutdown, query cancelled),
* 40 - transaction rollback (deadlock, serialization failure).

A code of any other class (23505 unique violation, 42P01 undefined table, ...) is NOT
transient, whatever SQLAlchemy class wraps it. Without a code the SQLAlchemy exception
class decides (`OperationalError`, `InterfaceError`, an invalidated connection, and the
pool checkout `TimeoutError`, which is resource pressure, not a poison event).

Temporal: an outage surfaces as several statuses (UNAVAILABLE, DEADLINE_EXCEEDED,
RESOURCE_EXHAUSTED, ABORTED, CANCELLED, UNKNOWN, INTERNAL) and, in the SDK, as a
`RuntimeError` for a closed or lost client. They all stay transient: narrowing the set
would turn a frontend restart into a dead letter after a few quick attempts, which is
the CR-01 failure mode again. What changes is that they are BOUNDED: the consumer base
dead-letters a message that keeps failing transiently, after both its attempt floor
and its time cap, as `TransientExhausted` (visible in `dlq_events_total` and on the DLQ
topic, recoverable by replay). A deterministic bug that happens to look transient can
therefore delay one partition for a bounded time, never block it for ever (WR-03).

The classifier never reads or logs an exception message: it looks at classes, status
codes and SQLSTATE codes only (PHI rule, T-3-64).
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "TRANSIENT_RPC_STATUSES",
    "TRANSIENT_SQLSTATE_CLASSES",
    "is_transient",
    "is_transient_broker",
    "is_transient_db",
    "is_transient_network",
    "is_transient_registry",
    "is_transient_temporal",
    "sqlstate_of",
]

# 08 connection, 53 insufficient resources, 57 operator intervention, 40 rollback.
TRANSIENT_SQLSTATE_CLASSES: tuple[str, ...] = ("08", "53", "57", "40")

# Member NAMES of `temporalio.service.RPCStatusCode` (names, not members, so the
# analytics consumer does not have to import the Temporal SDK).
TRANSIENT_RPC_STATUSES: frozenset[str] = frozenset(
    {
        "UNAVAILABLE",
        "DEADLINE_EXCEEDED",
        "RESOURCE_EXHAUSTED",
        "ABORTED",
        "CANCELLED",
        "UNKNOWN",
        "INTERNAL",
    }
)

_MAX_CHAIN_HOPS = 5
_SQLSTATE_ATTRIBUTES = ("sqlstate", "pgcode")


def sqlstate_of(exc: BaseException) -> str | None:
    """The first 5-character SQLSTATE found on `exc`, its `orig`, then its
    `__cause__` / `__context__` chain (depth-bounded and cycle-safe), else None."""
    seen: set[int] = set()
    frontier: list[Any] = [exc]
    for _ in range(_MAX_CHAIN_HOPS + 1):
        following: list[Any] = []
        for candidate in frontier:
            if candidate is None or id(candidate) in seen:
                continue
            seen.add(id(candidate))
            for attribute in _SQLSTATE_ATTRIBUTES:
                code = getattr(candidate, attribute, None)
                if isinstance(code, str) and len(code) == 5:
                    return code
            following.extend(
                (
                    getattr(candidate, "orig", None),
                    getattr(candidate, "__cause__", None),
                    getattr(candidate, "__context__", None),
                )
            )
        if not following:
            break
        frontier = following
    return None


def is_transient_db(exc: BaseException) -> bool:
    """A Postgres / SQLAlchemy failure that waiting can cure (see the module docstring)."""
    code = sqlstate_of(exc)
    if code is not None:
        return code[:2] in TRANSIENT_SQLSTATE_CLASSES

    import sqlalchemy.exc as sa_exc  # noqa: PLC0415 - kept off the import path of services without a database

    if isinstance(exc, (sa_exc.OperationalError, sa_exc.InterfaceError, sa_exc.TimeoutError)):
        return True
    if isinstance(exc, sa_exc.DBAPIError) and exc.connection_invalidated:
        return True
    try:
        import asyncpg.exceptions as pg_exc  # noqa: PLC0415
    except ImportError:  # pragma: no cover - asyncpg ships with the project
        return False
    # asyncpg's client-side InterfaceError (closed connection, cancelled operation) carries
    # no SQLSTATE; it is raised raw when a connection cannot be used.
    return isinstance(exc, pg_exc.InterfaceError)


def is_transient_temporal(exc: BaseException) -> bool:
    """A Temporal outage: an outage-class `RPCError` status, or the SDK's `RuntimeError`
    for a closed or lost client (bounded by the consumer base's cap, WR-03)."""
    from temporalio.service import RPCError  # noqa: PLC0415

    if isinstance(exc, RPCError):
        return exc.status.name in TRANSIENT_RPC_STATUSES
    return isinstance(exc, RuntimeError)


def is_transient_broker(exc: BaseException) -> bool:
    """A Celery broker (kombu / AMQP) or Mongo failure."""
    from amqp.exceptions import AMQPError  # noqa: PLC0415
    from kombu.exceptions import KombuError  # noqa: PLC0415
    from pymongo.errors import PyMongoError  # noqa: PLC0415

    return isinstance(exc, (KombuError, AMQPError, PyMongoError))


def is_transient_network(exc: BaseException) -> bool:
    """A socket-level failure (timeouts, refused or reset connections, unreachable host)."""
    return isinstance(exc, (TimeoutError, ConnectionError, OSError))


def is_transient_registry(exc: BaseException) -> bool:
    """A Schema Registry decode failure that waiting can cure (WR-02): a 408, 429 or 5xx
    answer, an answer without a usable status, or a transport failure. Any other 4xx
    (an unknown schema id: forged or foreign) is poison."""
    import httpx  # noqa: PLC0415
    from confluent_kafka.schema_registry.error import SchemaRegistryError  # noqa: PLC0415

    if isinstance(exc, SchemaRegistryError):
        status = getattr(exc, "http_status_code", -1)
        if not isinstance(status, int) or isinstance(status, bool):
            return True
        if status in (408, 429):
            return True
        return not 400 <= status < 500
    return isinstance(exc, httpx.TransportError) or is_transient_network(exc)


def is_transient(
    exc: BaseException,
    *,
    db: bool = True,
    temporal: bool = False,
    broker: bool = False,
) -> bool:
    """The composite the consumers call: database (on by default), Temporal and broker
    (off by default), network always."""
    if db and is_transient_db(exc):
        return True
    if temporal and is_transient_temporal(exc):
        return True
    if broker and is_transient_broker(exc):
        return True
    return is_transient_network(exc)
