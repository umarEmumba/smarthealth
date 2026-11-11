"""The shared failure classifier (plan 03-21; IN-01,
CR-01, WR-02, WR-03, T-3-66).

Pure unit tests: no stack. The exceptions are the REAL asyncpg, SQLAlchemy, Temporal,
kombu, amqp and pymongo classes, so a SQLSTATE or class that a library changes shows up
here rather than in production.
"""

from __future__ import annotations

import amqp.exceptions
import asyncpg.exceptions as pg
import httpx
import kombu.exceptions
import pymongo.errors
import pytest
import sqlalchemy.exc as sa
from confluent_kafka.schema_registry.error import SchemaRegistryError
from temporalio.service import RPCError, RPCStatusCode

from libs.events import classify

class _DialectError(Exception):
    """What the SQLAlchemy asyncpg dialect puts in `DBAPIError.orig`: an exception with
    `sqlstate` and `pgcode` set from the asyncpg error."""

    def __init__(self, code: str) -> None:
        super().__init__("db error")
        self.sqlstate = code
        self.pgcode = code


def _dbapi(code: str) -> sa.DBAPIError:
    return sa.DBAPIError("SELECT 1", {}, _DialectError(code))


# ----------------------------------------------------------------- sqlstate_of


def test_sqlstate_of_reads_a_raw_asyncpg_error() -> None:
    assert classify.sqlstate_of(pg.TooManyConnectionsError("too many")) == "53300"


def test_sqlstate_of_reads_the_orig_of_a_dbapi_error() -> None:
    assert classify.sqlstate_of(_dbapi("53300")) == "53300"
    assert classify.sqlstate_of(sa.DBAPIError("SELECT 1", {}, pg.CannotConnectNowError("x"))) == "57P03"


def test_sqlstate_of_follows_the_cause_chain() -> None:
    outer = RuntimeError("wrapper")
    outer.__cause__ = pg.TooManyConnectionsError("too many")
    assert classify.sqlstate_of(outer) == "53300"
    contextual = ValueError("raised while handling")
    contextual.__context__ = pg.DeadlockDetectedError("deadlock")
    assert classify.sqlstate_of(contextual) == "40P01"


def test_sqlstate_of_is_none_without_a_code_and_survives_cycles_and_long_chains() -> None:
    assert classify.sqlstate_of(ValueError("plain")) is None
    first, second = RuntimeError("a"), RuntimeError("b")
    first.__cause__, second.__cause__ = second, first
    assert classify.sqlstate_of(first) is None  # a cycle neither hangs nor finds a code
    chain = RuntimeError("head")
    tail = chain
    for _ in range(10):
        nxt = RuntimeError("link")
        tail.__cause__ = nxt
        tail = nxt
    tail.__cause__ = pg.TooManyConnectionsError("far away")
    assert classify.sqlstate_of(chain) is None  # the walk is depth-bounded


def test_sqlstate_of_ignores_a_value_that_is_not_a_five_character_code() -> None:
    odd = RuntimeError("x")
    odd.sqlstate = "53"  # type: ignore[attr-defined]
    assert classify.sqlstate_of(odd) is None


# ---------------------------------------------------------------- is_transient_db


@pytest.mark.parametrize(
    "error",
    [
        pg.TooManyConnectionsError("x"),  # 53300
        pg.CannotConnectNowError("x"),  # 57P03
        pg.DeadlockDetectedError("x"),  # 40P01
        pg.SerializationError("x"),  # 40001
        pg.AdminShutdownError("x"),  # 57P01
        pg.ConnectionDoesNotExistError("x"),  # 08003
        _dbapi("53300"),
        _dbapi("57P03"),
        _dbapi("40P01"),
        sa.OperationalError("SELECT 1", {}, ConnectionRefusedError("down")),
        sa.InterfaceError("SELECT 1", {}, ConnectionResetError("reset")),
        sa.DBAPIError("SELECT 1", {}, RuntimeError("x"), connection_invalidated=True),
        sa.TimeoutError("QueuePool limit of size 5 overflow 10 reached"),
        pg.InterfaceError("connection is closed"),
    ],
    ids=lambda e: f"{type(e).__name__}-{classify.sqlstate_of(e)}",
)
def test_database_resource_and_connection_errors_are_transient(error: BaseException) -> None:
    assert classify.is_transient_db(error) is True
    assert classify.is_transient(error) is True


@pytest.mark.parametrize(
    "error",
    [
        _dbapi("23505"),  # unique violation, whatever class wraps it (T-3-66)
        _dbapi("42P01"),  # undefined table
        pg.UniqueViolationError("x"),
        pg.UndefinedTableError("x"),
        sa.IntegrityError("INSERT", {}, _DialectError("23505")),
        # A code that is present decides: an OperationalError wrapping 23505 is not retried.
        sa.OperationalError("SELECT 1", {}, _DialectError("23505")),
        KeyError("missing"),
        ValueError("bad"),
    ],
    ids=lambda e: f"{type(e).__name__}-{classify.sqlstate_of(e)}",
)
def test_other_database_errors_and_plain_bugs_are_not_transient(error: BaseException) -> None:
    assert classify.is_transient_db(error) is False
    assert classify.is_transient(error) is False


# ---------------------------------------------------------- Temporal / broker / net


@pytest.mark.parametrize(
    "status",
    [
        RPCStatusCode.UNAVAILABLE,
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.RESOURCE_EXHAUSTED,
        RPCStatusCode.ABORTED,
        RPCStatusCode.CANCELLED,
        RPCStatusCode.UNKNOWN,
        RPCStatusCode.INTERNAL,
    ],
)
def test_temporal_outage_statuses_are_transient(status: RPCStatusCode) -> None:
    error = RPCError("down", status, b"")
    assert classify.is_transient_temporal(error) is True
    assert classify.is_transient(error, temporal=True) is True
    assert classify.is_transient(error) is False  # the analytics composite has no Temporal branch


@pytest.mark.parametrize(
    "status",
    [
        RPCStatusCode.INVALID_ARGUMENT,
        RPCStatusCode.NOT_FOUND,
        RPCStatusCode.ALREADY_EXISTS,
        RPCStatusCode.FAILED_PRECONDITION,
        RPCStatusCode.PERMISSION_DENIED,
    ],
)
def test_temporal_request_errors_are_not_transient(status: RPCStatusCode) -> None:
    error = RPCError("bad", status, b"")
    assert classify.is_transient_temporal(error) is False
    assert classify.is_transient(error, temporal=True, broker=True) is False


def test_the_closed_client_runtime_error_is_transient_only_with_the_temporal_branch() -> None:
    error = RuntimeError("Client closed")
    assert classify.is_transient_temporal(error) is True
    assert classify.is_transient(error, temporal=True) is True
    assert classify.is_transient(error) is False  # a RuntimeError in analytics is a bug


def test_the_transient_rpc_status_set_is_the_seven_outage_statuses() -> None:
    assert classify.TRANSIENT_RPC_STATUSES == {
        "UNAVAILABLE",
        "DEADLINE_EXCEEDED",
        "RESOURCE_EXHAUSTED",
        "ABORTED",
        "CANCELLED",
        "UNKNOWN",
        "INTERNAL",
    }
    assert {member.name for member in RPCStatusCode} >= classify.TRANSIENT_RPC_STATUSES
    assert classify.TRANSIENT_SQLSTATE_CLASSES == ("08", "53", "57", "40")


@pytest.mark.parametrize(
    "error",
    [
        kombu.exceptions.OperationalError("broker down"),
        amqp.exceptions.ConnectionForced("forced"),
        pymongo.errors.ServerSelectionTimeoutError("mongo down"),
        pymongo.errors.AutoReconnect("reconnect"),
    ],
)
def test_broker_and_mongo_errors_are_transient_with_the_broker_branch(error: BaseException) -> None:
    assert classify.is_transient_broker(error) is True
    assert classify.is_transient(error, db=False, temporal=True, broker=True) is True
    assert classify.is_transient(error) is False


@pytest.mark.parametrize("error", [TimeoutError(), ConnectionError(), ConnectionRefusedError(), OSError()])
def test_network_errors_are_always_transient(error: BaseException) -> None:
    assert classify.is_transient_network(error) is True
    assert classify.is_transient(error) is True
    assert classify.is_transient(error, db=False) is True


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, -1])
def test_registry_back_pressure_and_server_errors_are_transient(status: int) -> None:
    assert classify.is_transient_registry(SchemaRegistryError(status, 1, "x")) is True


@pytest.mark.parametrize("status", [400, 401, 404, 409, 422])
def test_other_registry_client_errors_are_poison(status: int) -> None:
    assert classify.is_transient_registry(SchemaRegistryError(status, 1, "x")) is False


def test_registry_transport_failures_are_transient_and_bytes_errors_are_not() -> None:
    assert classify.is_transient_registry(httpx.ConnectError("refused")) is True
    assert classify.is_transient_registry(ConnectionError()) is True
    assert classify.is_transient_registry(ValueError("corrupt avro")) is False


def test_the_classifier_never_exposes_the_exception_message() -> None:
    secret = "patient Jane Doe 1980-01-01"
    error = sa.OperationalError("SELECT 1", {}, ConnectionRefusedError(secret))
    # Classification is a bool computed from classes and codes; nothing text-shaped returns.
    assert classify.is_transient(error) is True
    assert classify.sqlstate_of(error) is None
