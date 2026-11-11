"""The shared failure classifier and its single-classifier guard (plan 03-21; IN-01,
CR-01, WR-02, WR-03, T-3-66).

Pure unit tests: no stack. The exceptions are the REAL asyncpg, SQLAlchemy, Temporal,
kombu, amqp and pymongo classes, so a SQLSTATE or class that a library changes shows up
here rather than in production.

The AST guard (`test_no_transient_classifier_outside_classify`) keeps
`libs/events/classify.py` the only place that decides what is transient. It is NOT
name-based only: besides a classifier-shaped function name and a status / SQLSTATE
literal it finds a transient exception class tested in an `isinstance` call or caught
by an `except` handler that turns it into a `TransientEventError` / `NonRetryableEventError`,
a read of `sqlstate` / `pgcode`, a transient Temporal status used in any expression, and
a transient class name compared as a string. Every detector has a positive control on a
synthetic offender with an innocuous function name, so the guard is proven able to fail.

Allow rule (what is NOT classification): ordinary try/except handling is fine. An
`except OSError:` (or `RPCError`, `TimeoutError`, ...) that logs, returns a default,
re-raises something else or compares a status to `NOT_FOUND` is domain or cleanup
handling and is allowed; so is `isinstance(cause, TimeoutError)` when `TimeoutError` is
the Temporal activity-timeout class imported from `temporalio.exceptions` (a different
class, used to unwrap a workflow failure). Only a handler that names a
`Transient...Error` / `NonRetryable...Error` (a verdict for the consumer base) counts.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

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

REPO = Path(__file__).resolve().parents[2]


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


# ------------------------------------------------------------------- AST guard

TRANSIENT_CLASS_NAMES = frozenset(
    {
        "OperationalError",
        "InterfaceError",
        "DBAPIError",
        "TimeoutError",
        "ConnectionError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "BrokenPipeError",
        "OSError",
        "RPCError",
        "KombuError",
        "AMQPError",
        "PyMongoError",
        "ServerSelectionTimeoutError",
        "AutoReconnect",
        "TooManyConnectionsError",
        "CannotConnectNowError",
        "DeadlockDetectedError",
        "PostgresError",
        "TransportError",
        "SchemaRegistryError",
    }
)
TRANSIENT_RPC_MEMBERS = frozenset(
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
SQLSTATE_CLASSES = frozenset({"08", "53", "57", "40"})
SQLSTATE_ATTRIBUTES = frozenset({"sqlstate", "pgcode"})
# `TimeoutError` imported from here is Temporal's activity-timeout class, not the builtin.
NON_CLASSIFYING_IMPORT_MODULES = ("temporalio.exceptions",)
CLASSIFIER_FUNCTION_NAME = re.compile(r"^_*(is_)?(transient|retryable)", re.IGNORECASE)
VERDICT_ERROR_NAME = re.compile(r"(Transient|NonRetryable)\w*Error")

SHAPE_FUNCTION = "classifier-function"
SHAPE_STATUS_SET = "rpc-status-set"
SHAPE_SQLSTATE_SET = "sqlstate-collection"
SHAPE_ISINSTANCE = "transient-isinstance"
SHAPE_EXCEPT = "transient-except-classification"
SHAPE_SQLSTATE_READ = "sqlstate-read"
SHAPE_RPC_STATUS = "transient-rpc-status"
SHAPE_NAME_STRING = "transient-class-name-string"


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    shape: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.shape}"


def _terminal_names(node: ast.AST) -> set[str]:
    """The last name of each class reference in `node` (a Name, an Attribute, or a
    tuple / list / set of them)."""
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {node.attr}
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        found: set[str] = set()
        for element in node.elts:
            found |= _terminal_names(element)
        return found
    return set()


def _shadowed_names(tree: ast.Module) -> set[str]:
    """Names imported from a module whose classes only share a name with a transient
    class (see NON_CLASSIFYING_IMPORT_MODULES)."""
    shadowed: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in NON_CLASSIFYING_IMPORT_MODULES:
            shadowed |= {alias.asname or alias.name for alias in node.names}
    return shadowed


def _transient_names(node: ast.AST, shadowed: set[str]) -> set[str]:
    return {
        name
        for name in _terminal_names(node)
        if name in TRANSIENT_CLASS_NAMES and not (isinstance(node, ast.Name) and name in shadowed)
    }


def _returns_bool_or_nothing(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    annotation = node.returns
    if annotation is None:
        return True
    if isinstance(annotation, ast.Name):
        return annotation.id == "bool"
    return isinstance(annotation, ast.Constant) and annotation.value == "bool"


def _is_sqlstate_class(value: object) -> bool:
    return isinstance(value, str) and (
        value in SQLSTATE_CLASSES or (len(value) == 5 and value[:2] in SQLSTATE_CLASSES)
    )


def _collection_findings(node: ast.Set | ast.Tuple | ast.List) -> list[str]:
    """Shapes of one literal collection: a status set, a SQLSTATE set, a class-name set."""
    shapes: list[str] = []
    elements = node.elts
    if any(
        isinstance(e, ast.Attribute) and isinstance(e.value, ast.Name) and e.value.id == "RPCStatusCode"
        for e in elements
    ):
        shapes.append(SHAPE_STATUS_SET)
    if sum(1 for e in elements if isinstance(e, ast.Constant) and _is_sqlstate_class(e.value)) >= 2:
        shapes.append(SHAPE_SQLSTATE_SET)
    if any(
        isinstance(e, ast.Constant) and isinstance(e.value, str) and e.value in TRANSIENT_CLASS_NAMES
        for e in elements
    ):
        shapes.append(SHAPE_NAME_STRING)
    return shapes


def scan_source(source: str, path: str) -> list[Finding]:
    """Every place in `source` that decides what is transient (see the module docstring)."""
    tree = ast.parse(source, filename=path)
    shadowed = _shadowed_names(tree)
    findings: list[Finding] = []
    covered: set[int] = set()  # nodes already reported as part of a literal collection

    def add(node: ast.AST, shape: str) -> None:
        findings.append(Finding(path, getattr(node, "lineno", 0), shape))

    for node in ast.walk(tree):
        # (a) a classifier-shaped function (name AND bool / missing return annotation).
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and CLASSIFIER_FUNCTION_NAME.match(node.name)
            and _returns_bool_or_nothing(node)
        ):
            add(node, SHAPE_FUNCTION)
        # (b), (c), name strings: a literal collection of statuses, SQLSTATE codes or
        # class names. Reported once, at the collection, whatever it is assigned to.
        if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
            shapes = _collection_findings(node)
            for shape in shapes:
                add(node, shape)
            if shapes:
                covered |= {id(child) for child in ast.walk(node)}
        # an isinstance() test of a transient class.
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "isinstance"
            and len(node.args) == 2
            and _transient_names(node.args[1], shadowed)
        ):
            add(node, SHAPE_ISINSTANCE)
        # an except handler that names a transient class AND gives a verdict for the
        # consumer base (raises / mentions a Transient...Error or NonRetryable...Error).
        if isinstance(node, ast.ExceptHandler) and node.type is not None:
            if _transient_names(node.type, shadowed) and any(
                (isinstance(inner, ast.Name) and VERDICT_ERROR_NAME.search(inner.id))
                or (isinstance(inner, ast.Attribute) and VERDICT_ERROR_NAME.search(inner.attr))
                for statement in node.body
                for inner in ast.walk(statement)
            ):
                add(node, SHAPE_EXCEPT)
        # a read of sqlstate / pgcode (attribute, or getattr / dict key by name).
        if isinstance(node, ast.Attribute) and node.attr in SQLSTATE_ATTRIBUTES:
            add(node, SHAPE_SQLSTATE_READ)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in SQLSTATE_ATTRIBUTES:
            add(node, SHAPE_SQLSTATE_READ)
        # a transient Temporal status used anywhere (compare, set, membership).
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "RPCStatusCode"
            and node.attr in TRANSIENT_RPC_MEMBERS
            and id(node) not in covered
        ):
            add(node, SHAPE_RPC_STATUS)
        # `type(exc).__name__ == "OperationalError"`.
        if isinstance(node, ast.Compare):
            operands = [node.left, *node.comparators]
            if any(isinstance(o, ast.Attribute) and o.attr == "__name__" for o in operands) and any(
                isinstance(o, ast.Constant) and isinstance(o.value, str) and o.value in TRANSIENT_CLASS_NAMES
                for o in operands
            ):
                add(node, SHAPE_NAME_STRING)
    return findings


def scan_repository() -> list[Finding]:
    """`services/` and `libs/events/`, minus `libs/events/classify.py` itself."""
    excluded = REPO / "libs" / "events" / "classify.py"
    findings: list[Finding] = []
    for root in (REPO / "services", REPO / "libs" / "events"):
        for file in sorted(root.rglob("*.py")):
            if file == excluded or "__pycache__" in file.parts:
                continue
            findings += scan_source(file.read_text(encoding="utf-8"), str(file.relative_to(REPO)))
    return findings


def test_no_transient_classifier_outside_classify() -> None:
    findings = scan_repository()
    assert findings == [], "transient classification outside libs/events/classify.py:\n" + "\n".join(
        str(finding) for finding in findings
    )


# Positive controls: the guard must FAIL on a synthetic offender of every shape. The
# function names are deliberately innocuous so a name-based scan could not find them.

_ORIGINAL_THREE = '''
def _is_transient(exc):
    return False

_STATUSES = frozenset({RPCStatusCode.UNAVAILABLE, RPCStatusCode.INTERNAL})
_CLASSES = ("08", "53")
'''

_OFFENDERS: dict[str, tuple[str, list[str]]] = {
    "isinstance of transient classes": (
        "def _should_hold(exc):\n    return isinstance(exc, (OperationalError, TimeoutError))\n",
        [SHAPE_ISINSTANCE],
    ),
    "isinstance with an attribute class": (
        "def _should_hold(exc):\n    return isinstance(exc, sqlalchemy.exc.InterfaceError)\n",
        [SHAPE_ISINSTANCE],
    ),
    "except that gives a transient verdict": (
        "async def work(env):\n"
        "    try:\n        await go()\n"
        "    except (ConnectionError, OSError) as exc:\n"
        "        raise TransientEventError('x') from exc\n",
        [SHAPE_EXCEPT],
    ),
    "except that gives a non-retryable verdict on a transient class": (
        "async def work(env):\n"
        "    try:\n        await go()\n"
        "    except RPCError:\n"
        "        raise NonRetryableEventError('x')\n",
        [SHAPE_EXCEPT],
    ),
    "except that maps to a private transient error": (
        "def decode(raw):\n"
        "    try:\n        return load(raw)\n"
        "    except pymongo.errors.PyMongoError as exc:\n"
        "        raise _TransientDecodeError from exc\n",
        [SHAPE_EXCEPT],
    ),
    "sqlstate attribute read": (
        "def _hold(exc):\n    return exc.orig.sqlstate.startswith('53')\n",
        [SHAPE_SQLSTATE_READ],
    ),
    "pgcode via getattr": (
        "def _hold(exc):\n    return getattr(exc, 'pgcode', None) == '53300'\n",
        [SHAPE_SQLSTATE_READ],
    ),
    "transient status in a comparison": (
        "def _hold(exc):\n    return exc.status == RPCStatusCode.UNAVAILABLE\n",
        [SHAPE_RPC_STATUS],
    ),
    "transient status in a membership test": (
        "def _hold(exc):\n    return exc.status in (RPCStatusCode.INTERNAL,)\n",
        [SHAPE_STATUS_SET],
    ),
    "class name compared as a string": (
        "def _hold(exc):\n    return type(exc).__name__ == 'OperationalError'\n",
        [SHAPE_NAME_STRING],
    ),
    "class names in a set": (
        "def _hold(exc):\n    return type(exc).__name__ in {'OperationalError', 'InterfaceError'}\n",
        [SHAPE_NAME_STRING],
    ),
}


def test_positive_control_the_name_based_shapes_are_found() -> None:
    """One function, one status set and one SQLSTATE tuple: exactly three findings."""
    findings = scan_source(_ORIGINAL_THREE, "synthetic.py")
    assert sorted(f.shape for f in findings) == sorted([SHAPE_FUNCTION, SHAPE_STATUS_SET, SHAPE_SQLSTATE_SET])
    assert len(findings) == 3


@pytest.mark.parametrize("name", list(_OFFENDERS))
def test_positive_control_the_structural_shapes_are_found(name: str) -> None:
    source, expected = _OFFENDERS[name]
    assert sorted(f.shape for f in scan_source(source, "synthetic.py")) == sorted(expected)


def test_positive_control_a_misnamed_classifier_is_still_found() -> None:
    """The guard does not depend on the function being called `is_transient`."""
    source = (
        "def keep_for_later(exc):\n"
        "    if isinstance(exc, (OperationalError, ConnectionError)):\n"
        "        return True\n"
        "    return False\n"
    )
    assert [f.shape for f in scan_source(source, "synthetic.py")] == [SHAPE_ISINSTANCE]


_ALLOWED = {
    "except that only logs": (
        "def read():\n    try:\n        return open_file()\n"
        "    except OSError as exc:\n        log.warning('x', error=type(exc).__name__)\n        return None\n"
    ),
    "except that re-raises something else": (
        "def connect():\n    try:\n        dial()\n"
        "    except (ConnectionError, TimeoutError) as exc:\n        raise HTTPException(502) from exc\n"
    ),
    "a domain status check": (
        "async def exists(handle):\n    try:\n        await handle.describe()\n"
        "    except RPCError as exc:\n        if exc.status == RPCStatusCode.NOT_FOUND:\n            return False\n"
        "        raise\n    return True\n"
    ),
    "the temporal activity timeout": (
        "from temporalio.exceptions import ActivityError, TimeoutError\n"
        "def unwrap(cause):\n    return isinstance(cause, TimeoutError)\n"
    ),
    "calling the shared classifier": (
        "async def handle(env):\n    try:\n        await go()\n    except Exception as exc:\n"
        "        if classify.is_transient(exc, temporal=True):\n            raise TransientEventError(type(exc).__name__) from exc\n"
        "        raise\n"
    ),
    "poison mapping of non-transient classes": (
        "def parse(raw):\n    try:\n        return load(raw)\n"
        "    except (KeyError, ValueError) as exc:\n        raise NonRetryableEventError('InvalidEvent') from exc\n"
    ),
    "a function named like a transient helper that returns an exception": (
        "def _retryable(exc) -> Exception:\n    return exc\n"
    ),
}


@pytest.mark.parametrize("name", list(_ALLOWED))
def test_negative_control_ordinary_handling_is_not_flagged(name: str) -> None:
    assert scan_source(_ALLOWED[name], "synthetic.py") == []
