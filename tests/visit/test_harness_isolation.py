"""Harness isolation from the live visit-reconciler (plan 03-22; W5, W6, orchestrator W-1).

The Compose `visit-worker` restarts, at start and every 60 s, every completed visit in the
shared visit_db whose `downstream_started_at` is still NULL after a 10 minute grace. The
harness tests in this directory use the same database, so:

* every `restart_lost_triggers` / `list_lost_triggers` call under tests/visit must pass
  `clinic_id` (an unscoped pass would mark live dev visits as started and start their
  workflows on a throw-away Temporal server);
* `VisitActivities.reconcile()` may only be called in a test function that replaces BOTH of
  its pass functions (`reconcile_unsettled`, `restart_lost_triggers`) by recorders, and a
  `VisitActivities` built with a Temporal client is held to the same rule;
* a completed NULL-marker harness visit must not survive the session (the guard in
  `conftest.py`).

The AST check below enforces the first two over the whole tests/visit tree; its positive
controls prove it fails on a synthetic offender of each shape.
"""

from __future__ import annotations

import ast
import pathlib
import uuid
from typing import Any

import httpx
import pytest
from httpx import AsyncClient

from tests.visit.conftest import (
    VisitWorld,
    guard_null_marker_leftovers,
    null_marker_leftovers,
)
from tests.visit.test_downstream import _completed_visit

pytestmark = pytest.mark.integration

VISIT_TESTS = pathlib.Path(__file__).parent
SCOPED_CALLS = ("restart_lost_triggers", "list_lost_triggers")
PASS_FUNCTIONS = ("reconcile_unsettled", "restart_lost_triggers")


def _call_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _patched_names(function: ast.AST) -> set[str]:
    """String constants passed as the attribute name of a `<x>.setattr(target, "name", ...)`
    call inside `function` (monkeypatch.setattr / patch.object style)."""
    names: set[str] = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Call) and _call_name(node) in ("setattr", "object"):
            for arg in node.args[:3]:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    names.add(arg.value)
    return names


def _enclosing_functions(tree: ast.AST) -> dict[int, list[ast.AST]]:
    """id(node) -> the chain of FunctionDef / AsyncFunctionDef nodes around it."""
    chains: dict[int, list[ast.AST]] = {}

    def visit(node: ast.AST, chain: list[ast.AST]) -> None:
        for child in ast.iter_child_nodes(node):
            inner = chain + [child] if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) else chain
            chains[id(child)] = inner
            visit(child, inner)

    visit(tree, [])
    return chains


def _is_none_constant(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def scan_source(source: str, where: str) -> list[str]:
    """Violations of the isolation rules in one module's source, as `where:line: why`."""
    tree = ast.parse(source)
    chains = _enclosing_functions(tree)
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        chain = chains.get(id(node), [])
        patched: set[str] = set()
        for function in chain:
            patched |= _patched_names(function)
        both_recorded = set(PASS_FUNCTIONS) <= patched

        if name in SCOPED_CALLS and not any(k.arg == "clinic_id" for k in node.keywords):
            found.append(f"{where}:{node.lineno}: {name} without clinic_id")
        elif name == "reconcile" and isinstance(node.func, ast.Attribute) and not both_recorded:
            found.append(
                f"{where}:{node.lineno}: reconcile() without both pass functions replaced by recorders"
            )
        elif name == "VisitActivities" and not both_recorded:
            client: ast.AST | None = node.args[4] if len(node.args) > 4 else None
            for keyword in node.keywords:
                if keyword.arg == "client":
                    client = keyword.value
            if client is not None and not _is_none_constant(client):
                found.append(
                    f"{where}:{node.lineno}: VisitActivities with a Temporal client but "
                    "without both pass functions replaced by recorders"
                )
    return found


def scan_tree(root: pathlib.Path) -> list[str]:
    found: list[str] = []
    for path in sorted(root.rglob("*.py")):
        found.extend(scan_source(path.read_text(), f"{path.relative_to(root.parent)}"))
    return found


def test_no_harness_call_acts_on_visits_outside_its_own_clinic() -> None:
    assert scan_tree(VISIT_TESTS) == []


# --- positive controls: the scan must fail on a synthetic offender of each shape --------------

_UNSCOPED_RESTART = """
async def test_x(factory, client):
    await downstream.restart_lost_triggers(factory, client, grace_minutes=0)
"""
_UNSCOPED_LIST = """
async def test_x(db):
    await visit_repo.list_lost_triggers(db, older_than_minutes=0)
"""
_UNRECORDED_RECONCILE = """
async def test_x(acts):
    await acts.reconcile()
"""
_HALF_RECORDED_RECONCILE = """
async def test_x(acts, monkeypatch):
    monkeypatch.setattr(downstream, "reconcile_unsettled", recorder)
    await acts.reconcile()
"""
_ACTIVITIES_WITH_CLIENT = """
def build(session_factory, http, tokens, mongo, temporal):
    return VisitActivities(session_factory, http, tokens, mongo, temporal)
"""
_COMPLIANT = """
async def test_x(factory, client, acts, monkeypatch, db):
    monkeypatch.setattr(downstream, "reconcile_unsettled", recorder)
    monkeypatch.setattr(downstream, "restart_lost_triggers", recorder)
    await downstream.restart_lost_triggers(factory, client, grace_minutes=0, clinic_id=clinic)
    await visit_repo.list_lost_triggers(db, older_than_minutes=0, clinic_id=clinic)
    await acts.reconcile()
    VisitActivities(factory, http, tokens, mongo, None)

    def build(temporal):
        return VisitActivities(factory, http, tokens, mongo, client=temporal)
"""


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (_UNSCOPED_RESTART, "restart_lost_triggers without clinic_id"),
        (_UNSCOPED_LIST, "list_lost_triggers without clinic_id"),
        (_UNRECORDED_RECONCILE, "reconcile() without both pass functions"),
        (_HALF_RECORDED_RECONCILE, "reconcile() without both pass functions"),
        (_ACTIVITIES_WITH_CLIENT, "VisitActivities with a Temporal client"),
    ],
)
def test_the_scan_fails_on_a_synthetic_offender(source: str, expected: str) -> None:
    found = scan_source(source, "synthetic.py")
    assert len(found) == 1 and expected in found[0], found


def test_the_scan_accepts_scoped_and_recorded_calls() -> None:
    assert scan_source(_COMPLIANT, "synthetic.py") == []


# --- the leftover guard (W6) --------------------------------------------------------------


async def test_the_guard_sees_a_completed_null_marker_visit(
    client: AsyncClient, mesh: httpx.AsyncClient, visit_world: VisitWorld
) -> None:
    """Positive control: such a row IS counted before teardown (the guard can see it). The
    visit_world teardown removes it, so the session guard finds nothing afterwards."""
    assert await null_marker_leftovers(visit_world.provider_ids) == 0
    visit, _, completed = await _completed_visit(client, mesh, visit_world)
    assert completed.status_code == 200, completed.text
    assert await null_marker_leftovers(visit_world.provider_ids) == 1
    assert visit["id"] and await null_marker_leftovers([uuid.uuid4()]) == 0  # another provider's: not counted


async def test_the_guard_cleans_up_and_raises_when_leftovers_exist() -> None:
    calls: list[tuple[str, list[uuid.UUID]]] = []
    ids = [uuid.uuid4(), uuid.uuid4()]

    async def count(provider_ids: list[uuid.UUID]) -> int:
        calls.append(("count", provider_ids))
        return 2

    async def cleanup_outbox(provider_ids: list[uuid.UUID]) -> None:
        calls.append(("outbox", provider_ids))

    async def cleanup_rows(provider_ids: list[uuid.UUID]) -> None:
        calls.append(("rows", provider_ids))

    with pytest.raises(AssertionError, match="2 completed visit"):
        await guard_null_marker_leftovers(
            ids, count=count, cleanup_outbox=cleanup_outbox, cleanup_rows=cleanup_rows
        )

    assert calls == [("count", ids), ("outbox", ids), ("rows", ids)]  # cleaned, in order


async def test_the_guard_is_silent_when_nothing_is_left() -> None:
    cleaned: list[Any] = []

    async def count(provider_ids: list[uuid.UUID]) -> int:
        return 0

    async def cleanup(provider_ids: list[uuid.UUID]) -> None:
        cleaned.append(provider_ids)

    assert (
        await guard_null_marker_leftovers(
            [uuid.uuid4()], count=count, cleanup_outbox=cleanup, cleanup_rows=cleanup
        )
        == 0
    )
    assert cleaned == []
