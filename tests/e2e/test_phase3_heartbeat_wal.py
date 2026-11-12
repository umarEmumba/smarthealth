"""A quiet database's replication slot keeps moving (plan 03-17, WAL retention guard).

Postgres keeps the WAL a logical slot has not confirmed. `provider_db` is nearly idle
while the cluster as a whole writes WAL, so before the heartbeat action query its slot
`dbz_provider` pinned hundreds of MB; the cluster's `max_slot_wal_keep_size=2GB` would
invalidate it and provider events would silently stop.

These checks run against the live slots (read-only superuser queries inside the postgres
container) with no write to any outbox table: after the connectors' heartbeat interval
and one Connect offset flush, every slot's `confirmed_flush_lsn` must have advanced and
each slot must sit within a bounded distance of the current WAL position.

    SMARTHEALTH_E2E_REQUIRE_STACK=1 pytest tests/e2e/test_phase3_heartbeat_wal.py -m e2e
"""

from __future__ import annotations

import json
import time

import pytest

from tests.e2e import phase3_stack as stack

pytestmark = pytest.mark.e2e

MB = 1024 * 1024
# Connect flushes source offsets (and so confirms the LSN) every 60 s by default.
ADVANCE_DEADLINE_SECONDS = 150
MAX_UNCONFIRMED_BYTES = 48 * MB
MAX_RETAINED_BYTES = 96 * MB
SLOTS = {
    "provider-outbox": "dbz_provider",
    "scheduling-outbox": "dbz_scheduling",
    "patient-outbox": "dbz_patient",
    "visit-outbox": "dbz_visit",
}


@pytest.fixture(scope="module", autouse=True)
def _stack() -> None:
    stack.require_phase3_stack()


def _slot(slot: str) -> dict[str, object]:
    """Active flag, confirmed LSN (as a comparable number) and the byte distances."""
    row = stack.psql_superuser(
        "SELECT active, "
        "pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint, "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, "
        "wal_status "
        f"FROM pg_replication_slots WHERE slot_name = '{slot}'"
    )
    assert row, f"replication slot {slot} does not exist"
    active, confirmed, unconfirmed, retained, status = row.split("|")
    return {
        "active": active == "t",
        "confirmed": int(confirmed),
        "unconfirmed": int(unconfirmed),
        "retained": int(retained),
        "status": status,
    }


def test_deployed_connectors_carry_the_heartbeat_action() -> None:
    """The registered config (not only the file) has the heartbeat settings."""
    for name in SLOTS:
        out = stack.compose(
            "exec", "-T", stack.CONNECT_SERVICE, "curl", "-fs",
            f"{stack.CONNECT_LOCAL_URL}/connectors/{name}/config",
        )
        deployed = json.loads(out.stdout)
        expected = stack.connector_config(name)
        for key in ("heartbeat.interval.ms", "heartbeat.action.query", "message.prefix.exclude.list"):
            assert deployed.get(key) == expected[key], f"{name}: deployed {key} differs from the file"


@pytest.mark.parametrize("connector", sorted(SLOTS))
def test_quiet_slot_confirmed_lsn_advances_and_wal_stays_bounded(connector: str) -> None:
    """With no outbox write in the window, the slot still confirms newer WAL."""
    slot = SLOTS[connector]
    stack.wait_connectors_running([connector], timeout=60)
    before = _slot(slot)
    assert before["active"], f"{slot} is not active"

    deadline = time.monotonic() + ADVANCE_DEADLINE_SECONDS
    latest = before
    while time.monotonic() < deadline:
        latest = _slot(slot)
        if latest["confirmed"] > before["confirmed"]:
            break
        time.sleep(5)
    assert latest["confirmed"] > before["confirmed"], (
        f"{slot}: confirmed_flush_lsn did not advance in {ADVANCE_DEADLINE_SECONDS}s "
        f"(the heartbeat is not moving a quiet slot)"
    )

    # Settled position: unconfirmed distance is a few offset-flush intervals of cluster
    # WAL, never hundreds of MB; the slot is healthy and not on its way to invalidation.
    settle_deadline = time.monotonic() + ADVANCE_DEADLINE_SECONDS
    while time.monotonic() < settle_deadline:
        latest = _slot(slot)
        if latest["unconfirmed"] <= MAX_UNCONFIRMED_BYTES and latest["retained"] <= MAX_RETAINED_BYTES:
            break
        time.sleep(5)
    assert latest["unconfirmed"] <= MAX_UNCONFIRMED_BYTES, (
        f"{slot}: {latest['unconfirmed'] // MB} MB of WAL unconfirmed"
    )
    assert latest["retained"] <= MAX_RETAINED_BYTES, (
        f"{slot}: {latest['retained'] // MB} MB of WAL retained"
    )
    assert latest["status"] in ("reserved", "extended"), f"{slot}: wal_status {latest['status']}"
