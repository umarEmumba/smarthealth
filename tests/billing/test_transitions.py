"""Billing state-targeted transitions and re-link (BILL-02; D-06, D-13, D-14, D-16).

Every call names the TARGET state, so a retried activity is a no-op: already in
the target, or a key already recorded for the record -> 200 with no new
history; a move the state machine forbids -> 409; an unknown appointment -> 404.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
from httpx import AsyncClient
from pymongo import AsyncMongoClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.config import shared_settings
from tests.billing.conftest import (
    BillingWorld,
    auth,
    new_key,
    precheck,
    relink,
    service_token,
    transition,
)

pytestmark = pytest.mark.integration


async def _verified(mesh: httpx.AsyncClient, world: BillingWorld, coverage_ref: str | None = None) -> str:
    token = await service_token(mesh, "scheduling")
    resp = await precheck(mesh, token, world.clinic_id, coverage_ref=coverage_ref)
    assert resp.status_code == 201, resp.text
    return resp.json()["appointment_id"]


async def _read(client: AsyncClient, world: BillingWorld, appointment_id: str) -> dict:
    resp = await client.get(
        "/api/billing/records", params={"appointment_id": appointment_id}, headers=auth(world.admin)
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _moves(body: dict) -> list[tuple]:
    return [(h["from_status"], h["to_status"], h["reason_code"]) for h in body["history"]]


async def _audit_count(dedupe_key: str) -> int:
    mongo: AsyncMongoClient = AsyncMongoClient(shared_settings.mongo_url)
    try:
        collection = mongo[shared_settings.mongo_db_name]["audit_log"]
        return await collection.count_documents({"dedupe_key": dedupe_key})
    finally:
        await mongo.close()


async def test_verified_to_voided_is_a_state_targeted_compensation(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")
    key = new_key()

    resp = await transition(mesh, token, appointment_id, "voided", key=key, reason_code="booking_compensated")

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "voided"
    assert resp.json()["reason_code"] == "booking_compensated"
    assert _moves(resp.json())[-1] == ("verified", "voided", "booking_compensated")
    assert await _audit_count(f"billing:{appointment_id}:{key}") == 1


async def test_failed_to_voided(mesh: httpx.AsyncClient, billing_world: BillingWorld) -> None:
    appointment_id = await _verified(mesh, billing_world, coverage_ref="DENY-1")
    token = await service_token(mesh, "scheduling")

    resp = await transition(mesh, token, appointment_id, "voided")

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "voided"
    # The original denial reason is kept when the void names none.
    assert resp.json()["reason_code"] == "coverage_denied"
    assert _moves(resp.json())[-1] == ("failed", "voided", None)


async def test_pending_to_voided(mesh: httpx.AsyncClient, billing_world: BillingWorld) -> None:
    """`pending` is never left behind by the API; seed one directly to prove the
    state machine still lets a stray pending record be voided."""
    from services.billing.config import settings as billing_settings  # noqa: PLC0415

    appointment_id = str(uuid.uuid4())
    engine = create_async_engine(billing_settings.database_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO billing_records (appointment_id, clinic_id, status, coverage_kind) "
                    "VALUES (:a, :c, 'pending', 'self_pay')"
                ),
                {"a": appointment_id, "c": billing_world.clinic_id},
            )
    finally:
        await engine.dispose()
    token = await service_token(mesh, "scheduling")

    resp = await transition(mesh, token, appointment_id, "voided")

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "voided"
    assert _moves(resp.json()) == [("pending", "voided", None)]


async def test_cancel_before_cutoff_goes_refund_pending_then_refunded(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")

    pending = await transition(mesh, token, appointment_id, "refund_pending", reason_code="cancelled")
    refunded = await transition(mesh, token, appointment_id, "refunded", reason_code="cancelled")

    assert pending.status_code == 200 and pending.json()["status"] == "refund_pending"
    assert refunded.status_code == 200, refunded.text
    assert refunded.json()["status"] == "refunded"
    assert [m[:2] for m in _moves(refunded.json())] == [
        (None, "pending"),
        ("pending", "verified"),
        ("verified", "refund_pending"),
        ("refund_pending", "refunded"),
    ]


async def test_cancel_inside_cutoff_is_no_refund(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")

    resp = await transition(mesh, token, appointment_id, "no_refund", reason_code="inside_cutoff")

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "no_refund"
    assert _moves(resp.json())[-1] == ("verified", "no_refund", "inside_cutoff")


async def test_visit_token_finalizes_after_completion(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "visit")

    resp = await transition(mesh, token, appointment_id, "finalized", reason_code="visit_completed")

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "finalized"


async def test_visit_token_may_only_finalize(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    visit = await service_token(mesh, "visit")

    refund = await transition(mesh, visit, appointment_id, "refund_pending")
    relinked = await relink(mesh, visit, appointment_id)

    assert refund.status_code == 403, refund.text
    assert relinked.status_code == 403, relinked.text


async def test_same_target_is_a_noop_without_new_history(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")
    first = await transition(mesh, token, appointment_id, "voided")

    again = await transition(mesh, token, appointment_id, "voided")  # new key, same target

    assert again.status_code == 200, again.text
    assert again.json()["status"] == "voided"
    assert again.json()["history"] == first.json()["history"]


async def test_repeated_idempotency_key_is_a_noop(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")
    key = new_key()
    first = await transition(mesh, token, appointment_id, "refund_pending", key=key)
    assert first.status_code == 200, first.text

    # A retry carrying the same key changes nothing, even naming another target.
    replay = await transition(mesh, token, appointment_id, "refund_pending", key=key)
    other = await transition(mesh, token, appointment_id, "refunded", key=key)

    assert replay.status_code == 200 and other.status_code == 200
    assert other.json()["status"] == "refund_pending"
    assert other.json()["history"] == first.json()["history"]
    assert await _audit_count(f"billing:{appointment_id}:{key}") == 1


@pytest.mark.parametrize(
    ("path", "target"),
    [
        (("refund_pending", "refunded"), "verified"),
        (("voided",), "refunded"),
        (("finalized",), "refund_pending"),
        (("no_refund",), "finalized"),
        ((), "pending"),
    ],
)
async def test_disallowed_transitions_are_409(
    client: AsyncClient,
    mesh: httpx.AsyncClient,
    billing_world: BillingWorld,
    path: tuple[str, ...],
    target: str,
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    scheduling = await service_token(mesh, "scheduling")
    visit = await service_token(mesh, "visit")

    def token_for(step: str) -> str:
        return visit if step == "finalized" else scheduling

    for step in path:
        assert (await transition(mesh, token_for(step), appointment_id, step)).status_code == 200
    before = await _read(client, billing_world, appointment_id)

    resp = await transition(mesh, token_for(target), appointment_id, target)

    assert resp.status_code == 409, resp.text
    # Nothing changed.
    after = await _read(client, billing_world, appointment_id)
    assert after == before


async def test_unknown_appointment_is_404(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    token = await service_token(mesh, "scheduling")
    missing = str(uuid.uuid4())

    assert (await transition(mesh, token, missing, "voided")).status_code == 404
    assert (await relink(mesh, token, missing)).status_code == 404


async def test_unknown_target_is_422(mesh: httpx.AsyncClient, billing_world: BillingWorld) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")
    assert (await transition(mesh, token, appointment_id, "settled")).status_code == 422


async def test_concurrent_same_target_requests_apply_once(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")

    responses = await asyncio.gather(
        *(transition(mesh, token, appointment_id, "voided") for _ in range(5))
    )

    assert all(r.status_code == 200 for r in responses), [r.text for r in responses]
    final = responses[-1].json()
    assert final["status"] == "voided"
    assert [m[1] for m in _moves(final)].count("voided") == 1


async def test_relink_appends_history_without_changing_state(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")
    key = new_key()

    first = await relink(mesh, token, appointment_id, key=key)
    again = await relink(mesh, token, appointment_id, key=key)

    assert first.status_code == 200, first.text
    assert first.json()["status"] == "verified"
    assert _moves(first.json())[-1] == ("verified", "verified", "rescheduled")
    assert again.status_code == 200
    assert again.json()["history"] == first.json()["history"]
    # No refund state was ever entered.
    assert not {"refund_pending", "refunded", "no_refund"} & {m[1] for m in _moves(again.json())}
    assert await _audit_count(f"billing:{appointment_id}:{key}") == 1


async def test_relink_of_a_closed_record_is_409(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    appointment_id = await _verified(mesh, billing_world)
    token = await service_token(mesh, "scheduling")
    assert (await transition(mesh, token, appointment_id, "voided")).status_code == 200

    assert (await relink(mesh, token, appointment_id)).status_code == 409
