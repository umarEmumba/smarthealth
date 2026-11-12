"""Billing pre-check (BILL-01, D-10): deterministic, single-transaction,
idempotent, readable by clinic staff through the gateway.
"""

from __future__ import annotations

import httpx
import pytest
from httpx import AsyncClient

from tests.billing.conftest import BillingWorld, auth, precheck, service_token

pytestmark = pytest.mark.integration


async def test_tracer_scheduling_precheck_verifies_and_front_desk_reads_it_via_gateway(
    client: AsyncClient, mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    token = await service_token(mesh, "scheduling")

    created = await precheck(mesh, token, billing_world.clinic_id, coverage_ref=None)

    assert created.status_code == 201, created.text
    body = created.json()
    assert body["status"] == "verified"
    assert body["coverage_kind"] == "self_pay"
    appointment_id = body["appointment_id"]

    read = await client.get(
        "/api/billing/records",
        params={"appointment_id": appointment_id},
        headers=auth(billing_world.front_desk),
    )

    assert read.status_code == 200, read.text
    record = read.json()
    assert record["status"] == "verified"
    assert record["clinic_id"] == billing_world.clinic_id
    assert [(h["from_status"], h["to_status"]) for h in record["history"]] == [
        (None, "pending"),
        ("pending", "verified"),
    ]
    # No keys, actors or coverage reference ever leave Billing.
    assert set(record) == {
        "id",
        "appointment_id",
        "clinic_id",
        "status",
        "coverage_kind",
        "reason_code",
        "created_at",
        "updated_at",
        "history",
    }
    assert set(record["history"][0]) == {"from_status", "to_status", "reason_code", "created_at"}

    by_id = await client.get(
        f"/api/billing/records/{record['id']}", headers=auth(billing_world.front_desk)
    )
    assert by_id.status_code == 200, by_id.text
    assert by_id.json() == record


# --- deterministic rule set (D-10) -------------------------------------------


@pytest.mark.parametrize(
    ("coverage_ref", "expected"),
    [
        (None, ("verified", "self_pay", None)),
        ("", ("verified", "self_pay", None)),
        ("SELF-PAY", ("verified", "self_pay", None)),
        ("self-pay", ("verified", "self_pay", None)),
        ("ACME-9", ("verified", "insured", None)),
        ("DENY-123", ("failed", "insured", "coverage_denied")),
        ("deny-123", ("verified", "insured", None)),  # prefix match is case-sensitive
    ],
)
def test_evaluate_precheck_is_deterministic(coverage_ref: str | None, expected: tuple) -> None:
    from services.billing.domain.rules import evaluate_precheck  # noqa: PLC0415

    decision = evaluate_precheck(coverage_ref, "DENY-")
    assert (decision.status, decision.coverage_kind, decision.reason_code) == expected
    # The decision has no field that could carry the raw reference.
    assert set(type(decision).__slots__) == {"status", "coverage_kind", "reason_code"}


def test_fail_prefix_setting_is_bound_to_its_alias() -> None:
    from services.billing.config import BillingSettings  # noqa: PLC0415

    assert BillingSettings(_env_file=None).precheck_fail_prefix == "DENY-"
    assert BillingSettings(_env_file=None, BILLING_PRECHECK_FAIL_PREFIX="ACME-").precheck_fail_prefix == "ACME-"


@pytest.mark.parametrize(
    ("coverage_ref", "status", "kind", "reason"),
    [
        ("DENY-123", "failed", "insured", "coverage_denied"),
        ("SELF-PAY", "verified", "self_pay", None),
        ("ACME-9", "verified", "insured", None),
    ],
)
async def test_precheck_outcome_follows_the_rule_set(
    mesh: httpx.AsyncClient,
    billing_world: BillingWorld,
    coverage_ref: str,
    status: str,
    kind: str,
    reason: str | None,
) -> None:
    token = await service_token(mesh, "scheduling")

    resp = await precheck(mesh, token, billing_world.clinic_id, coverage_ref=coverage_ref)

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert (body["status"], body["coverage_kind"], body["reason_code"]) == (status, kind, reason)
    assert [(h["from_status"], h["to_status"]) for h in body["history"]] == [
        (None, "pending"),
        ("pending", status),
    ]
    # The raw reference is never echoed back.
    assert coverage_ref not in resp.text


async def test_fail_prefix_override_changes_the_outcome(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    from services.billing.api.internal import get_billing_settings  # noqa: PLC0415
    from services.billing.config import BillingSettings  # noqa: PLC0415
    from tests.conftest import harness_apps  # noqa: PLC0415

    custom = BillingSettings(_env_file=None, BILLING_PRECHECK_FAIL_PREFIX="ACME-")
    app = harness_apps()["billing"]
    app.dependency_overrides[get_billing_settings] = lambda: custom
    try:
        token = await service_token(mesh, "scheduling")
        acme = await precheck(mesh, token, billing_world.clinic_id, coverage_ref="ACME-9")
        deny = await precheck(mesh, token, billing_world.clinic_id, coverage_ref="DENY-1")
    finally:
        app.dependency_overrides.pop(get_billing_settings, None)

    assert acme.status_code == 201 and acme.json()["status"] == "failed"
    assert acme.json()["reason_code"] == "coverage_denied"
    assert deny.status_code == 201 and deny.json()["status"] == "verified"


# --- idempotency ----------------------------------------------------------------


async def test_repeated_precheck_returns_the_same_record_without_reevaluating(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    token = await service_token(mesh, "scheduling")
    first = await precheck(mesh, token, billing_world.clinic_id, coverage_ref="DENY-7")
    assert first.status_code == 201, first.text
    appointment_id = first.json()["appointment_id"]

    # Different key AND a reference that would now verify: the first outcome stands.
    again = await precheck(
        mesh, token, billing_world.clinic_id, appointment_id=appointment_id, coverage_ref="SELF-PAY"
    )

    assert again.status_code == 200, again.text
    assert again.json()["id"] == first.json()["id"]
    assert again.json()["status"] == "failed"
    assert len(again.json()["history"]) == 2


async def test_concurrent_duplicate_prechecks_create_one_record(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    import asyncio  # noqa: PLC0415
    import uuid  # noqa: PLC0415

    token = await service_token(mesh, "scheduling")
    appointment_id = str(uuid.uuid4())

    responses = await asyncio.gather(
        *(
            precheck(mesh, token, billing_world.clinic_id, appointment_id=appointment_id)
            for _ in range(5)
        )
    )

    codes = sorted(r.status_code for r in responses)
    assert codes == [200, 200, 200, 200, 201], [r.text for r in responses]
    assert len({r.json()["id"] for r in responses}) == 1
    assert all(len(r.json()["history"]) == 2 for r in responses)


async def test_precheck_for_another_clinic_on_an_existing_record_is_409(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    token = await service_token(mesh, "scheduling")
    first = await precheck(mesh, token, billing_world.clinic_id)
    clash = await precheck(
        mesh, token, billing_world.other_clinic_id, appointment_id=first.json()["appointment_id"]
    )
    assert clash.status_code == 409, clash.text


# --- authentication / authorization -------------------------------------------


async def test_visit_token_cannot_run_a_precheck(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    token = await service_token(mesh, "visit")
    resp = await precheck(mesh, token, billing_world.clinic_id)
    assert resp.status_code == 403, resp.text


async def test_precheck_requires_an_idempotency_key_and_a_billing_token(
    mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    import uuid  # noqa: PLC0415

    from tests.billing.conftest import bearer, billing_url  # noqa: PLC0415

    body = {"appointment_id": str(uuid.uuid4()), "clinic_id": billing_world.clinic_id}
    token = await service_token(mesh, "scheduling")
    no_key = await mesh.post(billing_url("/internal/billing/prechecks"), json=body, headers=bearer(token))
    assert no_key.status_code == 422

    wrong_aud = await service_token(mesh, "scheduling", audience="patient")
    resp = await precheck(mesh, wrong_aud, billing_world.clinic_id)
    assert resp.status_code == 401

    anonymous = await mesh.post(
        billing_url("/internal/billing/prechecks"), json=body, headers={"Idempotency-Key": "k"}
    )
    assert anonymous.status_code == 401

    front_desk = await precheck(mesh, billing_world.front_desk["access_token"], billing_world.clinic_id)
    assert front_desk.status_code == 403


async def test_reads_are_clinic_scoped_staff_only_and_internal_routes_are_not_routed(
    client: AsyncClient, mesh: httpx.AsyncClient, billing_world: BillingWorld
) -> None:
    import uuid  # noqa: PLC0415

    token = await service_token(mesh, "scheduling")
    created = (await precheck(mesh, token, billing_world.clinic_id)).json()

    other = await client.get(
        "/api/billing/records",
        params={"appointment_id": created["appointment_id"]},
        headers=auth(billing_world.other_front_desk),
    )
    missing = await client.get(
        "/api/billing/records",
        params={"appointment_id": str(uuid.uuid4())},
        headers=auth(billing_world.front_desk),
    )
    other_by_id = await client.get(
        f"/api/billing/records/{created['id']}", headers=auth(billing_world.other_front_desk)
    )
    assert other.status_code == missing.status_code == other_by_id.status_code == 404
    assert other.json() == missing.json() == other_by_id.json() == {"detail": "not found"}

    admin = await client.get(
        "/api/billing/records",
        params={"appointment_id": created["appointment_id"]},
        headers=auth(billing_world.admin),
    )
    assert admin.status_code == 200

    email = f"billing-patient-{uuid.uuid4().hex[:8]}@example.com"
    patient = await client.post(
        "/api/auth/register", json={"email": email, "password": "correct-horse-battery"}
    )
    assert patient.status_code == 201
    login = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    denied = await client.get(
        "/api/billing/records",
        params={"appointment_id": created["appointment_id"]},
        headers=auth(login.json()),
    )
    assert denied.status_code == 403

    through_gateway = await client.post(
        "/internal/billing/prechecks",
        json={"appointment_id": str(uuid.uuid4()), "clinic_id": billing_world.clinic_id},
        headers={"Authorization": f"Bearer {token}", "Idempotency-Key": "k"},
    )
    assert through_gateway.status_code == 404
