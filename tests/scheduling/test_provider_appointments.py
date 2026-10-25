"""A provider reads their own appointments (plan 02.1-12, C4).

`GET /api/appointments` and `/{id}` now admit the PROVIDER role. Ownership is the
provider profile linked to the caller's login (Provider's by-user lookup), never
a client-supplied id: another provider's appointment is the same 404 as a
missing one and never appears in a list. The write routes still refuse the role,
and every other role's reads are exactly what they were.

Appointments are seeded with plain SQL on fresh slots (`confirmed_appointment`)
for provider ids of the test's own worlds, so nothing the live reconcilers or
the other suites own is touched; rows are removed by provider id at the end.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text

from tests.conftest import degraded_mesh, unique_email
from tests.scheduling.seed import PASSWORD, auth
from tests.visit.conftest import VisitWorld, _visit_world, add_provider, confirmed_appointment

pytestmark = pytest.mark.integration

NOT_FOUND = {"detail": "not found"}
COVERAGE_REF = "COV-SYNTHETIC-REF-12345"
PATIENT_NAME = "Zzyzx Seed Synthetic"


@dataclass
class Providers:
    """Two providers of one clinic (A is the world's own) and one of another clinic."""

    vw: VisitWorld
    other: VisitWorld
    a_id: str
    a_tokens: dict
    b_id: str
    b_tokens: dict
    c_id: str
    c_tokens: dict


@pytest_asyncio.fixture
async def world(client: AsyncClient, mesh: httpx.AsyncClient) -> AsyncIterator[VisitWorld]:
    async for vw in _visit_world(client, mesh):
        yield vw


@pytest_asyncio.fixture
async def foreign_world(client: AsyncClient, mesh: httpx.AsyncClient) -> AsyncIterator[VisitWorld]:
    async for vw in _visit_world(client, mesh):
        yield vw


@pytest_asyncio.fixture
async def providers(
    client: AsyncClient, world: VisitWorld, foreign_world: VisitWorld
) -> Providers:
    b_id, b_tokens = await add_provider(client, world)
    return Providers(
        vw=world,
        other=foreign_world,
        a_id=str(world.world.provider_id),
        a_tokens=world.world.provider,
        b_id=b_id,
        b_tokens=b_tokens,
        c_id=str(foreign_world.world.provider_id),
        c_tokens=foreign_world.world.provider,
    )


async def _list(client: AsyncClient, tokens: dict, **params) -> httpx.Response:
    return await client.get("/api/appointments", params=params, headers=auth(tokens))


async def _detail(client: AsyncClient, tokens: dict, appointment_id: str) -> httpx.Response:
    return await client.get(f"/api/appointments/{appointment_id}", headers=auth(tokens))


async def _set_coverage(vw: VisitWorld, appointment_id: str) -> None:
    async with vw.engine.begin() as conn:
        await conn.execute(
            text("UPDATE appointments SET coverage_ref = :c WHERE id = :a"),
            {"c": COVERAGE_REF, "a": uuid.UUID(appointment_id)},
        )


# --- the provider's own list ---------------------------------------------------------


async def test_provider_lists_only_their_own_appointments(
    client: AsyncClient, providers: Providers
) -> None:
    own_one = await confirmed_appointment(providers.vw)
    own_two = await confirmed_appointment(providers.vw, status="completed")
    other_provider = await confirmed_appointment(providers.vw, provider_id=uuid.UUID(providers.b_id))
    foreign = await confirmed_appointment(providers.other)

    resp = await _list(client, providers.a_tokens)

    assert resp.status_code == 200, resp.text
    ids = {row["id"] for row in resp.json()}
    assert ids == {own_one, own_two}
    assert {row["provider_id"] for row in resp.json()} == {providers.a_id}
    assert other_provider not in ids and foreign not in ids

    second = await _list(client, providers.b_tokens)
    assert {row["id"] for row in second.json()} == {other_provider}


async def test_provider_list_filters_work(client: AsyncClient, providers: Providers) -> None:
    confirmed = await confirmed_appointment(providers.vw)
    completed = await confirmed_appointment(providers.vw, status="completed")
    later = await confirmed_appointment(providers.vw)

    by_status = await _list(client, providers.a_tokens, status="completed")
    assert [row["id"] for row in by_status.json()] == [completed]

    rows = (await _list(client, providers.a_tokens)).json()
    assert len(rows) == 3
    starts = {row["id"]: row["slot_start"] for row in rows}
    assert starts[confirmed] < starts[completed] < starts[later]

    after_first = await _list(client, providers.a_tokens, start=starts[completed])
    assert {row["id"] for row in after_first.json()} == {completed, later}
    before_last = await _list(client, providers.a_tokens, end=starts[later])
    assert {row["id"] for row in before_last.json()} == {confirmed, completed}
    window = await _list(client, providers.a_tokens, start=starts[completed], end=starts[later])
    assert [row["id"] for row in window.json()] == [completed]

    bad = await _list(client, providers.a_tokens, status="nonsense")
    assert bad.status_code == 422


async def test_requested_provider_id_that_is_not_their_own_returns_nothing(
    client: AsyncClient, providers: Providers
) -> None:
    own = await confirmed_appointment(providers.vw)
    theirs = await confirmed_appointment(providers.vw, provider_id=uuid.UUID(providers.b_id))

    other = await _list(client, providers.a_tokens, provider_id=providers.b_id)
    assert other.status_code == 200
    assert other.json() == []

    mine = await _list(client, providers.a_tokens, provider_id=providers.a_id)
    assert [row["id"] for row in mine.json()] == [own]
    assert theirs not in {row["id"] for row in mine.json()}


async def test_a_clinic_id_filter_cannot_widen_or_narrow_the_provider_view(
    client: AsyncClient, providers: Providers
) -> None:
    own = await confirmed_appointment(providers.vw)

    foreign_clinic = await _list(
        client, providers.a_tokens, clinic_id=str(providers.other.clinic_id)
    )
    own_clinic = await _list(client, providers.a_tokens, clinic_id=str(providers.vw.clinic_id))

    # A provider is scoped by their provider id; the clinic filter can only
    # narrow, so a clinic that is not theirs matches none of their rows.
    assert foreign_clinic.json() == []
    assert [row["id"] for row in own_clinic.json()] == [own]


# --- detail --------------------------------------------------------------------------


async def test_provider_reads_their_own_appointment_with_history(
    client: AsyncClient, providers: Providers
) -> None:
    appointment_id = await confirmed_appointment(providers.vw)
    async with providers.vw.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO appointment_status_history "
                "(appointment_id, from_status, to_status, reason, actor, created_at) VALUES "
                "(:a, NULL, 'pending', NULL, 'user:x', now() - interval '2 minutes'), "
                "(:a, 'pending', 'confirmed', NULL, 'workflow:x', now() - interval '1 minute')"
            ),
            {"a": uuid.UUID(appointment_id)},
        )

    resp = await _detail(client, providers.a_tokens, appointment_id)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == appointment_id
    assert body["provider_id"] == providers.a_id
    assert body["status"] == "confirmed"
    assert [(h["from_status"], h["to_status"]) for h in body["history"]] == [
        (None, "pending"),
        ("pending", "confirmed"),
    ]


async def test_another_providers_appointment_is_the_identical_404(
    client: AsyncClient, providers: Providers
) -> None:
    same_clinic = await confirmed_appointment(providers.vw, provider_id=uuid.UUID(providers.b_id))
    other_clinic = await confirmed_appointment(providers.other)
    missing = str(uuid.uuid4())

    answers = [
        await _detail(client, providers.a_tokens, appointment_id)
        for appointment_id in (same_clinic, other_clinic, missing)
    ]

    assert [a.status_code for a in answers] == [404, 404, 404]
    assert all(a.json() == NOT_FOUND for a in answers)
    # ... and the owner still sees their own.
    assert (await _detail(client, providers.b_tokens, same_clinic)).status_code == 200


async def test_provider_without_a_provider_profile_sees_nothing(
    client: AsyncClient, providers: Providers
) -> None:
    appointment_id = await confirmed_appointment(providers.vw)
    email = unique_email("provider")
    created = await client.post(
        "/api/auth/admin/users",
        json={
            "email": email,
            "password": PASSWORD,
            "role": "provider",
            "clinic_id": str(providers.vw.clinic_id),
        },
        headers=auth(providers.vw.world.admin),
    )
    assert created.status_code == 201, created.text
    login = await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert login.status_code == 200, login.text
    unlinked = login.json()

    listing = await _list(client, unlinked)
    detail = await _detail(client, unlinked, appointment_id)

    assert listing.status_code == 200 and listing.json() == []
    assert detail.status_code == 404 and detail.json() == NOT_FOUND


async def test_provider_login_without_a_clinic_claim_still_reads_their_own(
    client: AsyncClient, providers: Providers
) -> None:
    """Provider reads are scoped by the provider id, not the token's clinic claim."""
    world = providers.vw.world
    email = unique_email("provider")
    created = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": PASSWORD, "role": "provider"},
        headers=auth(world.admin),
    )
    assert created.status_code == 201, created.text
    profile = await client.post(
        "/api/providers",
        json={
            "department_id": str(world.department_id),
            "full_name": "Dr. Unscoped Synthetic",
            "specialties": ["General"],
            "user_id": created.json()["id"],
        },
        headers=auth(world.admin),
    )
    assert profile.status_code == 201, profile.text
    providers.vw.provider_ids.append(uuid.UUID(profile.json()["id"]))
    tokens = (
        await client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    ).json()
    own = await confirmed_appointment(providers.vw, provider_id=uuid.UUID(profile.json()["id"]))
    someone_elses = await confirmed_appointment(providers.vw)

    listing = await _list(client, tokens)
    detail = await _detail(client, tokens, own)
    foreign = await _detail(client, tokens, someone_elses)

    assert [row["id"] for row in listing.json()] == [own]
    assert detail.status_code == 200
    assert foreign.status_code == 404 and foreign.json() == NOT_FOUND


# --- the flagged worklist fields -------------------------------------------------------


async def test_flagged_filter_and_fields_work_for_the_providers_own_appointments(
    client: AsyncClient, providers: Providers
) -> None:
    flagged = await confirmed_appointment(providers.vw)
    plain = await confirmed_appointment(providers.vw)
    flagged_foreign = await confirmed_appointment(providers.vw, provider_id=uuid.UUID(providers.b_id))
    async with providers.vw.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE appointments SET schedule_change_flagged_at = now(), "
                "schedule_change_reason = 'template_updated' WHERE id = ANY(:ids)"
            ),
            {"ids": [uuid.UUID(flagged), uuid.UUID(flagged_foreign)]},
        )

    only_flagged = await _list(client, providers.a_tokens, flagged="true", status="confirmed")
    unflagged = await _list(client, providers.a_tokens, flagged="false")
    detail = await _detail(client, providers.a_tokens, flagged)
    plain_detail = await _detail(client, providers.a_tokens, plain)

    assert [row["id"] for row in only_flagged.json()] == [flagged]
    assert only_flagged.json()[0]["schedule_change_flagged"] is True
    assert only_flagged.json()[0]["schedule_change_reason"] == "template_updated"
    assert [row["id"] for row in unflagged.json()] == [plain]
    assert detail.json()["schedule_change_flagged"] is True
    assert plain_detail.json()["schedule_change_flagged"] is False
    assert (await _detail(client, providers.a_tokens, flagged_foreign)).status_code == 404


# --- what is never returned or logged ------------------------------------------------------


async def test_responses_never_carry_the_coverage_reference(
    client: AsyncClient, providers: Providers
) -> None:
    appointment_id = await confirmed_appointment(providers.vw)
    await _set_coverage(providers.vw, appointment_id)

    listing = await _list(client, providers.a_tokens)
    detail = await _detail(client, providers.a_tokens, appointment_id)

    assert listing.status_code == detail.status_code == 200
    for response in (listing, detail):
        assert COVERAGE_REF not in response.text
        assert "coverage" not in response.text


async def test_provider_reads_log_no_phi(
    client: AsyncClient, providers: Providers, capsys: pytest.CaptureFixture[str]
) -> None:
    appointment_id = await confirmed_appointment(providers.vw)
    await _set_coverage(providers.vw, appointment_id)
    capsys.readouterr()

    assert (await _list(client, providers.a_tokens)).status_code == 200
    assert (await _detail(client, providers.a_tokens, appointment_id)).status_code == 200
    assert (await _detail(client, providers.a_tokens, str(uuid.uuid4()))).status_code == 404
    output = capsys.readouterr().out

    assert output  # the request was logged
    for value in (COVERAGE_REF, PATIENT_NAME, "Zzyzx", "1990-01-01"):
        assert value not in output


# --- writes are still refused --------------------------------------------------------------


async def test_provider_cannot_book_cancel_or_reschedule(
    client: AsyncClient, providers: Providers
) -> None:
    appointment_id = await confirmed_appointment(providers.vw)
    slot_id = str(uuid.uuid4())

    booked = await client.post(
        "/api/appointments",
        json={"slot_id": slot_id, "patient_id": str(providers.vw.world.patient_id)},
        headers={**auth(providers.a_tokens), "Idempotency-Key": f"prov-{uuid.uuid4().hex}"},
    )
    cancelled = await client.post(
        f"/api/appointments/{appointment_id}/cancel", json={}, headers=auth(providers.a_tokens)
    )
    rescheduled = await client.post(
        f"/api/appointments/{appointment_id}/reschedule",
        json={"new_slot_id": slot_id},
        headers=auth(providers.a_tokens),
    )

    assert booked.status_code == 403, booked.text
    assert cancelled.status_code == 403, cancelled.text
    assert rescheduled.status_code == 403, rescheduled.text
    # the appointment is untouched
    still = await _detail(client, providers.a_tokens, appointment_id)
    assert still.json()["status"] == "confirmed" and still.json()["version"] == 1


async def test_no_token_is_401(client: AsyncClient, providers: Providers) -> None:
    appointment_id = await confirmed_appointment(providers.vw)

    assert (await client.get("/api/appointments")).status_code == 401
    assert (await client.get(f"/api/appointments/{appointment_id}")).status_code == 401


# --- an unreachable Provider fails closed ------------------------------------------------------


async def test_provider_service_down_is_503_for_provider_reads(
    client: AsyncClient, providers: Providers
) -> None:
    appointment_id = await confirmed_appointment(providers.vw)

    async with degraded_mesh("provider"):
        listing = await _list(client, providers.a_tokens)
        detail = await _detail(client, providers.a_tokens, appointment_id)

    assert listing.status_code == 503, listing.text
    assert detail.status_code == 503, detail.text
    # recovered
    assert (await _detail(client, providers.a_tokens, appointment_id)).status_code == 200


async def test_provider_service_down_does_not_affect_staff_reads(
    client: AsyncClient, providers: Providers
) -> None:
    appointment_id = await confirmed_appointment(providers.vw)
    world = providers.vw.world

    async with degraded_mesh("provider"):
        front_desk = await _detail(client, world.front_desk, appointment_id)
        admin = await _list(client, world.admin, provider_id=providers.a_id)

    assert front_desk.status_code == 200
    assert admin.status_code == 200


# --- every other role is unchanged ----------------------------------------------------------------


async def test_other_roles_keep_exactly_their_previous_reads(
    client: AsyncClient, providers: Providers
) -> None:
    world = providers.vw.world
    mine = await confirmed_appointment(providers.vw)
    colleague = await confirmed_appointment(providers.vw, provider_id=uuid.UUID(providers.b_id))
    foreign = await confirmed_appointment(providers.other)

    # Front desk: their whole clinic, never another clinic.
    desk_rows = await _list(client, world.front_desk, provider_id=providers.a_id)
    assert [row["id"] for row in desk_rows.json()] == [mine]
    desk_b = await _list(client, world.front_desk, provider_id=providers.b_id)
    assert [row["id"] for row in desk_b.json()] == [colleague]
    assert (await _detail(client, world.front_desk, colleague)).status_code == 200
    assert (await _detail(client, world.front_desk, foreign)).status_code == 404
    assert (await _list(client, world.front_desk, provider_id=providers.c_id)).json() == []

    # Admin: anywhere.
    for appointment_id in (mine, colleague, foreign):
        assert (await _detail(client, world.admin, appointment_id)).status_code == 200

    # Patient: only their own appointments (the seeded patient owns all three
    # rows of this clinic; the foreign one belongs to another clinic's patient).
    patient_rows = await _list(client, world.patient, provider_id=providers.a_id)
    assert [row["id"] for row in patient_rows.json()] == [mine]
    assert (await _detail(client, world.patient, mine)).status_code == 200
    assert (await _detail(client, world.patient, foreign)).status_code == 404


# --- history ordering (the tie 02.1-11 left behind) -------------------------------------------------


async def test_history_orders_a_transition_before_the_note_written_with_it(
    client: AsyncClient, providers: Providers
) -> None:
    """Rows written in one transaction share created_at; a note must follow the
    transition it was written with, whatever their random ids are."""
    appointment_id = await confirmed_appointment(providers.vw, status="failed")
    transition_id = uuid.UUID("ffffffff-0000-4000-8000-000000000001")
    note_id = uuid.UUID("00000000-0000-4000-8000-000000000001")  # sorts BEFORE the transition's id
    async with providers.vw.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO appointment_status_history "
                "(id, appointment_id, from_status, to_status, reason, actor, created_at) VALUES "
                "(:note, :a, 'failed', 'failed', 'billing_void_checked', 'system:reconciler', now()), "
                "(:transition, :a, 'pending', 'failed', 'workflow_lost', 'system:reconciler', now())"
            ),
            {"a": uuid.UUID(appointment_id), "note": note_id, "transition": transition_id},
        )

    for token in (providers.a_tokens, providers.vw.world.front_desk):
        resp = await _detail(client, token, appointment_id)
        assert resp.status_code == 200, resp.text
        assert [(h["to_status"], h["reason"]) for h in resp.json()["history"]] == [
            ("failed", "workflow_lost"),
            ("failed", "billing_void_checked"),
        ]


async def test_history_keeps_creation_time_order_across_transactions(
    client: AsyncClient, providers: Providers
) -> None:
    appointment_id = await confirmed_appointment(providers.vw)
    async with providers.vw.engine.begin() as conn:
        base = timedelta(minutes=3)
        for index, (from_status, to_status, reason) in enumerate(
            [(None, "pending", None), ("pending", "confirmed", None), ("confirmed", "confirmed", "note")]
        ):
            await conn.execute(
                text(
                    "INSERT INTO appointment_status_history "
                    "(appointment_id, from_status, to_status, reason, actor, created_at) "
                    "VALUES (:a, :f, :t, :r, 'x', now() - :ago * interval '1 second')"
                ),
                {
                    "a": uuid.UUID(appointment_id),
                    "f": from_status,
                    "t": to_status,
                    "r": reason,
                    "ago": int(base.total_seconds()) - 30 * index,
                },
            )

    resp = await _detail(client, providers.a_tokens, appointment_id)

    assert [(h["from_status"], h["to_status"], h["reason"]) for h in resp.json()["history"]] == [
        (None, "pending", None),
        ("pending", "confirmed", None),
        ("confirmed", "confirmed", "note"),
    ]
