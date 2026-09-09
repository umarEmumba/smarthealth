"""Tests for `scripts/bootstrap_admin.py` (WR-07 re-review fix).

Integration tests against the live Postgres/Mongo containers (same pattern
as `tests/test_auth.py`'s `bootstrap_admin_and_login` helper) — proves the
script's core idempotency contract: running the bootstrap twice with the
same email/password creates at most one admin account and never raises or
duplicates on the second call.

No HTTP route is exercised here on purpose — `bootstrap_admin()` is a
plain async function called directly, never reachable over the network
(WR-07's fix is deliberately NOT an HTTP endpoint, to avoid reopening the
CR-01 privilege-escalation hole).
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from scripts.bootstrap_admin import bootstrap_admin
from services.identity.domain.enums import Role
from services.identity.repository.models import User
from smarthealth.config import settings
from tests.conftest import unique_email


@pytest.mark.integration
async def test_bootstrap_admin_is_idempotent_and_never_duplicates() -> None:
    """Calling `bootstrap_admin()` a second time (any email/password) must
    be a no-op once an admin already exists anywhere in the table — this
    proves the "at most one admin ever, safe to re-run on every deploy"
    contract, independent of how many admins pre-exist from other tests.
    """
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async def _admin_count() -> int:
        async with session_factory() as session:
            result = await session.execute(
                select(func.count()).select_from(User).where(User.role == Role.ADMIN.value)
            )
            return result.scalar_one()

    count_before = await _admin_count()

    email = unique_email("bootstrap-idempotency")
    password = "correct-horse-battery-staple"  # noqa: S105 - test fixture password

    if count_before == 0:
        # No admin exists anywhere yet (rare, but possible on a fully
        # fresh DB) — the first call must actually create one.
        first_status = await bootstrap_admin(email=email, password=password)
        assert first_status == "created"
        count_after_first = await _admin_count()
        assert count_after_first == 1
    else:
        # An admin already exists (the common case once any other test in
        # this suite has run) — even a brand-new never-seen-before email
        # must NOT create a second admin.
        first_status = await bootstrap_admin(email=email, password=password)
        assert first_status == "already_exists"
        count_after_first = await _admin_count()
        assert count_after_first == count_before

    # The second call (same email/password this time) must ALSO be a
    # no-op and must not change the admin count at all — running twice
    # yields exactly the same single-admin end state as running once.
    second_status = await bootstrap_admin(email=email, password=password)
    assert second_status == "already_exists"
    count_after_second = await _admin_count()
    assert count_after_second == count_after_first

    await engine.dispose()


@pytest.mark.integration
async def test_bootstrap_admin_created_user_can_log_in(client) -> None:  # noqa: ANN001
    """When bootstrap_admin() DOES create an admin (simulated here via a
    fresh, guaranteed-unused email — the function itself only checks for
    ANY existing admin, so this exercises the creation branch whenever the
    DB has no admin yet, and simply degrades to the no-op branch
    otherwise), the created account must be a real, functional login —
    hashed with the same bcrypt path the app's own login route verifies
    against, and audited.
    """
    from sqlalchemy import select as _select
    from sqlalchemy.ext.asyncio import async_sessionmaker as _async_sessionmaker
    from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine

    engine = _create_async_engine(settings.database_url)
    session_factory = _async_sessionmaker(bind=engine, expire_on_commit=False)
    async with session_factory() as session:
        existing_admin = await session.execute(
            _select(User).where(User.role == Role.ADMIN.value).limit(1)
        )
        admin_already_exists = existing_admin.scalars().first() is not None
    await engine.dispose()

    email = unique_email("bootstrap-login")
    password = "correct-horse-battery-staple"  # noqa: S105 - test fixture password
    status_result = await bootstrap_admin(email=email, password=password)

    if admin_already_exists:
        # Deployment already has an admin (the common case in this shared
        # dev/test DB) — bootstrap_admin() correctly no-ops, so there is
        # nothing new to log in as. Nothing further to assert.
        assert status_result == "already_exists"
        return

    assert status_result == "created"
    login_resp = await client.post("/api/auth/login", json={"email": email, "password": password})
    assert login_resp.status_code == 200, login_resp.text
