"""One-shot, idempotent first-admin bootstrap (WR-07).

Why this exists: CR-01 removed the ONLY code path that could create an
`admin`-role user without already being an authenticated admin
(`RegisterRequest` has no `role` field; `POST /api/auth/admin/users` is
itself gated by `Depends(require_role(Role.ADMIN))`). That closes the
privilege-escalation hole, but it also means a fresh deployment has no way
to create its first admin account through the application — a
bootstrapping deadlock. This script is the operational escape hatch,
deliberately NOT an HTTP route: it never runs inside the request/response
cycle, so it cannot be reached by an unauthenticated (or any) network
caller, and re-introduces no CR-01-shaped risk.

Idempotency contract:
    - Safe to re-run on every deploy (e.g. as a post-deploy hook/Job).
    - If ANY `admin`-role user already exists, this is a no-op — it never
      creates a second admin and never overwrites an existing admin's
      password or any other field.
    - If no admin exists yet, it creates exactly one, using the SAME
      bcrypt hashing path (`libs.auth.passwords.hash_password`) the
      application's own register/login routes use, and writes an
      `audit_log` entry (`action="user.bootstrap"`) so the very first
      admin's creation is itself part of the audit trail (this codebase's
      documented principle: mutations are always audited).

Usage (see README.md "First-Admin Bootstrap" section):

    BOOTSTRAP_ADMIN_EMAIL=admin@example.com \\
    BOOTSTRAP_ADMIN_PASSWORD='a-strong-password' \\
    python scripts/bootstrap_admin.py

Exit codes: 0 on success (whether an admin was created or one already
existed), 1 on misconfiguration (missing env vars) or failure.
"""

from __future__ import annotations

import asyncio
import os
import sys

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# Repo root on sys.path so this script works whether invoked as
# `python scripts/bootstrap_admin.py` from the repo root (the documented
# usage) or via an absolute path — mirrors how pytest resolves `smarthealth`/
# `services`/`libs` as top-level packages (see pyproject.toml's
# `[tool.setuptools] packages`).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from libs.audit.writer import write_audit  # noqa: E402
from libs.observability.logging import get_logger  # noqa: E402
from smarthealth.config import settings  # noqa: E402
from smarthealth.db import get_mongo_client  # noqa: E402

log = get_logger(__name__)


class BootstrapConfigError(RuntimeError):
    """Raised when required BOOTSTRAP_ADMIN_* environment variables are missing."""


async def bootstrap_admin(*, email: str, password: str) -> str:
    """Create the first admin user if (and only if) no admin exists yet.

    Returns a human-readable status string: "created" or "already_exists".
    Never creates a second admin and never overwrites an existing user —
    the existence check and the insert both go through the same
    short-lived engine/session so a re-run always sees the previous run's
    committed state.
    """
    from libs.auth.passwords import hash_password  # noqa: PLC0415
    from services.identity.repository.models import User  # noqa: PLC0415
    from services.identity.domain.enums import Role  # noqa: PLC0415

    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    try:
        async with session_factory() as session:
            # `.first()` (not `.scalar_one_or_none()`): this is strictly an
            # existence check — "does at least one admin already exist?" —
            # so it must tolerate more than one pre-existing admin row
            # (e.g. one created via the app's own `/api/auth/admin/users`
            # route after an earlier bootstrap run) without raising
            # `MultipleResultsFound`. Any match is grounds to no-op.
            existing_admin = await session.execute(
                select(User).where(User.role == Role.ADMIN.value).limit(1)
            )
            if existing_admin.scalars().first() is not None:
                log.info("bootstrap_admin.already_exists")
                return "already_exists"

            # Also guard the specific email against a non-admin collision
            # (e.g. a patient self-registered with this email first) —
            # surfacing a clear failure is safer than silently creating a
            # second row that violates the `users.email` unique constraint.
            existing_email = await session.execute(select(User).where(User.email == email))
            if existing_email.scalar_one_or_none() is not None:
                raise RuntimeError(
                    f"BOOTSTRAP_ADMIN_EMAIL {email!r} is already registered to a "
                    "non-admin user; refusing to bootstrap. Choose a different "
                    "BOOTSTRAP_ADMIN_EMAIL or promote the existing user manually."
                )

            user = User(
                email=email,
                hashed_password=hash_password(password),
                role=Role.ADMIN.value,
                clinic_id=None,
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)

            mongo = get_mongo_client()
            await write_audit(
                mongo,
                actor_id="system",
                actor_role="system",
                action="user.bootstrap",
                entity_type="user",
                entity_id=str(user.id),
                clinic_id=None,
                outcome="success",
            )
            log.info("bootstrap_admin.created", user_id=str(user.id))
            return "created"
    finally:
        await engine.dispose()


def _read_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise BootstrapConfigError(
            f"{name} is required to run scripts/bootstrap_admin.py (see README.md "
            "'First-Admin Bootstrap' section)."
        )
    return value


async def _main_async() -> int:
    try:
        email = _read_required_env("BOOTSTRAP_ADMIN_EMAIL")
        password = _read_required_env("BOOTSTRAP_ADMIN_PASSWORD")
    except BootstrapConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    try:
        status = await bootstrap_admin(email=email, password=password)
    except Exception as exc:  # noqa: BLE001 - top-level CLI error boundary
        print(f"ERROR: bootstrap_admin failed: {exc}", file=sys.stderr)
        return 1

    if status == "created":
        print(f"Admin user created for {email}.")
    else:
        print("An admin user already exists — no changes made (idempotent no-op).")
    return 0


def main() -> int:
    return asyncio.run(_main_async())


if __name__ == "__main__":
    raise SystemExit(main())
