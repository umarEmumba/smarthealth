"""identity_0002 (unique index on lower(email)) is safe over existing data (plan 02.1-02, C3).

Runs alembic as a subprocess against the live identity_db: downgrade to
identity_0001, plant two rows that differ only in case, and assert the upgrade
refuses with the NUMBER of duplicate groups and no email address; remove the
rows, assert the upgrade succeeds, and the downgrade drops the index. The test
always finishes by upgrading to head so a failed assertion never leaves the
index missing for the rest of the suite.
"""

from __future__ import annotations

import re
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.identity.config import settings as identity_settings
from services.identity.security.passwords import hash_password

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = "services/identity/migrations/alembic.ini"
INDEX = "uq_users_email_lower"


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-c", ALEMBIC_INI, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _output(proc: subprocess.CompletedProcess[str]) -> str:
    return f"{proc.stdout}\n{proc.stderr}"


async def _index_exists(factory: async_sessionmaker) -> bool:
    async with factory() as session:
        row = await session.execute(
            text("SELECT 1 FROM pg_indexes WHERE tablename = 'users' AND indexname = :name"),
            {"name": INDEX},
        )
        return row.first() is not None


async def test_upgrade_refuses_over_case_insensitive_duplicates_without_leaking_emails() -> None:
    token = uuid.uuid4().hex[:12]
    lower_email = f"migdup-{token}@example.com"
    upper_email = lower_email.upper()
    engine = create_async_engine(identity_settings.database_url)
    factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    hashed = hash_password("correct-horse-battery")

    try:
        down = _alembic("downgrade", "identity_0001")
        assert down.returncode == 0, _output(down)
        assert not await _index_exists(factory)

        async with factory() as session:
            for email in (lower_email, upper_email):
                await session.execute(
                    text(
                        "INSERT INTO users (email, hashed_password, role) "
                        "VALUES (:email, :hashed, 'patient')"
                    ),
                    {"email": email, "hashed": hashed},
                )
            await session.commit()

        refused = _alembic("upgrade", "head")
        out = _output(refused)
        assert refused.returncode != 0, out
        match = re.search(r"(\d+) group\(s\)", out)
        assert match is not None, out
        assert int(match.group(1)) >= 1
        assert "@" not in out, "migration output must not contain an email address"
        assert token not in out
        assert not await _index_exists(factory)

        async with factory() as session:
            await session.execute(
                text("DELETE FROM users WHERE lower(email) = :email"), {"email": lower_email}
            )
            await session.commit()

        upgraded = _alembic("upgrade", "head")
        assert upgraded.returncode == 0, _output(upgraded)
        assert await _index_exists(factory)

        # The index now rejects a case-variant of an existing address at the database level.
        async with factory() as session:
            await session.execute(
                text(
                    "INSERT INTO users (email, hashed_password, role) "
                    "VALUES (:email, :hashed, 'patient')"
                ),
                {"email": lower_email, "hashed": hashed},
            )
            await session.commit()
        from sqlalchemy.exc import IntegrityError  # noqa: PLC0415

        with pytest.raises(IntegrityError):
            async with factory() as session:
                await session.execute(
                    text(
                        "INSERT INTO users (email, hashed_password, role) "
                        "VALUES (:email, :hashed, 'patient')"
                    ),
                    {"email": upper_email, "hashed": hashed},
                )
                await session.commit()
    finally:
        try:
            async with factory() as session:
                await session.execute(
                    text("DELETE FROM users WHERE lower(email) = :email"), {"email": lower_email}
                )
                await session.commit()
        finally:
            restored = _alembic("upgrade", "head")
            await engine.dispose()
            assert restored.returncode == 0, _output(restored)


async def test_downgrade_drops_the_index_and_upgrade_restores_it() -> None:
    engine = create_async_engine(identity_settings.database_url)
    factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    try:
        assert _alembic("upgrade", "head").returncode == 0
        assert await _index_exists(factory)

        down = _alembic("downgrade", "identity_0001")
        assert down.returncode == 0, _output(down)
        assert not await _index_exists(factory)
    finally:
        restored = _alembic("upgrade", "head")
        await engine.dispose()
        assert restored.returncode == 0, _output(restored)
    check = _alembic("check")
    assert check.returncode == 0, _output(check)
