"""bcrypt runs off the event loop, in a bounded dedicated pool (plan 02.1-02, C2).

Proofs: the async wrappers match the sync functions and run bcrypt in a thread
named "bcrypt*" (never the loop thread); eight concurrent logins through the
gateway do not stall a 5 ms ticker beyond a bound; and the three auth routes
call only the `*_async` functions (AST check).
"""

from __future__ import annotations

import ast
import asyncio
import threading
import time
from pathlib import Path
from unittest.mock import patch

import bcrypt
import pytest
from httpx import AsyncClient

from services.identity.config import settings as identity_settings
from services.identity.security import passwords
from services.identity.security.passwords import (
    DUMMY_PASSWORD_HASH,
    hash_password,
    hash_password_async,
    verify_password,
    verify_password_async,
)
from tests.conftest import unique_email

PASSWORD = "correct-horse-battery"
AUTH_SOURCE = Path(__file__).resolve().parents[2] / "services" / "identity" / "api" / "auth.py"
SYNC_NAMES = {"hash_password", "verify_password"}
ROUTES_USING_BCRYPT = {"register", "create_staff_user", "login"}


async def test_async_wrappers_match_the_sync_functions() -> None:
    hashed = await hash_password_async(PASSWORD)
    assert verify_password(PASSWORD, hashed)
    assert await verify_password_async(PASSWORD, hashed) is True
    assert await verify_password_async("wrong-password", hashed) is False
    assert await verify_password_async(PASSWORD, hash_password(PASSWORD)) is True


async def test_72_byte_truncation_is_preserved_by_the_async_path() -> None:
    long_password = "a" * 72 + "tail-that-bcrypt-ignores"
    hashed = await hash_password_async(long_password)
    assert await verify_password_async("a" * 72, hashed) is True


async def test_bcrypt_runs_in_a_bcrypt_named_thread_not_the_loop_thread() -> None:
    loop_thread = threading.get_ident()
    seen: list[tuple[int, str]] = []
    real_hashpw, real_checkpw = bcrypt.hashpw, bcrypt.checkpw

    def spy_hashpw(*args, **kwargs):
        seen.append((threading.get_ident(), threading.current_thread().name))
        return real_hashpw(*args, **kwargs)

    def spy_checkpw(*args, **kwargs):
        seen.append((threading.get_ident(), threading.current_thread().name))
        return real_checkpw(*args, **kwargs)

    with patch.object(bcrypt, "hashpw", spy_hashpw), patch.object(bcrypt, "checkpw", spy_checkpw):
        hashed = await hash_password_async(PASSWORD)
        assert await verify_password_async(PASSWORD, hashed)

    assert len(seen) == 2
    for ident, name in seen:
        assert ident != loop_thread
        assert name.startswith("bcrypt"), name


def test_pool_is_bounded_by_the_setting() -> None:
    assert identity_settings.bcrypt_threads >= 1
    assert passwords._executor._max_workers == identity_settings.bcrypt_threads


@pytest.mark.integration
async def test_concurrent_logins_do_not_stall_the_event_loop(client: AsyncClient) -> None:
    email = unique_email("loopLag")
    reg = await client.post("/api/auth/register", json={"email": email, "password": PASSWORD})
    assert reg.status_code == 201, reg.text

    # Warm-up burst: the first concurrent requests open Postgres/Mongo connections
    # (driver-side auth and pool setup), which is not what this test measures.
    warmup = await asyncio.gather(
        *[client.post("/api/auth/login", json={"email": email, "password": PASSWORD}) for _ in range(8)]
    )
    assert all(r.status_code == 200 for r in warmup)

    gaps: list[float] = []
    stop = asyncio.Event()

    async def ticker() -> None:
        last = time.perf_counter()
        while not stop.is_set():
            await asyncio.sleep(0.005)
            now = time.perf_counter()
            gaps.append(now - last)
            last = now

    tick_task = asyncio.create_task(ticker())
    await asyncio.sleep(0.05)
    try:
        responses = await asyncio.gather(
            *[
                client.post("/api/auth/login", json={"email": email, "password": PASSWORD})
                for _ in range(8)
            ]
        )
    finally:
        stop.set()
        await tick_task

    assert all(r.status_code == 200 for r in responses), [r.status_code for r in responses]
    assert len(gaps) > 5
    worst_lag = max(gaps) - 0.005
    assert worst_lag < 0.100, f"event loop stalled {worst_lag * 1000:.0f} ms during concurrent logins"


def _route_calls(route_name: str) -> set[str]:
    tree = ast.parse(AUTH_SOURCE.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == route_name:
            return {
                call.func.id
                for call in ast.walk(node)
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
            }
    raise AssertionError(f"route {route_name} not found in auth.py")


@pytest.mark.parametrize("route_name", sorted(ROUTES_USING_BCRYPT))
def test_routes_call_only_the_async_bcrypt_functions(route_name: str) -> None:
    calls = _route_calls(route_name)
    assert not (calls & SYNC_NAMES), f"{route_name} calls sync bcrypt: {calls & SYNC_NAMES}"
    assert calls & {"hash_password_async", "verify_password_async"}, route_name


def test_auth_module_does_not_import_the_sync_functions() -> None:
    tree = ast.parse(AUTH_SOURCE.read_text())
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported |= {alias.name for alias in node.names}
    assert not (imported & SYNC_NAMES), imported & SYNC_NAMES
    assert DUMMY_PASSWORD_HASH  # the constant-time login hash is still defined
