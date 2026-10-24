"""Password hashing via passlib[bcrypt] (CLAUDE.md-mandated).

Never hand-roll hash+salt logic (RESEARCH "Don't Hand-Roll") — bcrypt's
work-factor tuning and salt handling are solved problems in this library.
Hashed passwords are the only form ever stored/logged; the plaintext
password is never persisted or logged anywhere.

Deviation (Rule 1 - bug, documented in 01-03-SUMMARY.md): passlib 1.7.4's
bcrypt backend runs an internal self-test (`detect_wrap_bug`) on first use
that probes with a 255-byte secret. bcrypt>=4.1 (pinned to 5.0.0 here per
CLAUDE.md) enforces its 72-byte hard limit strictly and raises `ValueError`
instead of the old silent-truncation behavior pybcrypt/bcrypt<4.1 had —
this is a documented upstream passlib/bcrypt version incompatibility with
no passlib-1.7.4-side fix, unrelated to any real user password. We call
the `bcrypt` library directly (still bcrypt, still the CLAUDE.md-mandated
algorithm) rather than routing through passlib's incompatible self-test.
Passwords are truncated to bcrypt's own 72-byte hard limit before hashing —
standard, industry-wide practice (bcrypt has never supported longer
secrets; anything beyond 72 bytes is ignored by the algorithm itself).

C2: bcrypt costs tens of milliseconds of CPU per call, so calling it directly
inside an `async def` route freezes the event loop for every other request.
Routes use `hash_password_async` / `verify_password_async`, which run the sync
functions in a bounded dedicated `ThreadPoolExecutor` (threads named `bcrypt*`,
size `IDENTITY_BCRYPT_THREADS`). bcrypt releases the GIL while hashing, so the
pool gives real parallelism without competing with the default executor, and a
login burst queues in the pool instead of stalling the loop. The sync functions
stay for scripts and unit tests.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import bcrypt

from services.identity.config import settings

_MAX_PASSWORD_BYTES = 72

_executor = ThreadPoolExecutor(max_workers=settings.bcrypt_threads, thread_name_prefix="bcrypt")

# WR-03: a precomputed dummy hash, verified against when no real user
# exists, so a login attempt against a non-existent email pays the SAME
# bcrypt cost as one against a real email with a wrong password — closing
# a timing side-channel that would otherwise let an attacker enumerate
# valid registered emails (a privacy-sensitive fact in a healthcare
# system) purely from response latency. Computed once at import time
# (bcrypt's own gensalt() cost factor governs the cost paid either way).
DUMMY_PASSWORD_HASH = bcrypt.hashpw(b"dummy-password-for-constant-time-login", bcrypt.gensalt()).decode(
    "ascii"
)


def _truncate_to_bcrypt_limit(password: str) -> bytes:
    return password.encode("utf-8")[:_MAX_PASSWORD_BYTES]


def hash_password(password: str) -> str:
    """Hash a plaintext password with bcrypt. Never log the input or output."""
    hashed = bcrypt.hashpw(_truncate_to_bcrypt_limit(password), bcrypt.gensalt())
    return hashed.decode("ascii")


def verify_password(password: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash. `bcrypt.checkpw`
    performs a constant-time comparison internally.
    """
    return bcrypt.checkpw(_truncate_to_bcrypt_limit(password), hashed.encode("ascii"))


async def hash_password_async(password: str) -> str:
    """`hash_password` on the bounded bcrypt pool; the event loop stays free."""
    return await asyncio.get_running_loop().run_in_executor(_executor, hash_password, password)


async def verify_password_async(password: str, hashed: str) -> bool:
    """`verify_password` on the bounded bcrypt pool; the event loop stays free."""
    return await asyncio.get_running_loop().run_in_executor(
        _executor, verify_password, password, hashed
    )
