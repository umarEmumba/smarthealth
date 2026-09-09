"""`require_role(*roles)` — server-side RBAC dependency guard (IAM-02).

Raises 403 when the authenticated user's role is not in the allowed set.
This is a FastAPI dependency layered on top of `get_current_user` (which
itself raises 401 for missing/invalid/revoked tokens) — RBAC is enforced
here, server-side, on every route that declares it; never client-side and
never merely "documented" at router registration without an actual runtime
guard.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import Depends, HTTPException, status

from services.identity.domain.enums import Role
from services.identity.security.current_user import AuthenticatedUser, get_current_user


def require_role(*allowed: Role) -> Callable[..., AuthenticatedUser]:
    def _guard(
        user: AuthenticatedUser = Depends(get_current_user),
    ) -> AuthenticatedUser:
        if user.role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="insufficient role for this operation",
            )
        return user

    return _guard
