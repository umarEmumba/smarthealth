"""GET /api/admin/ping — the minimal RBAC probe route.

Not full admin CRUD (that's Plan 04's scope) — this route exists solely to
prove `require_role(Role.ADMIN)` enforces 403/200 at the HTTP boundary
end-to-end (IAM-02).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from services.identity.domain.enums import Role
from services.identity.security.current_user import AuthenticatedUser
from services.identity.security.rbac import require_role

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.get("/ping")
async def admin_ping(
    user: AuthenticatedUser = Depends(require_role(Role.ADMIN)),
) -> dict[str, str]:
    return {"status": "ok", "admin_sub": user.sub}
