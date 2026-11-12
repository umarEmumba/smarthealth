"""GET /.well-known/jwks.json — Identity's PUBLIC signing keys.

Unauthenticated by design (RFC 7517 JWKS discovery): every other service
fetches this to verify tokens locally. It serves public members only — the
private key never leaves `services.identity.security.keys`.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response

from services.identity.security.keys import jwks_document

router = APIRouter(tags=["jwks"])


@router.get("/.well-known/jwks.json")
async def jwks(response: Response) -> dict[str, Any]:
    response.headers["Cache-Control"] = "public, max-age=300"
    return jwks_document()
