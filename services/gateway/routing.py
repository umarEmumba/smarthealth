"""Gateway route table and path-safety checks.

The route table maps a PUBLIC path prefix to the service that owns it. It is
the whole of the gateway's routing knowledge, and it deliberately contains NO
`/internal` prefix: internal reference-lookup endpoints are genuinely absent
from the route map, not "routed but blocked" (RESEARCH anti-pattern; STRIDE
T-01.1-11). Scheduling's public prefixes (plan 02-04) are `/api/slots`,
`/api/appointments` and `/api/waitlist`; like every other service, its
`/internal` routes are never in this table.

`is_safe_path` is the first gate every request passes (STRIDE T-01.1-12). The
gateway forwards the RAW request path, so anything that could be re-interpreted
by an upstream's own routing (dot segments, encoded dots/slashes/backslashes,
NUL, empty segments) is refused here rather than normalised: a traversal
attempt such as `/api/clinics/%2e%2e/%2e%2e/internal/departments/x` must never
reach a service that might decode it into an `/internal` path.
"""

from __future__ import annotations

from libs.config import SharedSettings

# (public path prefix, owning service). Longest/most specific prefixes first is
# unnecessary today (no prefix is a prefix of another), but matching is always
# on a path-SEGMENT boundary — see `resolve_upstream`.
ROUTE_TABLE: tuple[tuple[str, str], ...] = (
    ("/.well-known/jwks.json", "identity"),
    ("/api/auth", "identity"),
    ("/api/admin", "identity"),
    ("/api/clinics", "organization"),
    ("/api/providers", "provider"),
    ("/api/patients", "patient"),
    ("/api/slots", "scheduling"),
    ("/api/appointments", "scheduling"),
    ("/api/waitlist", "scheduling"),
)

_FORBIDDEN_RAW_SEQUENCES: tuple[bytes, ...] = (b"%2e", b"%2f", b"%5c", b"%00")


def resolve_upstream(path: str, settings: SharedSettings) -> tuple[str, str] | None:
    """(upstream_name, base_url) for `path`, or None when no route matches.

    A prefix matches only when `path == prefix` or `path` continues with a
    `/` right after it, so `/api/clinicsX` does NOT route to Organization.
    """
    for prefix, upstream_name in ROUTE_TABLE:
        if path == prefix or path.startswith(prefix + "/"):
            return upstream_name, getattr(settings, f"{upstream_name}_service_url")
    return None


def is_safe_path(path: str, raw_path: bytes) -> bool:
    """False when the request path could be reinterpreted by an upstream.

    `path` is the percent-DECODED path, `raw_path` the bytes as received.
    Refuses: a `.` or `..` segment, `//` (empty segment), a backslash or NUL in
    the decoded path, and any encoded dot / slash / backslash / NUL in the raw
    path (case-insensitive).
    """
    if "\x00" in path or "\\" in path or "//" in path:
        return False
    if any(segment in (".", "..") for segment in path.split("/")):
        return False
    lowered = raw_path.lower()
    return not any(sequence in lowered for sequence in _FORBIDDEN_RAW_SEQUENCES)
