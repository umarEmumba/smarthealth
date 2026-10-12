"""Gateway settings: the shared infra settings (which already carry every
upstream's internal base URL) plus the per-request upstream timeout.

The gateway owns no database and holds no secret: it does not verify JWTs
(every service verifies for itself — defence in depth), so there is no key
or signing configuration here either.
"""

from __future__ import annotations

from libs.config import SharedSettings


class GatewaySettings(SharedSettings):
    # Upper bound on one proxied upstream call. A slow or hung upstream yields
    # a 504 at the edge instead of tying the gateway worker up indefinitely.
    gateway_upstream_timeout_seconds: float = 10.0

    # Per-client-IP fixed-window budgets (requests per minute). Credential
    # endpoints (login/register/refresh) share the stricter "auth" budget.
    # Env: GATEWAY_RATE_LIMIT_DEFAULT_PER_MINUTE / GATEWAY_RATE_LIMIT_AUTH_PER_MINUTE.
    gateway_rate_limit_default_per_minute: int = 300
    gateway_rate_limit_auth_per_minute: int = 20


settings = GatewaySettings()
