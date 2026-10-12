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


settings = GatewaySettings()
