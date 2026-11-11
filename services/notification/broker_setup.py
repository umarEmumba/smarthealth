"""Idempotent RabbitMQ provisioning for the notification service (plan 03-14, EVENT-04).

`python -m services.notification.broker_setup` is the Compose one-shot `rabbitmq-init`.
It is the ONLY place that holds the broker's administrator credentials. Through the
management HTTP API it creates

* the dedicated virtual host `smarthealth`,
* the dedicated broker user `smarthealth_notification` with NO tags (so it cannot use
  the management plugin and is not an administrator),
* permissions for that user on that vhost only (`.*` configure/write/read: the Celery
  worker declares its own queues and the dead-letter topology at start).

Every call is a PUT, so running it again changes nothing. The user's password comes
from the file named by `NOTIFICATION_BROKER_PASSWORD_FILE` (a Compose secret), the
administrator's from `RABBITMQ_ADMIN_USER` / `RABBITMQ_ADMIN_PASSWORD`. No password is
ever printed or put in an exception message; a failed call reports the HTTP status only.
"""

from __future__ import annotations

import os
import sys
import time
from urllib.parse import quote

import httpx

from services.notification.config import BrokerPasswordError, settings

__all__ = ["BrokerSetupError", "ensure_broker_user", "main"]

REQUEST_TIMEOUT_SECONDS = 10.0
CONNECT_ATTEMPTS = 15
CONNECT_DELAY_SECONDS = 2.0


class BrokerSetupError(RuntimeError):
    """A provisioning call failed (the message carries the status code, never a secret)."""


def _put(client: httpx.Client, path: str, body: dict[str, object] | None) -> None:
    response = client.put(path, json=body)
    if response.status_code not in (200, 201, 204):
        raise BrokerSetupError(f"RabbitMQ management API returned HTTP {response.status_code} for PUT {path.split('/')[2]}")


def ensure_broker_user(client: httpx.Client, *, vhost: str, user: str, password: str) -> None:
    """Create (or leave in place) the vhost, the tag-less user and its vhost permissions."""
    vhost_path = quote(vhost, safe="")
    user_path = quote(user, safe="")
    _put(client, f"/api/vhosts/{vhost_path}", None)
    # `tags: ""` makes the user a plain AMQP user: no management access, no administrator.
    _put(client, f"/api/users/{user_path}", {"password": password, "tags": ""})
    _put(
        client,
        f"/api/permissions/{vhost_path}/{user_path}",
        {"configure": ".*", "write": ".*", "read": ".*"},
    )


def main() -> int:
    admin_user = os.environ.get("RABBITMQ_ADMIN_USER", "")
    admin_password = os.environ.get("RABBITMQ_ADMIN_PASSWORD", "")
    if not admin_user or not admin_password:
        print("broker setup failed: RABBITMQ_ADMIN_USER and RABBITMQ_ADMIN_PASSWORD must be set", file=sys.stderr)
        return 1
    try:
        password = settings.read_broker_password().get_secret_value()
    except BrokerPasswordError as exc:
        print(f"broker setup failed: {exc}", file=sys.stderr)
        return 1

    try:
        for attempt in range(1, CONNECT_ATTEMPTS + 1):
            try:
                with httpx.Client(
                    base_url=settings.rabbitmq_management_url,
                    auth=(admin_user, admin_password),
                    timeout=REQUEST_TIMEOUT_SECONDS,
                ) as client:
                    ensure_broker_user(
                        client, vhost=settings.broker_vhost, user=settings.broker_user, password=password
                    )
                break
            except httpx.TransportError:
                # The management listener can come up a moment after the broker answers a ping.
                if attempt == CONNECT_ATTEMPTS:
                    raise
                time.sleep(CONNECT_DELAY_SECONDS)
    except BrokerSetupError as exc:
        print(f"broker setup failed: {exc}", file=sys.stderr)
        return 1
    except httpx.HTTPError as exc:
        print(f"broker setup failed: management API unreachable ({type(exc).__name__})", file=sys.stderr)
        return 1
    print("broker user ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
