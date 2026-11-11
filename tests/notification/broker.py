"""Throwaway RabbitMQ vhost and user for the notification tests (plan 03-02).

`throwaway_broker()` creates vhost and user `p3test-<hex>` with a random
password through `rabbitmqctl` inside the live compose container (fixed argv
lists, no shell) and removes both on exit. Nothing else on the broker is touched.
"""

from __future__ import annotations

import secrets
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "infra" / "docker-compose.yml")]


@dataclass(frozen=True)
class Broker:
    vhost: str
    user: str
    password: str
    host: str = "localhost"
    port: int = 5672

    @property
    def url(self) -> str:
        return f"amqp://{quote(self.user, safe='')}:{quote(self.password, safe='')}@{self.host}:{self.port}/{quote(self.vhost, safe='')}"


def rabbitmqctl(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [*COMPOSE, "exec", "-T", "rabbitmq", "rabbitmqctl", *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=check,
        cwd=REPO_ROOT,
    )


@contextmanager
def throwaway_broker() -> Iterator[Broker]:
    name = f"p3test-{secrets.token_hex(4)}"
    broker = Broker(vhost=name, user=name, password=secrets.token_urlsafe(18))
    try:
        rabbitmqctl("add_vhost", broker.vhost)
        rabbitmqctl("add_user", broker.user, broker.password)
        rabbitmqctl("set_permissions", "-p", broker.vhost, broker.user, ".*", ".*", ".*")
        yield broker
    finally:
        # Retried: a worker that is still shutting down can make the first delete fail,
        # and a leaked vhost would otherwise stay on the live broker.
        for _ in range(3):
            gone = rabbitmqctl("delete_vhost", broker.vhost, check=False).returncode == 0
            if gone:
                break
            time.sleep(2)
        rabbitmqctl("delete_user", broker.user, check=False)
