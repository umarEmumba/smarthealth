"""The gateway's /metrics is reachable only on the internal metrics listener.

The gateway's public port is the single published application port, so a
Prometheus exposition there would leak route names and traffic volumes to any
client (threat flag `public-metrics`, plans 01.1-03/04/08). The app decides by
the local socket a request arrived on (ASGI `server` scope), never by a header
a client could set.
"""

from __future__ import annotations

import httpx
import pytest

from services.gateway.app import create_app
from services.gateway.config import GatewaySettings


@pytest.fixture
def gateway_app():
    return create_app(GatewaySettings(_env_file=None, gateway_metrics_port=9100))


async def _get(app, url: str, **kwargs) -> httpx.Response:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as client:
        return await client.get(url, **kwargs)


async def test_metrics_served_on_the_internal_listener(gateway_app) -> None:
    response = await _get(gateway_app, "http://gateway:9100/metrics")
    assert response.status_code == 200
    assert "# HELP" in response.text


async def test_metrics_is_404_on_the_public_listener(gateway_app) -> None:
    for url in ("http://gateway/metrics", "http://gateway:8000/metrics"):
        response = await _get(gateway_app, url)
        assert response.status_code == 404, url
        assert "# HELP" not in response.text


async def test_metrics_cannot_be_unlocked_by_a_client_header(gateway_app) -> None:
    """A spoofed Host / forwarding header on the public listener changes nothing."""
    response = await _get(
        gateway_app,
        "http://gateway:8000/metrics",
        headers={"host": "gateway:9100", "x-forwarded-port": "9100", "x-forwarded-for": "10.0.0.1"},
    )
    assert response.status_code == 404


async def test_metrics_port_is_configurable() -> None:
    app = create_app(GatewaySettings(_env_file=None, gateway_metrics_port=9200))
    assert (await _get(app, "http://gateway:9200/metrics")).status_code == 200
    assert (await _get(app, "http://gateway:9100/metrics")).status_code == 404


async def test_health_stays_on_both_listeners(gateway_app) -> None:
    for url in ("http://gateway:8000/health", "http://gateway:9100/health"):
        assert (await _get(gateway_app, url)).status_code == 200
