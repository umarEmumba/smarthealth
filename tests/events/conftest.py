"""Fixtures for the event library tests (plan 03-01).

`mock_registry` points the registry client at an in-memory `mock://` registry
and registers every catalogue entry, so event types added by later plans are
covered without changing this fixture.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient

from libs.config import shared_settings
from libs.events.registry import (
    get_registry_client,
    register_all,
    reset_registry_for_tests,
)


@pytest.fixture
def mock_registry(monkeypatch: pytest.MonkeyPatch) -> Iterator[SchemaRegistryClient]:
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "mock://p3-tests")
    # The settings singleton is built at import time, so the env var alone
    # would not reach it; patch the attribute the registry module reads.
    monkeypatch.setattr(shared_settings, "schema_registry_url", "mock://p3-tests")
    reset_registry_for_tests()
    client = get_registry_client()
    register_all(client)
    yield client
    reset_registry_for_tests()
