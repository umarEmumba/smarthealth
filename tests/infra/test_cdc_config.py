"""Static checks of the CDC configuration (plan 03-05); no running stack needed.

The connector checks are parametrized over every JSON file in
`infra/connect/connectors/`, so the patient, provider and visit connectors that
plans 03-08 and 03-09 add are validated without editing this file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "infra" / "docker-compose.yml"
CONNECTORS_DIR = REPO_ROOT / "infra" / "connect" / "connectors"
CONNECTOR_FILES = sorted(CONNECTORS_DIR.glob("*.json"))

CONNECT_IMAGE = "quay.io/debezium/connect:3.7.1.Final"
CP_TAG = "7.6.13"

REQUIRED_CONNECTOR_VALUES = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.user": "debezium",
    "plugin.name": "pgoutput",
    "slot.drop.on.stop": "false",
    "publication.name": "dbz_outbox",
    "publication.autocreate.mode": "disabled",
    "table.include.list": "public.outbox",
    "snapshot.mode": "initial",
    "snapshot.locking.mode": "none",
    "tombstones.on.delete": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.debezium.converters.BinaryDataConverter",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.id": "id",
    "transforms.outbox.table.field.event.key": "aggregateid",
    "transforms.outbox.table.field.event.payload": "payload",
    "transforms.outbox.route.by.field": "aggregatetype",
    "transforms.outbox.route.topic.replacement": "smarthealth.${routedByValue}.events",
    "transforms.outbox.table.fields.additional.placement": (
        "type:header:eventType,traceparent:header:traceparent,schema_version:header:schemaVersion"
    ),
    "heartbeat.interval.ms": "10000",
    # A heartbeat alone does not move a quiet database's slot: the action query writes a
    # logical-decoding message INTO the monitored database every interval, so the slot has
    # something to confirm and the cluster's other databases cannot pin its WAL (the
    # 2GB max_slot_wal_keep_size would otherwise invalidate it and silently stop events).
    "heartbeat.action.query": (
        "SELECT pg_logical_emit_message(true, 'smarthealth-heartbeat', clock_timestamp()::text)"
    ),
    # The message is only a WAL position carrier: never routed as an event.
    "message.prefix.exclude.list": "smarthealth-heartbeat",
}


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def compose() -> dict[str, Any]:
    return yaml.safe_load(COMPOSE_FILE.read_text())


def test_at_least_one_connector_file_exists() -> None:
    assert CONNECTOR_FILES, "infra/connect/connectors/ must contain the scheduling connector"


@pytest.mark.parametrize("path", CONNECTOR_FILES, ids=lambda p: p.stem)
def test_connector_has_required_settings(path: Path) -> None:
    config = _load(path)
    for key, expected in REQUIRED_CONNECTOR_VALUES.items():
        assert config.get(key) == expected, f"{path.name}: {key} must be {expected!r}"
    for key in ("database.dbname", "topic.prefix", "slot.name", "database.hostname"):
        assert config.get(key), f"{path.name}: {key} is required"


@pytest.mark.parametrize("path", CONNECTOR_FILES, ids=lambda p: p.stem)
def test_connector_password_is_a_file_provider_reference(path: Path) -> None:
    """T-3-06: the password is never a literal, only a FileConfigProvider reference."""
    password = _load(path)["database.password"]
    assert password.startswith("${file:"), f"{path.name}: database.password must be ${{file:...}}"
    assert password.endswith(":password}")
    assert "/run/secrets/" in password


def test_connectors_have_distinct_slots_databases_and_prefixes() -> None:
    configs = [_load(path) for path in CONNECTOR_FILES]
    for key in ("slot.name", "database.dbname", "topic.prefix"):
        values = [config[key] for config in configs]
        assert len(values) == len(set(values)), f"{key} must be unique across connectors: {values}"


def test_compose_pins_confluent_images_to_the_same_tag(compose: dict[str, Any]) -> None:
    services = compose["services"]
    kafka = services["kafka"]["image"]
    registry = services["schema-registry"]["image"]
    kafka_init = services["kafka-init"]["image"]
    assert kafka.endswith(f":{CP_TAG}")
    assert registry.endswith(f":{CP_TAG}")
    assert kafka_init == kafka


def test_connect_uses_the_approved_debezium_image(compose: dict[str, Any]) -> None:
    services = compose["services"]
    assert services["connect"]["image"] == CONNECT_IMAGE
    assert services["connect-init"]["image"] == CONNECT_IMAGE


def test_connect_rest_api_is_not_published(compose: dict[str, Any]) -> None:
    """T-3-05: no host port for :8083 anywhere in the Compose file."""
    assert not compose["services"]["connect"].get("ports")
    assert "8083:" not in COMPOSE_FILE.read_text()


def test_connect_mounts_connectors_read_only(compose: dict[str, Any]) -> None:
    for service in ("connect", "connect-init"):
        volumes = compose["services"][service]["volumes"]
        assert "./connect:/connect-init:ro" in volumes, service


def test_connect_reads_its_password_through_the_file_config_provider(
    compose: dict[str, Any],
) -> None:
    connect = compose["services"]["connect"]
    environment = connect["environment"]
    assert environment["CONNECT_CONFIG_PROVIDERS"] == "file"
    assert environment["CONNECT_CONFIG_PROVIDERS_FILE_CLASS"].endswith("FileConfigProvider")
    assert {"source": "debezium_db_password", "target": "debezium.properties"} in connect["secrets"]
    # No literal credential for the Debezium role in any env value.
    assert not any("DEBEZIUM" in key.upper() and "PASSWORD" in key.upper() for key in environment)


def test_postgres_runs_with_logical_decoding(compose: dict[str, Any]) -> None:
    command = compose["services"]["postgres"]["command"]
    assert "wal_level=logical" in command
    assert any(str(part).startswith("max_slot_wal_keep_size=") for part in command)


def test_connect_init_waits_for_the_four_producing_apis(compose: dict[str, Any]) -> None:
    depends = compose["services"]["connect-init"]["depends_on"]
    for service in ("connect", "scheduling", "visit", "provider", "patient"):
        assert depends[service]["condition"] == "service_healthy", service


def test_schema_init_gates_the_producing_services(compose: dict[str, Any]) -> None:
    services = compose["services"]
    assert services["schema-init"]["command"][-1] == "libs.events.register"
    for service in ("scheduling", "visit", "provider", "patient"):
        assert services[service]["depends_on"]["schema-init"]["condition"] == (
            "service_completed_successfully"
        ), service


def test_new_secrets_come_from_the_infra_secret_directory(compose: dict[str, Any]) -> None:
    secrets = compose["secrets"]
    assert secrets["debezium_db_password"]["file"] == "./secrets/infra/debezium.properties"
    assert secrets["grafana_ro_password"]["file"] == "./secrets/infra/grafana_ro.secret"


def test_application_env_points_at_container_network_kafka_and_registry(
    compose: dict[str, Any],
) -> None:
    environment = compose["services"]["scheduling"]["environment"]
    assert environment["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:29092"
    assert environment["SCHEMA_REGISTRY_URL"] == "http://schema-registry:8081"


def test_topic_script_covers_every_catalogue_topic() -> None:
    from libs.events.catalog import AGGREGATE_TOPICS, DLQ_TOPICS  # noqa: PLC0415

    script = (REPO_ROOT / "infra" / "kafka" / "create-topics.sh").read_text()
    for topic in (*AGGREGATE_TOPICS.values(), *DLQ_TOPICS.values()):
        assert topic in script, topic
    assert "retention.ms=-1" in script
    assert "--partitions" in script


@pytest.mark.parametrize("path", CONNECTOR_FILES, ids=lambda p: p.stem)
def test_heartbeat_keeps_a_quiet_slot_acknowledging_wal(path: Path) -> None:
    """A quiet database must still move its slot (WAL retention guard, plan 03-17)."""
    config = _load(path)
    interval = int(config["heartbeat.interval.ms"])
    assert 0 < interval <= 30_000, f"{path.name}: heartbeat interval must be short and positive"
    query = config["heartbeat.action.query"]
    assert "pg_logical_emit_message" in query
    # Read-only to the application schema: no table write, no outbox row, no DDL.
    assert all(word not in query.upper() for word in ("INSERT", "UPDATE", "DELETE", "DROP"))
    prefix = config["message.prefix.exclude.list"]
    assert f"'{prefix}'" in query, "the excluded prefix must be the one the query emits"
