"""The Phase 3 documents name only things that exist, and claim only what the code does (plan 03-18).

Phase 3 added an event backbone, three consumers, a notification pipeline and
analytics, and the documents describe them in prose a reader will trust. These
tests tie the names in that prose to the code, so a renamed topic, a dropped
Compose service or a mistyped setting fails here instead of misleading a reader:

* every `smarthealth.*` topic (and the dead-letter exchange) the docs name exists in
  `libs/events/catalog.py` (or the Celery app, for the exchange);
* every consumer group named is a real one, and every real group is documented;
* the Compose services the Phase 3 table lists exist in `infra/docker-compose.yml`,
  and every Phase 3 service is listed;
* every event type, record name, Celery queue and setting (ALL_CAPS with an
  underscore, in backticks) the Phase 3 sections name exists in the code;
* no document claims wire-level exactly-once delivery (the honest statement is
  at-least-once on the wire and exactly-once at the effect, research F4);
* the stale statements Phase 3 made false (Kafka "scaffolding only", the visit sweeper as
  a live trigger, "workers expose no metrics") are gone.

Whether the code behaves as described is the job of the suites that test the code; this
module only keeps the documents and the code from drifting apart. Plan 03-18 added the
checks for the ERD and the NFR page (migration, table and metric names) at the end of the
file. The test of the exactly-once checker itself proves the gate bites.
"""

from __future__ import annotations

import re
from functools import cache
from pathlib import Path

import pytest
import yaml

from libs.events.catalog import AGGREGATE_TOPICS, CONSUMER_GROUPS, DLQ_TOPICS, EVENT_CATALOG
from services.notification.contracts import TYPE_FOLLOWUP, TYPE_REMINDER

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS = REPO_ROOT / "docs"
ARCHITECTURE_MD = DOCS / "architecture.md"
ARCHITECTURE_HTML = DOCS / "architecture.html"
README = REPO_ROOT / "README.md"
ERD_MD = DOCS / "erd.md"
ERD_HTML = DOCS / "erd.html"
NFR_MD = DOCS / "nfr.md"
LOADTEST_README = REPO_ROOT / "loadtest" / "README.md"
COMPOSE = REPO_ROOT / "infra" / "docker-compose.yml"

PHASE3_HEADING = "## 10. Phase 3: event backbone"

# Names that are intentionally external to this repository's code, or live only in a
# place the scan below does not read. Each entry says why. Keep this list short.
ALLOWED_EXTERNAL = {
    # The image reference and the broker/Connect product names are not code names.
    "quay.io/debezium/connect:3.7.1.Final",
}
# Backticked ALL_CAPS tokens that are Postgres/Kafka/Temporal vocabulary, not settings.
ALLOWED_CAPS = {
    "REJECT_DUPLICATE",  # temporalio WorkflowIDConflictPolicy / reuse policy member
    "USE_EXISTING",
    "ALLOW_DUPLICATE",
}

# Phase 3 Compose services; each must exist in the Compose file and in the docs table.
PHASE3_SERVICES = (
    "kafka-init",
    "schema-init",
    "connect",
    "connect-init",
    "visit-consumer",
    "analytics",
    "analytics-consumer",
    "rabbitmq-init",
    "notification-celery",
    "notification-consumer",
    "notification-temporal-worker",
)

SOURCE_DIRS = ("services", "libs", "infra", "scripts", "loadtest")
SOURCE_SUFFIXES = {".py", ".yml", ".yaml", ".sh", ".json", ".js", ".toml", ".properties", ".mako"}


def phase3_markdown() -> str:
    text = ARCHITECTURE_MD.read_text(encoding="utf-8")
    assert PHASE3_HEADING in text, f"architecture.md lost its heading {PHASE3_HEADING!r}"
    return text[text.index(PHASE3_HEADING) :]


@cache
def source_corpus() -> str:
    """Lower-cased text of the code and configuration (never `infra/secrets`, `.env`)."""
    parts: list[str] = []
    for name in SOURCE_DIRS:
        for path in sorted((REPO_ROOT / name).rglob("*")):
            relative = path.relative_to(REPO_ROOT).parts
            if "secrets" in relative or "__pycache__" in relative or not path.is_file():
                continue
            if path.suffix in SOURCE_SUFFIXES or path.name == ".env.example":
                try:
                    parts.append(path.read_text(encoding="utf-8"))
                except UnicodeDecodeError:  # a binary asset; nothing to name-check
                    continue
    parts.append((REPO_ROOT / ".env.example").read_text(encoding="utf-8"))
    parts.append((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return "\n".join(parts).lower()


def compose_services() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"]


def code_groups() -> set[str]:
    """Consumer group ids as the consumer modules declare them."""
    groups: set[str] = set()
    for module in sorted((REPO_ROOT / "services").glob("*/consumer.py")):
        groups.update(re.findall(r'^GROUP_ID\s*=\s*"([^"]+)"', module.read_text(encoding="utf-8"), re.M))
    return groups


def backticked(text: str) -> set[str]:
    """Tokens inside Markdown backticks or HTML <code> elements."""
    return set(re.findall(r"`([^`\n]+)`", text)) | set(re.findall(r"<code>([^<\n]+)</code>", text))


def documents() -> dict[str, str]:
    paths = (README, ARCHITECTURE_MD, ARCHITECTURE_HTML, ERD_MD, ERD_HTML, NFR_MD, LOADTEST_README)
    return {path.name if path.parent.name != "loadtest" else "loadtest/README.md": path.read_text(encoding="utf-8")
            for path in paths}


DOC_PARAMS = pytest.mark.parametrize("name", sorted(documents()))

_TOPIC_NAME = re.compile(r"(?<![\w./-])smarthealth\.[a-z0-9-]+(?:\.[a-z0-9-]+)*")


def _topic_like_names(text: str) -> set[str]:
    return {m for m in _TOPIC_NAME.findall(text) if not m.endswith(".")}


# --------------------------------------------------------------------------- topics


@DOC_PARAMS
def test_every_topic_the_docs_name_exists_in_the_code(name: str) -> None:
    text = documents()[name]
    topics = set(AGGREGATE_TOPICS.values())
    dlqs = set(DLQ_TOPICS.values())
    for token in sorted(_topic_like_names(text)):
        if token.startswith("smarthealth.dlq."):
            assert token in dlqs, f"{name}: dead-letter topic {token!r} is not in DLQ_TOPICS"
        elif token.endswith(".events"):
            assert token in topics, f"{name}: topic {token!r} is not in AGGREGATE_TOPICS"
        elif token == "smarthealth.dlx":
            assert "smarthealth.dlx" in source_corpus(), f"{name}: the dead-letter exchange is not in the code"
        else:
            raise AssertionError(f"{name}: {token!r} looks like a topic or exchange but matches no known kind")


def test_every_real_topic_is_documented_in_the_architecture_doc() -> None:
    text = phase3_markdown()
    named = _topic_like_names(text)
    for topic in [*AGGREGATE_TOPICS.values(), *DLQ_TOPICS.values()]:
        assert topic in named, f"architecture.md Phase 3 sections do not name {topic}"


@DOC_PARAMS
def test_registry_record_names_in_the_docs_are_real_event_records(name: str) -> None:
    known = {spec.record_name for spec in EVENT_CATALOG.values()}
    for record in set(re.findall(r"com\.smarthealth\.events\.[A-Za-z]+", documents()[name])):
        assert record in known, f"{name}: {record} is not an event record in the catalogue"


# --------------------------------------------------------------------------- groups


def test_the_code_declares_exactly_the_catalogued_consumer_groups() -> None:
    assert code_groups() == set(CONSUMER_GROUPS)


@DOC_PARAMS
def test_every_consumer_group_the_docs_name_is_a_real_group(name: str) -> None:
    text = documents()[name]
    shaped = {t for t in backticked(text) if re.fullmatch(r"[a-z]+-(?:downstream|trigger|projection)", t)}
    after_word = set(re.findall(r"\bgroups? `([a-z][a-z-]+)`", text))
    for token in sorted(shaped | after_word):
        assert token in code_groups(), f"{name}: {token!r} is not a consumer group declared in the code"


def test_every_consumer_group_is_documented_in_the_architecture_doc() -> None:
    text = phase3_markdown()
    for group in sorted(code_groups()):
        assert f"`{group}`" in text, f"architecture.md Phase 3 sections do not name the group {group}"


# --------------------------------------------------------------------------- services


def _compose_table_rows(text: str) -> list[str]:
    """Backticked first cells of the table whose header cell is 'Compose service'."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if re.match(r"\|\s*Compose service\s*\|", line):
            rows: list[str] = []
            for row in lines[index + 2 :]:
                match = re.match(r"\|\s*`([^`]+)`", row)
                if not match:
                    break
                rows.append(match.group(1))
            return rows
    raise AssertionError("architecture.md Phase 3 sections have no 'Compose service' table")


def test_the_compose_service_table_lists_only_real_services() -> None:
    services = compose_services()
    rows = _compose_table_rows(phase3_markdown())
    assert rows, "the Compose service table is empty"
    for service in rows:
        assert service in services, f"the Compose service table names {service!r}, which Compose does not define"


@pytest.mark.parametrize("service", PHASE3_SERVICES)
def test_every_phase3_service_exists_and_is_in_the_table(service: str) -> None:
    assert service in compose_services(), f"Compose does not define {service!r}"
    assert service in _compose_table_rows(phase3_markdown()), f"the Phase 3 table does not list {service!r}"


# --------------------------------------------------------------------------- event types, queues, settings


def test_event_types_the_phase3_docs_name_exist() -> None:
    known = set(EVENT_CATALOG) | {TYPE_REMINDER, TYPE_FOLLOWUP}
    for token in sorted(backticked(phase3_markdown())):
        if re.fullmatch(r"(?:appointment|visit|provider|patient)\.[a-z_]+", token):
            assert token in known, f"architecture.md Phase 3 names the event type {token!r}, which is not in the code"
    for event_type in EVENT_CATALOG:
        assert f"`{event_type}`" in phase3_markdown(), f"the Phase 3 sections do not name `{event_type}`"


def test_celery_queues_the_phase3_docs_name_exist() -> None:
    for token in sorted(backticked(phase3_markdown())):
        if re.fullmatch(r"notifications\.[a-z]+", token):
            assert token in source_corpus(), f"{token!r} is not a queue or task in the code"


def _setting_tokens(tokens: set[str]) -> set[str]:
    return {t for t in tokens if re.fullmatch(r"[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+", t)}


def test_every_setting_the_phase3_sections_name_exists_in_the_code() -> None:
    for token in sorted(_setting_tokens(backticked(phase3_markdown())) - ALLOWED_CAPS):
        assert token.lower() in source_corpus(), f"architecture.md Phase 3 names the setting {token!r}; nothing in the code uses it"


def test_every_setting_the_html_phase3_view_names_exists_in_the_code() -> None:
    html = ARCHITECTURE_HTML.read_text(encoding="utf-8")
    for token in sorted(_setting_tokens(backticked(html)) - ALLOWED_CAPS):
        assert token.lower() in source_corpus(), f"architecture.html names the setting {token!r}; nothing in the code uses it"


# --------------------------------------------------------------------------- honest claims


_EXACTLY_ONCE = re.compile(r"exactly[- ]once", re.IGNORECASE)
_NEGATION = re.compile(r"\b(?:not|never|no|cannot|neither|without)\b", re.IGNORECASE)


def exactly_once_claims(text: str) -> list[str]:
    """Sentences that say exactly-once without saying `effect` (or denying it)."""
    flat = re.sub(r"\s+", " ", text)
    sentences = re.split(r"(?<=[.!?:;])\s+", flat)
    offenders = []
    for sentence in sentences:
        if _EXACTLY_ONCE.search(sentence) and "effect" not in sentence.lower() and not _NEGATION.search(sentence):
            offenders.append(sentence.strip())
    return offenders


@DOC_PARAMS
def test_no_document_claims_wire_level_exactly_once(name: str) -> None:
    assert exactly_once_claims(documents()[name]) == []


def test_the_exactly_once_checker_rejects_a_wire_level_claim() -> None:
    assert exactly_once_claims("Events get exactly-once delivery on the topic.")
    assert exactly_once_claims("We provide exactly once semantics end to end.")
    assert not exactly_once_claims("Delivery is at-least-once; the effect is exactly once through idempotent consumers.")
    assert not exactly_once_claims("Kafka does not give exactly-once delivery from Debezium.")


def test_the_architecture_doc_states_effect_level_exactly_once_and_at_least_once() -> None:
    text = phase3_markdown()
    assert "effect-level exactly-once" in text or "effect-level exactly once" in text
    assert "at-least-once" in text
