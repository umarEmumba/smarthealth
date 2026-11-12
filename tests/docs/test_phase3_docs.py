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
    "CHILD_OF",  # Jaeger / OpenTracing reference type shown in a trace, not a setting
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


# --------------------------------------------------------------------------- README settings table


def _readme_phase3_settings_rows() -> list[tuple[list[str], list[str]]]:
    """(variable tokens, default tokens) of each row of the README's 'Phase 3 settings' table."""
    text = README.read_text(encoding="utf-8")
    assert "### Phase 3 settings" in text, "README lost its 'Phase 3 settings' section"
    section = text[text.index("### Phase 3 settings") :]
    section = section[: section.index("Retired in Phase 1.1")]
    rows = []
    for line in section.splitlines():
        if not line.startswith("| `"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        rows.append((re.findall(r"`([^`]+)`", cells[0]), re.findall(r"`([^`]+)`", cells[2])))
    return rows


def test_every_phase3_setting_in_the_readme_exists_and_its_default_is_in_the_code() -> None:
    rows = _readme_phase3_settings_rows()
    assert len(rows) >= 15
    for names, defaults in rows:
        for name in names:
            assert name.lower() in source_corpus(), f"README Phase 3 settings: {name!r} is not used by the code"
        for default in defaults:
            if "<" in default or default in {"none", "false", "true"}:
                continue
            assert default.lower() in source_corpus(), f"README default {default!r} for {names} is not in the code"


def test_the_readme_names_every_phase3_setting_the_code_defines() -> None:
    """The notification and analytics settings classes each alias their env names; all are documented."""
    readme = README.read_text(encoding="utf-8")
    for module in ("services/notification/config.py", "services/analytics/config.py"):
        source = (REPO_ROOT / module).read_text(encoding="utf-8")
        for alias in re.findall(r'validation_alias="([A-Z0-9_]+)"', source):
            assert f"`{alias}`" in readme, f"README does not document {alias} ({module})"


def test_env_example_carries_the_analytics_database_url_in_host_form() -> None:
    lines = [
        line
        for line in (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if line.startswith("ANALYTICS_DATABASE_URL=")
    ]
    assert len(lines) == 1
    assert "localhost:5433/analytics_db" in lines[0]
    assert "changeme" in lines[0], "the .env.example value must be a placeholder, never a real credential"


# --------------------------------------------------------------------------- stale statements


STALE_CLAIMS = (
    r"no application code uses them yet",
    r"scaffolding only",
    r"workers expose no metrics",
    r"phase 3 replaces\W",
    r"phase 3 can publish the same",
    r"the general fix is phase 3's",
    r"dashboards not built yet",
    r"grafana dashboards not built",
    r"container runs; dashboards not built",
    r"no kafka/outbox until phase 3",
    r"event backbone \(kafka\), celery jobs\s+and operational analytics are phase 3 and are \*\*not built yet",
    r"phase 3 observability measures them",
    r"measurement is phase 3 observability work",
    r"downstream sweeper \(temporal schedules\) recover",
    r"`visit-downstream-sweeper` \(every 60 s\)",
    r"visit-downstream-sweeper \(every 60 s\)",
    r"removed by\s+event-fed local copies in phase 3",
    # Plan 03-26 (CR-01): a transient outage IS dead-lettered, as TransientExhausted, once the cap is reached.
    r"outage[^.]{0,120}is never dead-lettered",
    r"retried and never dead-lettered",
    r"the message is never dead-lettered",
    r"an outage delays an event and never drops it",
)

STALE_DOCS = ("README.md", "architecture.md", "architecture.html")


@pytest.mark.parametrize("name", STALE_DOCS)
def test_documents_no_longer_make_claims_phase3_made_false(name: str) -> None:
    text = re.sub(r"\s+", " ", documents()[name].lower())
    for claim in STALE_CLAIMS:
        assert not re.search(claim, text), f"{name} still claims {claim!r}"


def test_the_architecture_doc_states_effect_level_exactly_once_and_the_deviation_records() -> None:
    text = phase3_markdown()
    assert "effect-level exactly-once" in text
    assert "EVENT-04" in text and "REQUIREMENTS.md" in text
    assert "idempotency key" in text
    assert "max_slot_wal_keep_size" in text
    assert "hold reaper" in text.lower() and "pending-booking starter" in text.lower()


# --------------------------------------------------------------------------- metric names


_METRIC_NAME = re.compile(r"[a-z][a-z0-9_]*_(?:seconds|total|lag)")


@pytest.mark.parametrize("name", ["architecture.md", "nfr.md", "loadtest/README.md"])
def test_metric_names_the_docs_cite_exist_in_the_code_or_the_dashboards(name: str) -> None:
    text = documents()[name]
    if name == "architecture.md":
        text = phase3_markdown()
    for token in sorted(backticked(text)):
        base = re.sub(r"\{.*\}$", "", token)
        if _METRIC_NAME.fullmatch(base):
            # k6 trend names live in loadtest/booking.js, which the corpus reads (`.js`).
            assert base in source_corpus(), f"{name}: the metric {token!r} is not defined in the code or dashboards"
def test_the_visit_sweeper_is_not_described_as_a_live_trigger_anywhere() -> None:
    """The sweeper Schedule is deleted and its workflow is a tombstone (plan 03-12)."""
    live = (
        r"sweeper stays as a safety net",
        r"sweeper still runs",
        r"visit sweeper still runs",
        r"sweeper finds\s+completed visits",
        r"sweeper starts the workflow",
        r"plan 02-11's sweeper finds",
        r"phase 3 replaces the trigger",
        r"and the lost-trigger sweep\)",
    )
    for path in sorted((REPO_ROOT / "services" / "visit").rglob("*.py")):
        if "migrations" in path.parts:  # revisions are history; their docstrings are not edited
            continue
        text = re.sub(r"\s+", " ", path.read_text(encoding="utf-8").lower())
        for claim in live:
            assert not re.search(claim, text), f"{path.relative_to(REPO_ROOT)} still says {claim!r}"


def test_the_three_named_visit_docstrings_say_what_replaced_the_sweeper() -> None:
    app = (REPO_ROOT / "services/visit/app.py").read_text(encoding="utf-8")
    assert "outbox" in app and "retired" in app
    init = (REPO_ROOT / "services/visit/workflows/__init__.py").read_text(encoding="utf-8")
    assert "tombstone" in init and "list_workflows" in init
    reconciler = (REPO_ROOT / "services/visit/workflows/reconciler.py").read_text(encoding="utf-8").lower()
    assert "outbox" in reconciler and "never-started" in reconciler




# --------------------------------------------------------------------------- ERD, NFR page and load-test README (Task 3)


def test_migration_revisions_the_erd_names_exist_as_files() -> None:
    revisions = {p.stem for p in (REPO_ROOT / "services").glob("*/migrations/versions/*.py")}
    named = set(re.findall(r"\b[a-z]+_\d{4}_[a-z0-9_]+\b", ERD_MD.read_text(encoding="utf-8")))
    assert named, "docs/erd.md names no migration revisions"
    for revision in sorted(named):
        assert revision in revisions, f"docs/erd.md names the revision {revision!r}; no such migration file"
    for revision in ("scheduling_0006_outbox", "visit_0003_outbox", "provider_0003_outbox", "patient_0002_outbox", "analytics_0001_projections"):
        assert revision in named, f"docs/erd.md does not name {revision}"
        assert revision in ERD_HTML.read_text(encoding="utf-8"), f"docs/erd.html does not name {revision}"


@pytest.mark.parametrize("name", ["erd.md", "erd.html"])
def test_tables_views_collections_and_indexes_the_erd_names_exist(name: str) -> None:
    text = documents()[name]
    tokens = backticked(text) | set(re.findall(r"\b(?:v_[a-z_]+|ux_[a-z_]+|notification_[a-z_]+|ix_[a-z_]+)\b", text))
    for token in sorted(tokens):
        if re.fullmatch(r"(?:v_|ux_|ix_)[a-z_]+|notification_(?:deliveries|provider_calls|appointment_state)", token):
            assert token in source_corpus(), f"{name}: {token!r} does not exist in the code or migrations"
    for table in ("outbox", "processed_events", "visits_completed", "notification_deliveries",
                  "notification_provider_calls", "notification_appointment_state", "analytics_db"):
        assert table in text, f"{name} does not mention {table}"


def test_the_dashboards_the_docs_name_exist() -> None:
    uids = {
        re.search(r'"uid":\s*"([^"]+)"', path.read_text(encoding="utf-8")).group(1)  # type: ignore[union-attr]
        for path in (REPO_ROOT / "infra" / "grafana" / "dashboards").glob("*.json")
    }
    assert {"smarthealth-booking-nfr", "smarthealth-events", "smarthealth-notifications", "smarthealth-operations"} <= uids
    for name in ("architecture.md", "nfr.md", "loadtest/README.md"):
        text = phase3_markdown() if name == "architecture.md" else documents()[name]
        for token in sorted(set(re.findall(r"smarthealth-(?:booking-nfr|events|notifications|operations)", text))):
            assert token in uids, f"{name} names the dashboard {token!r}, which is not provisioned"


def test_the_nfr_page_records_the_phase3_smoke_runs_and_keeps_the_unmeasured_status() -> None:
    nfr = NFR_MD.read_text(encoding="utf-8")
    loadtest = LOADTEST_README.read_text(encoding="utf-8")
    for text in (nfr, loadtest):
        for figure in ("21.8", "29.3", "49.1", "200.8", "170.0"):
            assert figure in text, f"a Phase 3 smoke figure {figure} is missing"
        assert "not run" in text or "not been run" in text
        assert "smarthealth-booking-nfr" in text
    assert "booking_saga_seconds" in nfr
    assert "not yet measured" in nfr


STALE_DOCS_ALL = ("README.md", "architecture.md", "architecture.html", "erd.md", "erd.html", "nfr.md", "loadtest/README.md")


@pytest.mark.parametrize("name", STALE_DOCS_ALL)
def test_every_document_is_free_of_the_claims_phase3_made_false(name: str) -> None:
    text = re.sub(r"\s+", " ", documents()[name].lower())
    for claim in STALE_CLAIMS:
        assert not re.search(claim, text), f"{name} still claims {claim!r}"
    assert "sweeper picks it up" not in text and "lets the sweeper find" not in text


# --------------------------------------------------------------------------- plan 03-26: gap closure

PHASE3_TERMS = (
    "TransientExhausted",
    "event_commit_failures_total",
    "restart_lost_triggers",
    "replay_dlq",
    "MultiProcessCollector",
)


@pytest.mark.parametrize("term", PHASE3_TERMS)
def test_the_phase3_architecture_text_names_the_gap_closure_mechanisms(term: str) -> None:
    assert term in phase3_markdown(), f"architecture.md section 10 no longer names {term!r}"
    assert term.lower() in source_corpus(), f"{term!r} is named in the docs but nothing in the code has it"


def test_every_script_the_phase3_section_names_exists() -> None:
    """W4: nothing aspirational: a `scripts/<name>.py` the docs name must be a file in the repository."""
    named = set(re.findall(r"scripts/([A-Za-z0-9_]+\.(?:py|sh))", phase3_markdown()))
    assert {"replay_dlq.py", "backfill_visit_completed_events.py"} <= named
    for name in sorted(named):
        assert (REPO_ROOT / "scripts" / name).is_file(), f"architecture.md names scripts/{name}; the file does not exist"


def test_the_stale_outage_guard_bites() -> None:
    """The guard patterns reject the claim in the three phrasings it used to have, and not the unrelated sentence."""
    old = (
        "an infrastructure outage (schema registry, postgres, temporal, rabbitmq) is never dead-lettered: the offset is held",
        "an infrastructure outage is retried and never dead-lettered.",
        "and the message is never dead-lettered, so an outage delays an event and never drops it.",
    )
    for sentence in old:
        assert any(re.search(claim, sentence) for claim in STALE_CLAIMS), sentence
    unrelated = "an event for an id the projection has never seen is absorbed, never dead-lettered."
    assert not any(re.search(claim, unrelated) for claim in STALE_CLAIMS)
