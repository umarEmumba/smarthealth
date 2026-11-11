"""Static checks on the Architecture & Decision dashboard (plan 02-12, roadmap SC6;
updated by plan 02.1-13 for Phase 2.1 and DASH-04, and by plan 03-19 for Phase 3,
DASH-03 and DASH-04: the five event flows, the nine Phase 3 decision cards, the
Phase 3 evaluation Q&A, the roadmap block and the `v4 · Phase 3` marker).

The dashboard must open from disk with networking off (no external script,
stylesheet or font), carry the Phase 2 content the mentor review grades
(implemented flows as inline SVG state diagrams, the Temporal rationale,
layered idempotency, interim mechanisms, the saga-order deviation, Q&A and
the roadmap) and the Phase 2.1 content (the seven-state appointment diagram,
the check-in window, schedule-change and reconciler flows, the new decisions
and the honest "targets, not yet measured" wording), and lose the stale
pre-Phase-2 wording. Visual quality is a manual check (02-VALIDATION
"Manual-Only Verifications"); no test here asserts how the page looks.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

DASHBOARD = Path(__file__).resolve().parents[2] / "docs" / "architecture-dashboard.html"


@pytest.fixture(scope="module")
def html() -> str:
    return DASHBOARD.read_text(encoding="utf-8")


def _section(html: str, section_id: str) -> str:
    match = re.search(rf'<section id="{section_id}">(.*?)</section>', html, re.DOTALL)
    assert match, f"section #{section_id} missing"
    return match.group(1)


def test_self_contained(html: str) -> None:
    assert not re.search(r"<script[^>]+src=\"https?://", html)
    assert not re.search(r"<link[^>]+href=\"https?://", html)
    assert "@import" not in html
    assert not re.search(r"url\(\s*['\"]?https?://", html)


def test_inline_svg_state_diagrams_are_accessible(html: str) -> None:
    svgs = re.findall(r"<svg\b[^>]*>.*?</svg>", html, re.DOTALL)
    assert len(svgs) >= 4
    for svg in svgs:
        opening = svg[: svg.index(">") + 1]
        assert 'role="img"' in opening, opening
        assert "<title" in svg, opening
    flows = _section(html, "flows")
    for states in (
        ("pending", "confirmed", "failed", "cancelled"),
        ("checked_in", "in_progress", "completed"),
        ("waiting", "matched", "booked"),
        ("verified", "voided", "refund_pending", "refunded", "no_refund", "finalized"),
    ):
        assert any(all(state in svg for state in states) for svg in svgs), states
    assert flows.count("<svg") >= 4
    # Phase 2.1 (M1): one diagram carries every new appointment state.
    assert any(all(state in svg for state in ("checked_in", "completed", "no_show")) for svg in svgs)


def test_appointment_svg_draws_all_seven_states(html: str) -> None:
    svgs = re.findall(r"<svg\b[^>]*>.*?</svg>", html, re.DOTALL)
    appointment = [svg for svg in svgs if 'id="sd-appt-t"' in svg]
    assert len(appointment) == 1
    labels = set(re.findall(r'<text class="sd-tx"[^>]*>([^<]+)</text>', appointment[0]))
    assert labels == {
        "pending",
        "confirmed",
        "checked_in",
        "completed",
        "no_show",
        "cancelled",
        "failed",
    }
    assert "<desc" in appointment[0]


def test_version_marker_in_meta_row_and_footer(html: str) -> None:
    # Plan 03-19: the marker moves from v3.1 · Phase 2.1 to v4 · Phase 3 (meta row and footer).
    assert html.count("v4 · Phase 3") >= 2
    assert "v3.1 · Phase 2.1" not in html
    assert "v3 · Phase 2" not in html


def test_phase3_booking_event_flow_present(html: str) -> None:
    flows = _section(html, "flows")
    svgs = re.findall(r"<svg\b[^>]*>.*?</svg>", flows, re.DOTALL)
    booking = [svg for svg in svgs if 'id="sd-ev-booking-t"' in svg]
    assert len(booking) == 1
    opening = booking[0][: booking[0].index(">") + 1]
    assert 'role="img"' in opening
    assert "<title" in booking[0] and "<desc" in booking[0]
    for needle in (
        "outbox",
        "Debezium",
        "smarthealth.appointment.events",
        "notification-trigger",
        "analytics-projection",
        "ReminderWorkflow",
        "Celery",
    ):
        assert needle in booking[0], needle


def test_phase2_content_present(html: str) -> None:
    # Plan 03-19: the Phase 2 callout said "Phase 3 replaces them". Phase 3 has now
    # replaced (or deliberately kept) each interim mechanism, so the needle is the
    # rewritten callout title "Interim mechanisms after Phase 3"; "Phase 3 replaces"
    # itself is asserted GONE in test_stale_text_removed.
    for needle in (
        "Why Temporal, not Celery",
        "outbox",
        "sweeper",
        "Interim mechanisms after Phase 3",
        "billing pre-check before notification",
        "uq_appointments_active_slot",
        "SKIP LOCKED",
    ):
        assert needle in html, needle


def test_phase21_content_present(html: str) -> None:
    flows = _section(html, "flows")
    decisions = _section(html, "decisions")
    for needle in ("check-in window", "schedule change", "reconcil", "no_show", "Phase 2.1"):
        assert needle in html, needle
    # The three new flows live in the flows section, the decisions in decisions.
    for needle in ("Check-in window and appointment states", "Provider schedule change", "Reconcilers"):
        assert needle in flows, needle
    for needle in (
        "Audit after commit",
        "Schedule-change delivery",
        "Which slots are stale",
        "Reconciler mechanism",
        "Single-use refresh tokens",
        "Load test and NFRs",
    ):
        assert needle in decisions, needle
    # Honest about the unmeasured targets and the open risks.
    for needle in ("not yet measured", "billing_pending", "audit.write_failed"):
        assert needle in html, needle


def test_qa_covers_phase21_themes(html: str) -> None:
    qa = _section(html, "qa").lower()
    for needle in (
        "checked in from cancelling",
        "provider changes their schedule",
        "fails after the database commit",
        "stuck states",
        "load and availability targets",
        "not yet verified",
        "not yet measured",
    ):
        assert needle in qa, needle
    # The pre-2.1 claim that no load test exists must be gone.
    assert "no load test" not in html.lower()


def test_qa_covers_phase2_questions(html: str) -> None:
    qa = _section(html, "qa").lower()
    for needle in ("double-book", "idempoten", "worker dies", "not yet verified"):
        assert needle in qa, needle


def test_roadmap_marks_phase_3_active(html: str) -> None:
    roadmap = _section(html, "roadmap")
    phases = re.findall(r'<div class="phase( active)?">(.*?)</div>\s*</div>', roadmap, re.DOTALL)
    by_title = {body: bool(active) for active, body in phases}
    phase_11 = next(body for body in by_title if "Phase 1.1 ·" in body)
    phase_2 = next(body for body in by_title if "Phase 2 ·" in body)
    phase_21 = next(body for body in by_title if "Phase 2.1 ·" in body)
    phase_3 = next(body for body in by_title if "Phase 3 ·" in body)
    # Blocks for 1.1, 2 and 2.1 stay; the ROADMAP marks 2 and 2.1 complete, so only Phase 3 is active.
    assert "complete" in phase_11 and not by_title[phase_11]
    assert "complete" in phase_2 and not by_title[phase_2]
    assert "complete" in phase_21 and not by_title[phase_21]
    assert by_title[phase_3]
    assert sum(by_title.values()) == 1
    assert len({phase_11, phase_2, phase_21, phase_3}) == 4
    assert "Phase 2.1 ·" not in phase_2 and "Phase 2 ·" not in phase_21
    # The Phase 3 block must not claim more than is verified: the phase gate (plan 03-20) and the
    # human visual checks come after this plan.
    p_num = re.search(r'<div class="p-num">(.*?)</div>', phase_3, re.DOTALL)
    assert p_num, "Phase 3 block has no p-num line"
    assert not re.search(r"\bcomplete(d)?\b", p_num.group(1)), p_num.group(1)
    assert "phase gate" in p_num.group(1) and "pending" in p_num.group(1)
    assert "EVENT-01..05" in phase_3 and "DASH-03..04" in phase_3


def test_stale_text_removed(html: str) -> None:
    assert "Hand a reminder job to Celery" not in html
    assert "implemented in Phase 2" not in html
    booking = _section(html, "flows")
    assert "via the outbox" not in booking
    # Plan 03-19: claims that Phase 3 made false (see docs/architecture.md section 10).
    for stale in (
        "scaffolding only",
        "no producer/consumer code yet",
        "no tasks defined yet",
        "Phase 3 replaces",
        "have no HTTP listener and no metrics yet",
        "Workers expose no metrics",
        "workers expose no metrics",
        "Grafana dashboards are not yet built",
        "A lost start is picked up by the <strong>sweeper</strong>",
        "Phase 3 removes the availability coupling",
        "event infrastructure does not exist yet",
        "a dedicated Audit service is Phase 3",
        "Phase 3 observability measures them",
        "nothing is delegated to it yet",
        "compute a projection",
        # Plan 03-26 (CR-01): an outage is dead-lettered as TransientExhausted once the cap is reached.
        "retried and never dead-lettered",
        "the message is never dead-lettered",
        "an outage delays an event and never drops it",
    ):
        assert stale not in html, stale


# --------------------------------------------------------------------------- Phase 3 (plan 03-19)

PHASE3_FLOWS = {
    "booking": ("outbox", "Debezium", "smarthealth.appointment.events", "notification-trigger", "analytics-projection", "ReminderWorkflow", "Celery"),
    "cancel": ("appointment.cancelled", "rescheduled", "notification-trigger", "signal", "reminder-", "cancelled_at", "emits nothing"),
    "visit": ("visit.completed", "visit-downstream", "VisitCompleted", "followup-", "sweeper is retired", "appointment.completed", "backfill_visit_completed_events"),
    "notify": ("notifications.send", "ReminderWorkflow", "dedupe_key", "notifications.dlq", "smarthealth.dlx", "skipped_closed", "skipped_stale", "idempotency key", "intent records"),
    "analytics": ("analytics-consumer", "processed_events", "/api/analytics", "grafana_ro", "cutover", "rebuild", "EVENT-04", "ONE tx"),
}


def _event_svgs(html: str) -> dict[str, str]:
    flows = _section(html, "flows")
    found: dict[str, str] = {}
    for svg in re.findall(r"<svg\b[^>]*>.*?</svg>", flows, re.DOTALL):
        match = re.search(r'id="sd-ev-([a-z]+)-t"', svg)
        if match:
            found[match.group(1)] = svg
    return found


def test_phase3_flows_present(html: str) -> None:
    svgs = _event_svgs(html)
    assert set(svgs) == set(PHASE3_FLOWS)
    for name, needles in PHASE3_FLOWS.items():
        svg = svgs[name]
        opening = svg[: svg.index(">") + 1]
        assert 'role="img"' in opening and "aria-labelledby" in opening, name
        assert "<title" in svg and "<desc" in svg, name
        for needle in needles:
            assert needle in svg, (name, needle)
    # The marker ids each figure references exist inside it (no cross-figure id reuse).
    for name, svg in svgs.items():
        assert f'id="ah-ev-{name}"' in svg and f"url(#ah-ev-{name})" in svg


def test_phase3_decision_cards_present(html: str) -> None:
    decisions = _section(html, "decisions")
    for heading in (
        "CDC with Debezium",
        "Debezium, off the Confluent line",
        "EVENT-04 deviation",
        "Avro, Schema Registry",
        "Topic per aggregate",
        "three flavours",
        "Temporal timers, not Celery ETA",
        "Celery on RabbitMQ 4.3",
        "sweeper retired",
    ):
        assert heading in decisions, heading
    # The cost accepted for CDC is stated, not only the benefit.
    for needle in ("Kafka Connect", "replication slot", "wal_level=logical"):
        assert needle in decisions, needle


def test_phase3_deviation_cards_present(html: str) -> None:
    decisions = _section(html, "decisions")
    for needle in ("EVENT-04", "quay.io/debezium/connect", "cp-kafka-connect:7.6.13", "same Confluent Platform line"):
        assert needle in decisions, needle
    # Both deviations carry the user's answer from plan 03-01.
    assert decisions.count("Accept all six (Recommended)") >= 2
    assert "REQUIREMENTS.md" in decisions


def test_qa_covers_phase3_themes(html: str) -> None:
    qa = _section(html, "qa").lower()
    for needle in (
        "polling publisher",  # why CDC
        "debezium",
        "celery eta",  # why Temporal reminders
        "at-least-once",
        "effect-level exactly-once",
        "idempotency key",  # the real-provider caveat
        "dedupe key",
        "phi",
        "outage",
        "rebuild",
        "cutover",
        "one connected trace",
        "event-04",
        "d-07",
        "not yet verified",
        "sustained",
        "burst",
        "contention",
    ):
        assert needle in qa, needle
    # The real-provider question is asked by name.
    assert "no idempotency key" in qa


def test_phase3_honesty_items_present(html: str) -> None:
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))
    for needle in (
        "no loss",
        "at-least-once on the wire",
        "effect-level exactly-once",
        "5 duplicates of 11",
        "D-13",
        "go-live",
        "throw-away test events",
        "not yet measured",
        "accept latency crept up",
        "deferred",
        "provider.schedule_changed",
        "published but not consumed",
        "paused",
        "accepted for now",
        "ten D-07 event types",
        "long-lived",
        "nobody has looked at",
        "local copies were not built",
        "no authentication",
    ):
        assert needle in text, needle
    # The measured smoke numbers are quoted only as smoke; nothing says the NFRs were measured.
    assert "NFR-01..08" in text
    assert not re.search(r"NFR-01\.\.08 (?:are|were) (?:met|measured|verified)", text)
    assert not re.search(r"sustained[^.]{0,40}(?:passed|met)", text, re.IGNORECASE)


_EXACTLY_ONCE = re.compile(r"exactly[- ]once", re.IGNORECASE)
_NEGATION = re.compile(r"\b(?:not|never|no|cannot|neither|without)\b", re.IGNORECASE)


def _exactly_once_claims(text: str) -> list[str]:
    flat = re.sub(r"\s+", " ", text)
    sentences = re.split(r"(?<=[.!?:;])\s+", flat)
    return [
        s.strip()
        for s in sentences
        if _EXACTLY_ONCE.search(s) and "effect" not in s.lower() and not _NEGATION.search(s)
    ]


def test_dashboard_never_claims_wire_level_exactly_once(html: str) -> None:
    text = re.sub(r"<[^>]+>", " ", html)
    assert _exactly_once_claims(text) == []
    # The checker itself bites (same wording rules as tests/docs/test_phase3_docs.py).
    assert _exactly_once_claims("Events get exactly-once delivery on the topic.")
    assert not _exactly_once_claims("Delivery is at-least-once; the effect is exactly once through idempotent consumers.")


def test_event_names_the_dashboard_cites_exist_in_the_catalog(html: str) -> None:
    from libs.events.catalog import AGGREGATE_TOPICS, DLQ_TOPICS, EVENT_CATALOG

    text = re.sub(r"<[^>]+>", " ", html)
    topics = set(re.findall(r"smarthealth\.(?:dlq\.[a-z-]+|[a-z]+\.events)", text))
    assert topics, "no topic names found"
    known = set(AGGREGATE_TOPICS.values()) | set(DLQ_TOPICS.values())
    assert topics <= known, topics - known
    types = set(
        re.findall(
            r"\b(?:appointment|visit|patient|provider)\.(?:booked|cancelled|rescheduled|checked_in|completed|no_show|registered|deactivated|schedule_changed)\b",
            text,
        )
    )
    assert types, "no event types found"
    assert types <= set(EVENT_CATALOG), types - set(EVENT_CATALOG)
    # Every event type of the D-07 set is named somewhere on the page.
    assert set(EVENT_CATALOG) <= types, set(EVENT_CATALOG) - types


def test_qa_outage_answer_states_the_cap_and_the_recovery_tools(html: str) -> None:
    """Plan 03-26: the outage answer says what the code does (cap, TransientExhausted, three recovery routes)."""
    qa = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", _section(html, "qa")))
    assert "TransientExhausted" in qa
    assert "10 attempts and 15 minutes" in qa
    for tool in ("visit reconciler", "analytics rebuild", "DLQ replay script"):
        assert tool in qa, tool
    assert "outage" in qa.lower()
