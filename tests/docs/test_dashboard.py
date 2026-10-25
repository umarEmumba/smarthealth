"""Static checks on the Architecture & Decision dashboard (plan 02-12, roadmap SC6;
updated by plan 02.1-13 for Phase 2.1 and DASH-04).

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
    # Plan 02.1-13: the marker moves from v3 · Phase 2 to v3.1 · Phase 2.1.
    assert html.count("v3.1 · Phase 2.1") >= 2
    assert "v3 · Phase 2" not in html


def test_phase2_content_present(html: str) -> None:
    for needle in (
        "Why Temporal, not Celery",
        "outbox",
        "sweeper",
        "Phase 3 replaces",
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


def test_roadmap_marks_phase_2_active(html: str) -> None:
    roadmap = _section(html, "roadmap")
    phases = re.findall(r'<div class="phase( active)?">(.*?)</div>\s*</div>', roadmap, re.DOTALL)
    by_title = {body: bool(active) for active, body in phases}
    phase_11 = next(body for body in by_title if "Phase 1.1 ·" in body)
    phase_2 = next(body for body in by_title if "Phase 2 ·" in body)
    assert "complete" in phase_11 and not by_title[phase_11]
    assert by_title[phase_2]
    # Phase 2.1 has a block but is deliberately not marked active: Phase 2's own
    # verification is still pending, so it keeps the active marker.
    phase_21 = next(body for body in by_title if "Phase 2.1 ·" in body)
    assert not by_title[phase_21]
    assert "Phase 2.1 ·" not in phase_2 and "Phase 2 ·" not in phase_21
    assert sum(by_title.values()) == 1


def test_stale_text_removed(html: str) -> None:
    assert "Hand a reminder job to Celery" not in html
    assert "implemented in Phase 2" not in html
    booking = _section(html, "flows")
    assert "via the outbox" not in booking
