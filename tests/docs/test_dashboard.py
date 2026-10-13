"""Static checks on the Architecture & Decision dashboard (plan 02-12, roadmap SC6).

The dashboard must open from disk with networking off (no external script,
stylesheet or font), carry the Phase 2 content the mentor review grades
(implemented flows as inline SVG state diagrams, the Temporal rationale,
layered idempotency, interim mechanisms, the saga-order deviation, Q&A and
the roadmap), and lose the stale pre-Phase-2 wording. Visual quality is a
manual check (02-VALIDATION "Manual-Only Verifications").
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


def test_version_marker_in_meta_row_and_footer(html: str) -> None:
    assert html.count("v3 · Phase 2") >= 2


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


def test_stale_text_removed(html: str) -> None:
    assert "Hand a reminder job to Celery" not in html
    assert "implemented in Phase 2" not in html
    booking = _section(html, "flows")
    assert "via the outbox" not in booking
