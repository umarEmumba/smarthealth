"""The NFR numbers have ONE source: `loadtest/nfr.json` (plan 02.1-04, D1/D2).

`.planning/REQUIREMENTS.md` (the PRD) and `docs/nfr.md` carry a machine-readable
`key | value | unit` table; the k6 thresholds read the JSON directly. These tests
fail when the three drift apart, when an NFR id is missing from the PRD, its
traceability table or the docs, or when the docs stop saying that the targets
are not measured yet (measurement is Phase 3).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
NFR_JSON = REPO_ROOT / "loadtest" / "nfr.json"
REQUIREMENTS = REPO_ROOT / ".planning" / "REQUIREMENTS.md"
NFR_DOC = REPO_ROOT / "docs" / "nfr.md"

NFR_IDS = [f"NFR-{n:02d}" for n in range(1, 9)]
PHASE2_IDS_SAMPLE = ["SCHED-01", "VISIT-05", "BILL-02"]  # untouched by this plan

# `| key | value | unit |` - key in backticks or bare, value a plain number.
TABLE_ROW = re.compile(
    r"^\|\s*`?(?P<key>[a-z0-9_]+)`?\s*\|\s*(?P<value>-?\d+(?:\.\d+)?)\s*\|\s*(?P<unit>[^|]+?)\s*\|\s*$",
    re.MULTILINE,
)


def _targets() -> dict[str, float]:
    return json.loads(NFR_JSON.read_text(encoding="utf-8"))


def _table(path: Path) -> dict[str, float]:
    """Rows of the machine-readable table whose key is an nfr.json key."""
    wanted = set(_targets())
    rows: dict[str, float] = {}
    for match in TABLE_ROW.finditer(path.read_text(encoding="utf-8")):
        if match["key"] in wanted:
            assert match["key"] not in rows, f"{path.name}: duplicate row for {match['key']}"
            rows[match["key"]] = float(match["value"])
    return rows


def test_nfr_json_is_flat_numbers() -> None:
    targets = _targets()
    assert targets, "nfr.json must not be empty"
    for key, value in targets.items():
        assert isinstance(value, (int, float)) and not isinstance(value, bool), key


@pytest.mark.parametrize("path", [REQUIREMENTS, NFR_DOC], ids=lambda p: p.name)
def test_machine_readable_table_equals_nfr_json_key_for_key(path: Path) -> None:
    table = _table(path)
    targets = _targets()
    assert set(table) == set(targets), (
        f"{path.name} table keys differ from nfr.json: "
        f"missing={sorted(set(targets) - set(table))} extra={sorted(set(table) - set(targets))}"
    )
    for key, value in targets.items():
        assert table[key] == pytest.approx(float(value)), f"{path.name}: {key} {table[key]} != {value}"


def test_headline_numbers_are_the_stated_targets() -> None:
    targets = _targets()
    assert targets["sustained_rps"] == 25
    assert targets["sustained_seconds"] == 600
    assert targets["burst_rps"] == 100
    assert targets["book_p95_ms"] == 300
    assert targets["availability_pct"] == 99.9
    # the quoted request counts follow from rate x duration
    assert targets["sustained_rps"] * targets["sustained_seconds"] == 15_000
    assert targets["burst_rps"] * targets["burst_seconds"] == 6_000


@pytest.mark.parametrize("nfr_id", NFR_IDS)
def test_every_nfr_has_a_checklist_line_and_a_traceability_row(nfr_id: str) -> None:
    text = REQUIREMENTS.read_text(encoding="utf-8")
    assert re.search(rf"^- \[ \] \*\*{nfr_id}\*\*: ", text, re.MULTILINE), f"{nfr_id} checklist line"
    row = re.search(rf"^\|\s*{nfr_id}\s*\|\s*Phase 2\.1\s*\|\s*([^|]+?)\s*\|\s*$", text, re.MULTILINE)
    assert row, f"{nfr_id} traceability row mapped to Phase 2.1"
    assert "measurement Phase 3" in row.group(1)


@pytest.mark.parametrize("nfr_id", NFR_IDS)
def test_every_nfr_is_named_in_the_docs(nfr_id: str) -> None:
    assert nfr_id in NFR_DOC.read_text(encoding="utf-8")


def test_docs_state_the_demand_model_and_that_targets_are_not_measured() -> None:
    doc = NFR_DOC.read_text(encoding="utf-8")
    assert "50,000" in doc and "Demand model" in doc
    assert "not yet measured" in doc
    assert "Phase 3" in doc
    assert "nfr.json" in doc  # the single-source rule is stated


def test_requirements_justify_the_numbers_against_the_stated_scale() -> None:
    text = REQUIREMENTS.read_text(encoding="utf-8")
    assert "Non-Functional Requirements (NFR)" in text
    assert "Demand model" in text and "50,000" in text
    # NFR section sits before "v2 Requirements" so v1 sections stay together
    assert text.index("Non-Functional Requirements (NFR)") < text.index("## v2 Requirements")
    assert "counted separately" in text


def test_phase2_requirement_rows_are_untouched() -> None:
    text = REQUIREMENTS.read_text(encoding="utf-8")
    for req in PHASE2_IDS_SAMPLE:
        assert re.search(rf"^\| {req} \| Phase 2 \|", text, re.MULTILINE), req
