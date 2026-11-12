"""The deterministic billing pre-check rule set (D-10, BILL-01).

A modeled stand-in for a payer eligibility check: no external call, no
randomness, so a "deliberately failed booking" is reproducible on demand.

    no reference / empty / "SELF-PAY" (any case) -> verified, self_pay
    starts with the configured failing prefix    -> failed,   insured, coverage_denied
    anything else                                -> verified, insured

The raw reference is evaluated in memory only. `PrecheckDecision` carries the
outcome and a coarse `coverage_kind`; it never carries the reference itself,
so nothing downstream can persist or log it (T-02-24).
"""

from __future__ import annotations

from dataclasses import dataclass

from services.billing.domain.states import BillingStatus

SELF_PAY_REFERENCE = "SELF-PAY"


class CoverageKind:
    SELF_PAY = "self_pay"
    INSURED = "insured"


class PrecheckReason:
    COVERAGE_DENIED = "coverage_denied"


@dataclass(frozen=True, slots=True)
class PrecheckDecision:
    status: str
    coverage_kind: str
    reason_code: str | None = None


def evaluate_precheck(coverage_ref: str | None, fail_prefix: str) -> PrecheckDecision:
    """Pure: the pre-check outcome for `coverage_ref` under `fail_prefix`."""
    reference = (coverage_ref or "").strip()
    if not reference or reference.upper() == SELF_PAY_REFERENCE:
        return PrecheckDecision(BillingStatus.VERIFIED, CoverageKind.SELF_PAY)
    # An empty prefix would deny every insured booking; treat it as "no rule".
    if fail_prefix and reference.startswith(fail_prefix):
        return PrecheckDecision(
            BillingStatus.FAILED, CoverageKind.INSURED, PrecheckReason.COVERAGE_DENIED
        )
    return PrecheckDecision(BillingStatus.VERIFIED, CoverageKind.INSURED)
