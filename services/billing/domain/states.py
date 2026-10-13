"""Billing record states and the allowed transitions between them (BILL-01/02).

`BillingStatus` is a plain class of `str` constants, NOT a `(str, Enum)`: the
values are what Scheduling and Visit activities send and read back, and plain
strings round-trip through Temporal's JSON converter intact (research Pitfall 1).

Lifecycle:

    pending ──> verified ──> voided            (booking compensated, D-09)
       │           ├──────> refund_pending ──> refunded   (cancel before cutoff, D-13)
       │           ├──────> no_refund                     (cancel inside cutoff, D-13)
       │           └──────> finalized                     (visit completed, D-16)
       ├──> failed ──> voided
       └──> voided

`pending` is never observable from outside: the pre-check writes it and its
outcome in ONE transaction. Reschedule (D-14) is not a state change; it appends
a re-link history row with `from_status == to_status`.
"""

from __future__ import annotations


class BillingStatus:
    PENDING = "pending"
    VERIFIED = "verified"
    FAILED = "failed"
    VOIDED = "voided"
    REFUND_PENDING = "refund_pending"
    REFUNDED = "refunded"
    NO_REFUND = "no_refund"
    FINALIZED = "finalized"

    ALL = (
        "pending",
        "verified",
        "failed",
        "voided",
        "refund_pending",
        "refunded",
        "no_refund",
        "finalized",
    )


ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    BillingStatus.PENDING: frozenset(
        {BillingStatus.VERIFIED, BillingStatus.FAILED, BillingStatus.VOIDED}
    ),
    BillingStatus.VERIFIED: frozenset(
        {
            BillingStatus.VOIDED,
            BillingStatus.REFUND_PENDING,
            BillingStatus.NO_REFUND,
            BillingStatus.FINALIZED,
        }
    ),
    BillingStatus.FAILED: frozenset({BillingStatus.VOIDED}),
    BillingStatus.REFUND_PENDING: frozenset({BillingStatus.REFUNDED}),
    # Terminal states.
    BillingStatus.VOIDED: frozenset(),
    BillingStatus.REFUNDED: frozenset(),
    BillingStatus.NO_REFUND: frozenset(),
    BillingStatus.FINALIZED: frozenset(),
}

# Targets a caller may request through the transitions endpoint. `verified` and
# `failed` are only ever set by the pre-check's own rule evaluation.
TRANSITION_TARGETS: tuple[str, ...] = (
    BillingStatus.VOIDED,
    BillingStatus.REFUND_PENDING,
    BillingStatus.REFUNDED,
    BillingStatus.NO_REFUND,
    BillingStatus.FINALIZED,
)

# Least privilege per calling service (T-02-22): Scheduling compensates and
# cancels (D-09, D-13); Visit only finalizes after completion (D-16). Admin
# user tokens may request any target.
SERVICE_TRANSITION_TARGETS: dict[str, frozenset[str]] = {
    "scheduling": frozenset(
        {
            BillingStatus.VOIDED,
            BillingStatus.REFUND_PENDING,
            BillingStatus.REFUNDED,
            BillingStatus.NO_REFUND,
        }
    ),
    "visit": frozenset({BillingStatus.FINALIZED}),
}

# States on which a reschedule re-link is meaningful (D-14): a live, paid-for
# booking. Anything else is a 409.
RELINKABLE: frozenset[str] = frozenset({BillingStatus.VERIFIED})

RELINK_REASON = "rescheduled"


def can_transition(current: str, target: str) -> bool:
    return target in ALLOWED_TRANSITIONS.get(current, frozenset())
