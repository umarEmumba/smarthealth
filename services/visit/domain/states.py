"""Visit states and their strict forward order (D-15, VISIT-02/03).

`VisitStatus` is a plain class of `str` constants, NOT a `(str, Enum)`: plan
02-11 carries these values through Temporal workflow I/O, where plain strings
round-trip through the JSON converter intact (research Pitfall 1).

Lifecycle (forward only, no state is ever re-entered):

    checked_in ──> in_progress ──> completed

Rules a transition request is judged by (`transition_visit`):
  * the target is the current state or one already passed -> a no-op (200);
  * the target is the NEXT state -> applied (conditional UPDATE + history row);
  * the target is further ahead (a skipped state) -> refused (409).
"""

from __future__ import annotations


class VisitStatus:
    CHECKED_IN = "checked_in"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"

    ALL = ("checked_in", "in_progress", "completed")


# Position of each state in the one-way lifecycle.
ORDER: dict[str, int] = {"checked_in": 0, "in_progress": 1, "completed": 2}

# The only state each target may be entered from.
PREVIOUS: dict[str, str] = {"in_progress": "checked_in", "completed": "in_progress"}


class TransitionOutcome:
    APPLIED = "applied"
    NOOP = "noop"
    SKIPPED = "skipped"
