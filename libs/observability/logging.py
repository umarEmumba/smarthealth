"""structlog configuration: JSON logs correlated to OTel traces, PHI-safe.

PHI-safe convention (enforced by discipline, not automated redaction — see
RESEARCH's PHI-Safe Structured Logging section): every log call in this
codebase MUST log identifiers only (user_id, patient_id, clinic_id, jti,
trace_id) and MUST NEVER pass a PHI field value (patient full_name,
date_of_birth, phone, address) as a log event kwarg. Full JWT token contents
are never logged — only `jti`/`sub`.

`add_trace_context` reads the *active* OTel span context (not a static
string) so log/trace correlation never breaks when span context changes
shape mid-request.
"""

from __future__ import annotations

import logging
import sys

import structlog
from opentelemetry import trace


def add_trace_context(
    logger: object, method_name: str, event_dict: dict[str, object]
) -> dict[str, object]:
    """structlog processor: inject trace_id/span_id from the active OTel span."""
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


_LOGGING_CONFIGURED = False


def configure_logging(level: int = logging.INFO) -> None:
    """Idempotently configure structlog + stdlib logging for JSON output.

    Safe to call more than once (app factory may run multiple times across
    tests) — subsequent calls are a no-op.
    """
    global _LOGGING_CONFIGURED  # noqa: PLW0603

    if _LOGGING_CONFIGURED:
        return

    logging.basicConfig(stream=sys.stdout, level=level, format="%(message)s")

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            add_trace_context,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _LOGGING_CONFIGURED = True


def get_logger(*args: object, **kwargs: object) -> structlog.BoundLogger:
    """Return a structlog bound logger. Configures logging on first use if
    the app factory hasn't already done so (keeps standalone script/test use
    working without requiring the full app startup sequence).
    """
    if not _LOGGING_CONFIGURED:
        configure_logging()
    return structlog.get_logger(*args, **kwargs)
