"""Shared observability wiring: OpenTelemetry tracing + structlog logging.

Both modules are configured once at app startup (`services/identity/app.py`)
and are PHI-safe by construction — see `otel.py` and `logging.py` docstrings.
"""
