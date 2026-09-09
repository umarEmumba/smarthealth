"""Shared Mongo audit-write helpers (who/what/when schema, PHI-scrubbing).

Phase 1 provides index setup only (`mongo_indexes.py`); the write helper
itself (used by every mutating route to satisfy IAM-04) is added in Plan 03.
"""
