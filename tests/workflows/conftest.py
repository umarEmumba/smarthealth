"""Fixtures for Temporal workflow tests (plan 02-06).

The Temporal fixtures this folder uses (`temporal_env`, `scheduling_worker`,
`booking_world`) live in `tests/temporal_fixtures.py`, registered from the root
`tests/conftest.py` via `pytest_plugins`, because `tests/scheduling/` needs them
too and pytest does not share a sibling folder's conftest fixtures.
"""
