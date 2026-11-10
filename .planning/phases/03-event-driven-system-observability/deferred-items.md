# Phase 3 deferred items (out of scope for the plan that found them)

## Found in plan 03-01

- `tests/provider/test_provider_tracer.py::test_admin_registers_provider_through_gateway` fails
  deterministically in the full `-m "not e2e"` run and in isolation (3 of 3 re-runs). The POST response
  lists `specialties` in one order and the GET returns the other, so `assert fetched.json() == provider`
  fails. The two specialty rows ("Tracer Cardiology", "Tracer Internal Medicine") persist in the live
  provider_db between runs, and the association load has no explicit ORDER BY, so the order depends on
  stored row order. Plan 03-01 touches no provider code (changes: `pyproject.toml`, `libs/config.py`,
  new `libs/events/`, new `tests/events/`), so the failure is not caused by it. Not fixed (scope
  boundary). Suggested fix: order the specialty association load (for example by name) in the provider
  repository, or compare sets in the test.
