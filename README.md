# SmartHealth — Intelligent Healthcare Operations Platform (Part A)

MediNova's foundational healthcare-operations backend: a production-grade, distributed
platform for patient/provider management, appointment scheduling, visit workflows,
billing pre-checks, notifications, and operational analytics.

This is Part A — the core platform. See `.planning/PROJECT.md` for full context, and
`docs/architecture-dashboard.html` for the living architecture & decision record.

> This README is a placeholder scaffolded in Phase 1, Plan 1. The full README (overview,
> architecture diagram, setup, API overview, stack) is a project deliverable completed
> incrementally as each phase lands — see `.planning/ROADMAP.md`.

## Local Development

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
cp infra/.env.example infra/.env

docker compose -f infra/docker-compose.yml up -d
scripts/compose_healthcheck.sh

pytest tests/ -x -k "not integration"
```
