# SmartHealth Organization service image (build context = repository root).
#
#   docker build -f services/organization/Dockerfile .
#
# One image per service: it carries ONLY the shared `libs/` package and this
# service's own package — never another service's code, never `infra/`, never
# any key material (.dockerignore keeps `infra/secrets` and `*.pem` out of the
# build context altogether). Base: python:3.12-slim, as in Phase 1.
FROM python:3.12-slim

WORKDIR /app

# System deps needed to build wheels for asyncpg / cryptography / bcrypt on slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY libs ./libs
COPY services/__init__.py ./services/__init__.py
COPY services/organization ./services/organization

RUN pip install --no-cache-dir .

# Migrations run before the server starts, so a container never serves traffic
# against a schema that is behind its code. gunicorn with the uvicorn worker
# class per CLAUDE.md; async workers, so the count is NOT the sync-WSGI formula.
CMD ["sh", "-c", "alembic -c services/organization/migrations/alembic.ini upgrade head && exec gunicorn -k uvicorn.workers.UvicornWorker -w ${WEB_CONCURRENCY:-2} -b 0.0.0.0:8000 services.organization.app:app"]
