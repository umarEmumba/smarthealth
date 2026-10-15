# SmartHealth API gateway image (build context = repository root).
#
#   docker build -f services/gateway/Dockerfile .
#
# The gateway is the only externally published application port. It owns no
# database, so there is no Alembic step. The image carries ONLY the shared
# `libs/` package and the gateway's own package — never another service's code,
# never `infra/`, never any key material (.dockerignore keeps `infra/secrets`
# and `*.pem` out of the build context altogether).
FROM python:3.12-slim

WORKDIR /app

# System deps needed to build wheels for asyncpg / cryptography / bcrypt on slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY libs ./libs
COPY services/__init__.py ./services/__init__.py
COPY services/gateway ./services/gateway

RUN pip install --no-cache-dir .

# gunicorn with the uvicorn worker class per CLAUDE.md; async workers, so the
# count is NOT the sync-WSGI formula. Two listeners, one app: :8000 is the public
# one (the only port Compose publishes); :9100 serves /metrics to Prometheus on the
# internal network only. The app decides by the socket a request arrived on.
CMD ["sh", "-c", "exec gunicorn -k uvicorn.workers.UvicornWorker -w ${WEB_CONCURRENCY:-2} -b 0.0.0.0:8000 -b 0.0.0.0:9100 services.gateway.app:app"]
