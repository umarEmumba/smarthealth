# SmartHealth API image.
# Base: python:3.12-slim per CLAUDE.md-mandated runtime.
FROM python:3.12-slim

WORKDIR /app

# System deps needed to build wheels for asyncpg / cryptography / bcrypt on slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY smarthealth ./smarthealth
COPY libs ./libs
COPY services ./services

RUN pip install --no-cache-dir .

# gunicorn with the uvicorn worker class per CLAUDE.md; worker count = CPU cores
# (not the (2*cores)+1 sync-WSGI formula — uvicorn workers are async).
# `services.identity.api.main:app` is the FastAPI app entrypoint added in a later plan;
# this Dockerfile is scaffolding for OBS-04 and is not expected to serve traffic yet.
CMD ["sh", "-c", "gunicorn -k uvicorn.workers.UvicornWorker -w ${WEB_CONCURRENCY:-4} -b 0.0.0.0:8000 smarthealth.main:app"]
