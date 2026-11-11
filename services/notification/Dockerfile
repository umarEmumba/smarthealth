# SmartHealth Notification service image (build context = repository root).
#
#   docker build -f services/notification/Dockerfile .
#
# One image, four Compose roles (plan 03-14): the Celery delivery worker (this CMD),
# the notification-trigger Kafka consumer, the notification Temporal worker and the
# `rabbitmq-init` provisioning job (each overrides `command`). It carries ONLY the
# shared `libs/` package and this service's own package: never another service's code,
# never `infra/`, never any key material (.dockerignore keeps `infra/secrets` and
# `*.pem` out of the build context altogether). Base: python:3.12-slim, as in Phase 1.
# The service owns no Postgres database, so nothing here runs migrations.
FROM python:3.12-slim

WORKDIR /app

# System deps needed to build wheels for cryptography / bcrypt on slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY libs ./libs
COPY services/__init__.py ./services/__init__.py
COPY services/notification ./services/notification

RUN pip install --no-cache-dir .

# The Celery worker command of plan 03-02: threads pool, the three work queues, and
# mingle/gossip/heartbeat off (required on RabbitMQ 4.3: Celery 5.6.3 refuses to start
# with its defaults because of transient non-exclusive queues).
CMD ["celery", "-A", "services.notification.celery_app", "worker", "-P", "threads", "-c", "8", \
     "-Q", "notifications.booking,notifications.reminder,notifications.followup", \
     "--without-mingle", "--without-gossip", "--without-heartbeat", "-l", "INFO"]
