"""Celery app for notification delivery on RabbitMQ 4.3 (plan 03-02, research Pattern 6).

Queues: `notifications.booking|reminder|followup` on the direct exchange
`notifications`, each dead-lettering to `smarthealth.dlx` with routing key
`notifications.dlq`; the DLQ `notifications.dlq` is bound to that durable direct
exchange. Classic queues (quorum queues with a delivery limit are the production
path, assumption A10).

Celery 5.6.3 refuses to start on RabbitMQ 4.3 with its defaults
(`transient_nonexcl_queues is deprecated`), so remote control, task events,
mingle, gossip and heartbeat are switched off. Worker command:

    celery -A services.notification.celery_app worker -P threads -c 8 \\
        -Q notifications.booking,notifications.reminder,notifications.followup \\
        --without-mingle --without-gossip --without-heartbeat

Importing this module reads no secret: the module-level `app` resolves the broker
URL (and so the password file) when a worker or publisher first needs it.
"""

from __future__ import annotations

import threading
from typing import Any

import prometheus_client
from celery import Celery
from celery.signals import worker_init
from kombu import Exchange, Queue

from libs.observability.logging import configure_logging, get_logger
from libs.observability.otel import configure_tracer_provider
from services.notification.config import settings
from services.notification.deliveries import ensure_notification_indexes, get_db

logger = get_logger()

NOTIFICATIONS_EXCHANGE = "notifications"
DEAD_LETTER_EXCHANGE = "smarthealth.dlx"
DEAD_LETTER_QUEUE = "notifications.dlq"
WORK_QUEUES = ("notifications.booking", "notifications.reminder", "notifications.followup")

_instrument_lock = threading.Lock()
_instrumented = False
_metrics_started = False


def build_queues() -> list[Queue]:
    exchange = Exchange(NOTIFICATIONS_EXCHANGE, type="direct", durable=True)
    arguments = {
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": DEAD_LETTER_QUEUE,
    }
    queues = [
        Queue(name, exchange, routing_key=name.split(".", 1)[1], queue_arguments=dict(arguments))
        for name in WORK_QUEUES
    ]
    queues.append(
        Queue(
            DEAD_LETTER_QUEUE,
            Exchange(DEAD_LETTER_EXCHANGE, type="direct", durable=True),
            routing_key=DEAD_LETTER_QUEUE,
        )
    )
    return queues


def create_celery_app(broker_url: str | None = None) -> Celery:
    """Build the notification Celery app. With no `broker_url` the URL is resolved
    lazily from the settings (see `ensure_broker_url`)."""
    app = Celery("smarthealth-notification", set_as_current=False)
    app.conf.update(
        task_default_queue="notifications.booking",
        task_default_exchange=NOTIFICATIONS_EXCHANGE,
        task_default_exchange_type="direct",
        task_default_routing_key="booking",
        task_queues=build_queues(),
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        # REQUIRED on RabbitMQ 4.3 (Pitfall 7).
        worker_enable_remote_control=False,
        worker_send_task_events=False,
        task_send_sent_event=False,
        result_backend=None,
        task_ignore_result=True,
        broker_connection_retry_on_startup=True,
        imports=("services.notification.tasks",),
    )
    if broker_url is not None:
        app.conf.broker_url = broker_url
    app._smarthealth_lazy_broker = broker_url is None  # type: ignore[attr-defined]
    return app


def ensure_broker_url(app: Celery) -> None:
    """Fill in the broker URL from the settings for an app built without one."""
    if getattr(app, "_smarthealth_lazy_broker", False):
        app.conf.broker_url = settings.broker_url
        app._smarthealth_lazy_broker = False  # type: ignore[attr-defined]


def instrument_publisher() -> None:
    """Idempotently install the Celery OpenTelemetry instrumentation (publishers and workers)."""
    global _instrumented  # noqa: PLW0603
    with _instrument_lock:
        if _instrumented:
            return
        from opentelemetry.instrumentation.celery import CeleryInstrumentor  # noqa: PLC0415

        CeleryInstrumentor().instrument()
        _instrumented = True


def declare_topology(app: Celery) -> None:
    """Declare the exchanges and queues (idempotent), so the dead-letter queue exists
    even though a worker started with `-Q` only declares the work queues."""
    ensure_broker_url(app)
    with app.connection_for_write() as connection:
        connection.ensure_connection(max_retries=10, interval_start=1, interval_step=1)
        channel = connection.default_channel
        for queue in app.conf.task_queues:
            queue.bind(channel).declare()


def _start_metrics_server() -> None:
    global _metrics_started  # noqa: PLW0603
    if _metrics_started:
        return
    try:
        prometheus_client.start_http_server(settings.metrics_port)
    except OSError:
        logger.warning("notification.metrics_server_unavailable", port=settings.metrics_port)
    _metrics_started = True


def _on_worker_init(sender: Any = None, **kwargs: Any) -> None:
    app = sender.app
    ensure_broker_url(app)
    configure_logging()
    configure_tracer_provider("smarthealth-notification-celery")
    instrument_publisher()
    ensure_notification_indexes(get_db())
    declare_topology(app)
    _start_metrics_server()


worker_init.connect(_on_worker_init, weak=False)

app = create_celery_app()
