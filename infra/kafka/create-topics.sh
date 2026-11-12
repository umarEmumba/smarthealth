#!/usr/bin/env bash
# One-shot, idempotent topic creation (plan 03-05, D-06, Pitfall 17).
#
# Kafka auto-creates a topic with ONE partition and default retention the first
# time a producer (or Debezium) touches it, which would silently cap consumer
# parallelism and expire the replayable event history. Every topic is therefore
# created here with explicit partitions and unlimited retention (the outbox is
# the replay source; retention.ms=-1 keeps the topics replayable too).
#
#   - one event topic per aggregate (key = aggregate id keeps each aggregate's
#     events ordered inside one partition); `cdcprobe` is a test-only topic that
#     no consumer reads
#   - one dead-letter topic per consumer group (D-08)
#
# `--if-not-exists` makes a re-run (every `docker compose up`) a no-op. Any
# failure exits non-zero so `kafka-init` never reports success on a half-created set.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
PARTITIONS=3

TOPICS=(
  smarthealth.appointment.events
  smarthealth.visit.events
  smarthealth.provider.events
  smarthealth.patient.events
  smarthealth.cdcprobe.events
  smarthealth.dlq.visit-downstream
  smarthealth.dlq.notification-trigger
  smarthealth.dlq.analytics-projection
)

for topic in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1 \
    --config retention.ms=-1
  echo "topic ${topic} ready"
done
