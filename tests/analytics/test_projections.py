"""Projection tests for the analytics-projection consumer (plan 03-10).

`test_tracer_*`: a patient event produced to live Kafka (a unique topic and group per
test, so nothing reaches the real topics) is consumed by the real `EventConsumer`
running the real `handle_event`, lands in the live analytics_db and shows in
`v_active_patients` for a unique clinic.
"""

from __future__ import annotations

import datetime
import uuid
from collections.abc import Callable
from typing import Any

import pytest

from services.analytics.config import AnalyticsSettings
from tests.analytics.conftest import query, running_consumer, wait_for

pytestmark = pytest.mark.integration

T0 = datetime.datetime(2026, 11, 11, 9, 0, tzinfo=datetime.UTC)


def _active(clinic: uuid.UUID) -> int:
    rows = query(
        "SELECT active_patients FROM v_active_patients WHERE clinic_id = :c", c=clinic
    )
    return int(rows[0][0]) if rows else 0


def _registered(clinic: uuid.UUID, patient: uuid.UUID) -> dict[str, Any]:
    return {
        "aggregate_id": str(patient),
        "patient_id": str(patient),
        "clinic_id": str(clinic),
        "registered_at": T0,
    }


def _deactivated(clinic: uuid.UUID, patient: uuid.UUID) -> dict[str, Any]:
    return {
        "aggregate_id": str(patient),
        "patient_id": str(patient),
        "clinic_id": str(clinic),
        "deactivated_at": T0 + datetime.timedelta(hours=1),
    }


def test_tracer_registered_events_reach_v_active_patients(
    clinic_id: uuid.UUID,
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    patients = [uuid.uuid4(), uuid.uuid4()]
    with running_consumer(unique_topic, unique_group, dlq_topic):
        for patient in patients:
            produce_event(
                unique_topic,
                "patient.registered",
                _registered(clinic_id, patient),
                occurred_at=T0,
            )
        assert wait_for(lambda: _active(clinic_id), 2) == 2


def test_tracer_deactivation_applied_before_registration_still_lowers_the_count(
    clinic_id: uuid.UUID,
    unique_topic: str,
    dlq_topic: str,
    unique_group: str,
    produce_event: Callable[..., dict[str, Any]],
) -> None:
    first, second = uuid.uuid4(), uuid.uuid4()
    with running_consumer(unique_topic, unique_group, dlq_topic):
        # The deactivation of `first` is on the topic BEFORE its registration.
        produce_event(
            unique_topic, "patient.deactivated", _deactivated(clinic_id, first), occurred_at=T0
        )
        produce_event(
            unique_topic, "patient.registered", _registered(clinic_id, first), occurred_at=T0
        )
        produce_event(
            unique_topic, "patient.registered", _registered(clinic_id, second), occurred_at=T0
        )
        assert wait_for(lambda: _active(clinic_id), 1) == 1
        # The registration of `first` did not resurrect it: still one after a pause.
        assert wait_for(lambda: _active(clinic_id), 1, timeout=2) == 1


def test_tracer_settings_hold_exactly_one_database_url() -> None:
    fields = {
        name: field
        for name, field in AnalyticsSettings.model_fields.items()
        if "database" in name.lower() or "database" in str(field.validation_alias).lower()
    }
    assert set(fields) == {"database_url"}
    assert fields["database_url"].validation_alias == "ANALYTICS_DATABASE_URL"
