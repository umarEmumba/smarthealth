"""Analytics' repository layer: the ORM mirror of the projection tables."""

from services.analytics.repository.models import (
    AnalyticsBase,
    Appointment,
    Patient,
    VisitCompleted,
)

__all__ = ["AnalyticsBase", "Appointment", "Patient", "VisitCompleted"]
