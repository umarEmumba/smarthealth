"""Billing's request-scoped database session dependency (billing_db only)."""

from __future__ import annotations

from libs.db.postgres import make_session_dependency
from services.billing.config import settings

get_db = make_session_dependency(lambda: settings.database_url)
