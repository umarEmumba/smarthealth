"""Visit's request-scoped database session dependency (visit_db only)."""

from __future__ import annotations

from libs.db.postgres import make_session_dependency
from services.visit.config import settings

get_db = make_session_dependency(lambda: settings.database_url)
