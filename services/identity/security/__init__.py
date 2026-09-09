"""Server-side security dependencies: token validation (current_user) and
role-based access control (rbac). Both are FastAPI `Depends`-based guards —
RBAC is NEVER enforced client-side or only at router registration (RESEARCH
anti-patterns / this plan's prohibitions).
"""
