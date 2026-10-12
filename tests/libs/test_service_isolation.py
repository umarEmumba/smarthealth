"""Assumption-delta invariant suite for the microservice split (plan 01.1-09).

Plan 01.1-01 recorded the assumption delta: Phase 1's single-database monolith
was PROMOTED to database-per-service with an Identity-owned RS256 keypair (not
"added alongside"). These tests turn that decision into regression tests: they
go red the moment a later phase reintroduces a shared database, a shared
base/metadata, a cross-service import, an unprefixed database setting, a shared
symmetric secret, or PHI / private-key handling outside its owning service.

Everything except `test_database_roles_are_isolated` is pure static analysis
(metadata, AST, settings introspection) and needs no running containers.
The role-isolation test is an integration test against the live Postgres.
"""

from __future__ import annotations

import ast
import importlib
import re
from pathlib import Path

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.config import SharedSettings
from services.identity.config import IdentitySettings
from services.identity.repository.models import IdentityBase
from services.organization.config import OrganizationSettings
from services.organization.repository.models import OrganizationBase
from services.patient.config import PatientSettings
from services.patient.repository.models import PatientBase
from services.provider.config import ProviderSettings
from services.provider.repository.models import ProviderBase
from services.scheduling.config import SchedulingSettings
from services.scheduling.repository.models import SchedulingBase

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICES_DIR = REPO_ROOT / "services"
LIBS_DIR = REPO_ROOT / "libs"

OWNED_TABLES: dict[str, set[str]] = {
    "identity": {"users"},
    "organization": {"clinics", "departments"},
    "provider": {"providers", "specialties", "provider_specialties", "availability_templates"},
    "patient": {"patients"},
    "scheduling": {"provider_slots"},
}

BASES = {
    "identity": IdentityBase,
    "organization": OrganizationBase,
    "provider": ProviderBase,
    "patient": PatientBase,
    "scheduling": SchedulingBase,
}

SETTINGS = {
    "identity": IdentitySettings,
    "organization": OrganizationSettings,
    "provider": ProviderSettings,
    "patient": PatientSettings,
    "scheduling": SchedulingSettings,
}

# Every deployable service package, including the gateway (which owns no data
# but must still not import another service: it only ever speaks HTTP).
ALL_SERVICE_PACKAGES = (*OWNED_TABLES, "gateway")

PHI_COLUMNS = {"date_of_birth", "phone", "address"}


def _py_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)


def _module_name(path: Path) -> str:
    rel = path.relative_to(REPO_ROOT).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _imported_modules(path: Path) -> set[str]:
    """Absolute dotted names a file imports (relative imports are resolved)."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package = _module_name(path)
    if path.name != "__init__.py":
        package = package.rpartition(".")[0]
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base_parts = package.split(".")
                base_parts = base_parts[: len(base_parts) - (node.level - 1)]
                base = ".".join(base_parts)
                module = f"{base}.{node.module}" if node.module else base
            else:
                module = node.module or ""
            found.add(module)
            # `from services import identity` imports the package services.identity
            found.update(f"{module}.{alias.name}" for alias in node.names)
    return found


def _service_of(module: str) -> str | None:
    """`services.<name>[.x]` -> name, else None."""
    parts = module.split(".")
    if len(parts) >= 2 and parts[0] == "services":
        return parts[1]
    return None


def test_metadata_owns_exactly_its_tables() -> None:
    for service, base in BASES.items():
        assert set(base.metadata.tables) == OWNED_TABLES[service], service


def test_no_foreign_key_crosses_a_service() -> None:
    for service, base in BASES.items():
        for table in base.metadata.tables.values():
            for fk in table.foreign_keys:
                target_table = fk.target_fullname.split(".")[-2]
                assert target_table in OWNED_TABLES[service], (
                    f"{service}.{table.name} has a foreign key to {fk.target_fullname}, "
                    "which belongs to a different service"
                )


def test_no_cross_service_imports() -> None:
    violations: list[str] = []
    for service in ALL_SERVICE_PACKAGES:
        for path in _py_files(SERVICES_DIR / service):
            for module in _imported_modules(path):
                other = _service_of(module)
                if other is not None and other != service:
                    violations.append(f"{path.relative_to(REPO_ROOT)} imports {module}")
    for path in _py_files(LIBS_DIR):
        for module in _imported_modules(path):
            if module == "services" or module.startswith("services."):
                violations.append(f"{path.relative_to(REPO_ROOT)} imports {module}")
    assert not violations, violations


def _all_settings_subclasses(cls: type) -> list[type]:
    result: list[type] = []
    for sub in cls.__subclasses__():
        result.append(sub)
        result.extend(_all_settings_subclasses(sub))
    return result


def test_database_url_is_service_prefixed() -> None:
    assert "database_url" not in SharedSettings.model_fields
    for service, settings_cls in SETTINGS.items():
        field = settings_cls.model_fields["database_url"]
        assert field.validation_alias == f"{service.upper()}_DATABASE_URL", service

    # Every settings class anywhere (gateway included) may only read a database
    # URL through its OWN service-prefixed alias.
    importlib.import_module("services.gateway.config")
    for settings_cls in _all_settings_subclasses(SharedSettings):
        owner = settings_cls.__module__.split(".")[1] if settings_cls.__module__.startswith("services.") else None
        for name, field in settings_cls.model_fields.items():
            alias = field.validation_alias if isinstance(field.validation_alias, str) else ""
            if "database" in name.lower() or "database_url" in alias.lower():
                assert owner is not None and alias == f"{owner.upper()}_DATABASE_URL", (
                    f"{settings_cls.__name__}.{name} reads {alias or name!r}"
                )


class _CodeNames(ast.NodeVisitor):
    """Collect identifiers and non-docstring string literals: the 'live code'
    surface. Comments and docstrings may legitimately explain why a symmetric
    algorithm is NOT used, so they are not scanned."""

    def __init__(self) -> None:
        self.names: list[str] = []

    def _skip_docstring(self, node: ast.AST) -> list[ast.stmt]:
        body = list(getattr(node, "body", []))
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            return body[1:]
        return body

    def visit_Module(self, node: ast.Module) -> None:
        for child in self._skip_docstring(node):
            self.visit(child)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.names.append(node.name)
        for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]:
            self.names.append(arg.arg)
        for child in self._skip_docstring(node):
            self.visit(child)

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.names.append(node.name)
        for child in self._skip_docstring(node):
            self.visit(child)

    def visit_Name(self, node: ast.Name) -> None:
        self.names.append(node.id)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self.names.append(node.attr)
        self.generic_visit(node)

    def visit_keyword(self, node: ast.keyword) -> None:
        if node.arg:
            self.names.append(node.arg)
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self.names.append(node.value)


_SYMMETRIC_ALG = re.compile(r"HS(256|384|512)")
_SHARED_SECRET_NAME = re.compile(r"jwt_secret", re.IGNORECASE)


def test_no_shared_symmetric_jwt_config() -> None:
    hits: list[str] = []
    for root in (LIBS_DIR, SERVICES_DIR):
        for path in _py_files(root):
            visitor = _CodeNames()
            visitor.visit(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
            for name in visitor.names:
                if _SYMMETRIC_ALG.search(name) or _SHARED_SECRET_NAME.search(name):
                    hits.append(f"{path.relative_to(REPO_ROOT)}: {name!r}")
    assert not hits, hits
    for settings_cls in [SharedSettings, *SETTINGS.values()]:
        for field_name in settings_cls.model_fields:
            assert not _SHARED_SECRET_NAME.search(field_name), (settings_cls.__name__, field_name)


def test_phi_columns_only_in_patient() -> None:
    for service, base in BASES.items():
        columns = {c.name for table in base.metadata.tables.values() for c in table.columns}
        if service == "patient":
            assert PHI_COLUMNS <= columns
        else:
            assert not (PHI_COLUMNS & columns), f"{service} holds PHI columns {PHI_COLUMNS & columns}"


def test_private_key_loading_only_in_identity() -> None:
    pattern = re.compile(r"\b(load_signing_key|private_pem)\b")
    hits: list[str] = []
    for root in (LIBS_DIR, SERVICES_DIR):
        for path in _py_files(root):
            rel = path.relative_to(REPO_ROOT)
            if rel.parts[:2] == ("services", "identity"):
                continue
            if pattern.search(path.read_text(encoding="utf-8")):
                hits.append(str(rel))
    assert not hits, hits


@pytest.mark.integration
@pytest.mark.parametrize("service", sorted(OWNED_TABLES))
async def test_database_roles_are_isolated(service: str) -> None:
    """A service's role connects to its own database and is refused by every
    other service's database (REVOKE ALL ON DATABASE ... FROM PUBLIC)."""
    own_url = make_url(SETTINGS[service]().database_url)

    engine = create_async_engine(own_url.set(database=f"{service}_db"))
    try:
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT 1"))).scalar_one() == 1
    finally:
        await engine.dispose()

    for other in sorted(set(OWNED_TABLES) - {service}):
        cross = create_async_engine(own_url.set(database=f"{other}_db"))
        try:
            # asyncpg's InsufficientPrivilegeError surfaces unwrapped from the
            # pool's first connect, so catch broadly and then pin the reason.
            with pytest.raises(Exception) as excinfo:
                async with cross.connect():
                    pass
            assert "permission denied" in str(excinfo.value).lower(), (
                f"{service}_svc -> {other}_db refused for an unexpected reason"
            )
        finally:
            await cross.dispose()
