"""Async repository for providers, specialties and weekly availability templates
(Provider service, provider_db).

Every provider is reached through its own id or its `department_id`, which is a
bare id owned by Organization: this module never looks a department up and holds
no department or clinic data (D-01, D-07). Callers (the routers) validate the
department and the account reference with their owning services BEFORE calling
the write functions here (D-05).

Nothing in this module touches another service's database or a concrete-slot
table: availability is stored as weekly templates only (Phase 1 Decision 2).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, time, timezone as dt_timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from services.provider.repository.models import AvailabilityTemplate, Provider, Specialty


async def get_specialties_by_ids(
    db: AsyncSession, specialty_ids: list[uuid.UUID]
) -> list[Specialty]:
    if not specialty_ids:
        return []
    result = await db.execute(select(Specialty).where(Specialty.id.in_(specialty_ids)))
    return list(result.scalars().all())


async def get_or_create_specialty(db: AsyncSession, name: str) -> Specialty:
    """Look up a specialty by name, creating it if it doesn't exist yet.

    Specialties have no admin-facing "manage specialties" endpoint (PPL-02 only
    requires a provider to carry >=1 specialty), so get-or-create by name keeps
    provider registration self-contained.

    Race-free: the insert is `ON CONFLICT (name) DO NOTHING`, so two concurrent
    creations introducing the same new name cannot make two rows nor raise a
    unique violation — `specialties.name UNIQUE` is the backstop and the select
    that follows sees the winner's row. Nothing is committed here; the caller's
    transaction commits the specialty together with whatever it is attached to.
    """
    await db.execute(
        pg_insert(Specialty).values(name=name).on_conflict_do_nothing(index_elements=["name"])
    )
    result = await db.execute(select(Specialty).where(Specialty.name == name))
    return result.scalar_one()


async def _specialties_for(db: AsyncSession, names: list[str]) -> list[Specialty]:
    """Specialties for `names` (get-or-create), de-duplicated in order so a
    repeated name cannot attach the same specialty to one provider twice."""
    return [await get_or_create_specialty(db, name) for name in dict.fromkeys(names)]


async def create_provider(
    db: AsyncSession,
    *,
    department_id: uuid.UUID,
    full_name: str,
    specialty_names: list[str],
    user_id: uuid.UUID | None = None,
) -> Provider:
    """Create a provider under `department_id`, attaching one-or-more
    specialties by name (get-or-create, see `get_or_create_specialty`).

    The department is NOT checked here: it belongs to Organization and the router
    has already validated it (D-05). Always returns the provider re-selected with
    `specialties` eagerly loaded — touching the lazy relationship after commit in
    an async session raises `MissingGreenlet`, and a fresh SELECT with
    `selectinload` is the correct async-ORM pattern, not a workaround.
    """
    specialties = await _specialties_for(db, specialty_names)

    provider = Provider(
        department_id=department_id,
        full_name=full_name,
        user_id=user_id,
        specialties=specialties,
    )
    db.add(provider)
    await db.commit()

    created = await get_provider(db, provider.id)
    assert created is not None  # just committed
    return created


async def get_provider(db: AsyncSession, provider_id: uuid.UUID) -> Provider | None:
    result = await db.execute(
        select(Provider)
        .where(Provider.id == provider_id)
        .options(selectinload(Provider.specialties))
    )
    return result.scalar_one_or_none()


async def list_providers(
    db: AsyncSession,
    department_id: uuid.UUID | None = None,
    *,
    include_inactive: bool = False,
) -> list[Provider]:
    """List providers, optionally scoped to a single department.

    `department_id=None` lists across all departments — reserved for
    admin-only call sites (the provider router guards writes to ADMIN;
    a future cross-department admin listing view could use this, but
    this plan's router always passes an explicit `department_id`).
    """
    stmt = select(Provider).options(selectinload(Provider.specialties)).order_by(
        Provider.created_at
    )
    if department_id is not None:
        stmt = stmt.where(Provider.department_id == department_id)
    if not include_inactive:
        stmt = stmt.where(Provider.is_active.is_(True))
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def update_provider(
    db: AsyncSession,
    provider_id: uuid.UUID,
    *,
    full_name: str | None = None,
    specialty_names: list[str] | None = None,
) -> Provider | None:
    """Partial-update a provider. `specialty_names`, when supplied, REPLACES
    the provider's full specialty set (get-or-create by name, same as
    `create_provider` — no separate specialty-management endpoint exists).
    Returns `None` if `provider_id` does not exist.
    """
    provider = await get_provider(db, provider_id)
    if provider is None:
        return None

    if full_name is not None:
        provider.full_name = full_name
    if specialty_names is not None:
        provider.specialties = await _specialties_for(db, specialty_names)

    await db.commit()

    # Re-select with `specialties` eagerly loaded — see create_provider's
    # docstring for why a post-commit lazy-load is unsafe here.
    return await get_provider(db, provider.id)


async def deactivate_provider(db: AsyncSession, provider_id: uuid.UUID) -> Provider | None:
    """Soft-delete a provider. Idempotent — deactivating an already-inactive
    provider is a no-op success.
    """
    provider = await get_provider(db, provider_id)
    if provider is None:
        return None

    if provider.is_active:
        provider.is_active = False
        provider.deactivated_at = datetime.now(dt_timezone.utc)
        await db.commit()
        provider = await get_provider(db, provider_id)
    return provider


async def create_availability_template(
    db: AsyncSession,
    *,
    provider_id: uuid.UUID,
    department_id: uuid.UUID,
    day_of_week: int,
    start_time,
    end_time,
    slot_duration_minutes: int,
    effective_from: date,
    effective_to: date | None = None,
) -> AvailabilityTemplate:
    """Persist a weekly availability template row.

    Validation (day_of_week range, end_time > start_time, positive
    duration, effective_from <= effective_to) happens in the router's
    Pydantic schema BEFORE this is called — this function trusts its
    caller and only persists (RESEARCH Decision 2: templates only, no
    concrete provider_slots rows are generated here).
    """
    template = AvailabilityTemplate(
        provider_id=provider_id,
        department_id=department_id,
        day_of_week=day_of_week,
        start_time=start_time,
        end_time=end_time,
        slot_duration_minutes=slot_duration_minutes,
        effective_from=effective_from,
        effective_to=effective_to,
    )
    db.add(template)
    await db.commit()
    await db.refresh(template)
    return template


async def list_availability(
    db: AsyncSession, provider_id: uuid.UUID, *, include_inactive: bool = False
) -> list[AvailabilityTemplate]:
    stmt = select(AvailabilityTemplate).where(AvailabilityTemplate.provider_id == provider_id)
    if not include_inactive:
        stmt = stmt.where(AvailabilityTemplate.is_active.is_(True))
    stmt = stmt.order_by(AvailabilityTemplate.day_of_week, AvailabilityTemplate.start_time)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def get_availability_template(
    db: AsyncSession, template_id: uuid.UUID, provider_id: uuid.UUID
) -> AvailabilityTemplate | None:
    """Fetch a single availability template scoped to `provider_id` — the
    provider-ownership PATCH/deactivate routes need to load-then-check
    ownership on the parent provider, so this stays id+provider scoped
    rather than id-only (mirrors the clinic/department scoping pattern).
    """
    result = await db.execute(
        select(AvailabilityTemplate).where(
            AvailabilityTemplate.id == template_id,
            AvailabilityTemplate.provider_id == provider_id,
        )
    )
    return result.scalar_one_or_none()


async def update_availability_template(
    db: AsyncSession,
    template_id: uuid.UUID,
    provider_id: uuid.UUID,
    *,
    day_of_week: int | None = None,
    start_time: time | None = None,
    end_time: time | None = None,
    slot_duration_minutes: int | None = None,
    effective_from: date | None = None,
    effective_to: date | None = None,
) -> AvailabilityTemplate | None:
    """Partial-update an availability template, scoped to `provider_id`.

    Window-consistency validation (end after start, effective_from <=
    effective_to) against the RESULTING row (existing values merged with
    the supplied partial update) happens in the router BEFORE this is
    called — mirrors `create_availability_template`'s trust-the-caller
    contract. Returns `None` if the template does not exist or does not
    belong to `provider_id`.
    """
    template = await get_availability_template(db, template_id, provider_id)
    if template is None:
        return None

    if day_of_week is not None:
        template.day_of_week = day_of_week
    if start_time is not None:
        template.start_time = start_time
    if end_time is not None:
        template.end_time = end_time
    if slot_duration_minutes is not None:
        template.slot_duration_minutes = slot_duration_minutes
    if effective_from is not None:
        template.effective_from = effective_from
    if effective_to is not None:
        template.effective_to = effective_to

    await db.commit()
    await db.refresh(template)
    return template


async def deactivate_availability_template(
    db: AsyncSession, template_id: uuid.UUID, provider_id: uuid.UUID
) -> AvailabilityTemplate | None:
    """Soft-delete an availability template, scoped to `provider_id`.
    Idempotent — deactivating an already-inactive template is a no-op
    success.
    """
    template = await get_availability_template(db, template_id, provider_id)
    if template is None:
        return None

    if template.is_active:
        template.is_active = False
        template.deactivated_at = datetime.now(dt_timezone.utc)
        await db.commit()
        await db.refresh(template)
    return template
