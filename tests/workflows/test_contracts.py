"""Workflow contracts stay ids-only plain strings (plan 02-06; D-06, T-02-27,
research Pitfall 1).

Every dataclass in `services.scheduling.domain.contracts` must round-trip
through Temporal's default data converter unchanged, must not use an Enum type
(a str-subclass Enum comes back as a list of characters), and must not carry a
field whose name suggests PHI, coverage data or credentials: workflow histories
are durable and operator-visible.
"""

from __future__ import annotations

import ast
import dataclasses
import enum
import inspect
import typing

import pytest
from temporalio.converter import DataConverter

from services.scheduling.domain import contracts
from services.visit.domain import contracts as visit_contracts

FORBIDDEN_FIELD_FRAGMENTS = ("coverage", "token", "jwt", "phone", "address", "date_of_birth")
ALLOWED_IMPORTS = {"__future__", "dataclasses", "typing"}


# Plan 02-11: Visit's contracts (services.visit.domain.contracts) obey the same rules.
CONTRACT_MODULES = (contracts, visit_contracts)


def contract_classes() -> list[type]:
    return [
        obj
        for module in CONTRACT_MODULES
        for _, obj in inspect.getmembers(module, inspect.isclass)
        if dataclasses.is_dataclass(obj) and obj.__module__ == module.__name__
    ]


# Plain JSON scalars only: str (ids, outcomes, reasons) and int (counts,
# plan 02-07 `MaintenanceResult.processed`).
PLAIN_SCALARS = (str, int)


def sample(cls: type) -> object:
    hints = typing.get_type_hints(cls)
    values = {
        f.name: (7 if hints[f.name] is int else f"{f.name}-value") for f in dataclasses.fields(cls)
    }
    return cls(**values)


def test_contract_module_defines_the_booking_contracts() -> None:
    names = {cls.__name__ for cls in contract_classes()}
    assert {"BookingInput", "BookingResult", "MarkFailedInput"} <= names


@pytest.mark.parametrize("cls", contract_classes(), ids=lambda c: c.__name__)
async def test_every_contract_round_trips_through_the_default_converter(cls: type) -> None:
    original = sample(cls)
    converter = DataConverter.default

    payloads = await converter.encode([original])
    (decoded,) = await converter.decode(payloads, [cls])

    assert decoded == original
    assert type(decoded) is cls
    hints = typing.get_type_hints(cls)
    for field in dataclasses.fields(cls):
        declared = hints[field.name]
        expected = declared if declared in PLAIN_SCALARS else str
        assert type(getattr(decoded, field.name)) is expected


async def test_optional_reason_round_trips_as_none() -> None:
    converter = DataConverter.default
    payloads = await converter.encode([contracts.BookingResult(outcome="confirmed")])
    (decoded,) = await converter.decode(payloads, [contracts.BookingResult])
    assert decoded == contracts.BookingResult(outcome="confirmed", reason=None)


@pytest.mark.parametrize("cls", contract_classes(), ids=lambda c: c.__name__)
def test_no_contract_field_is_an_enum(cls: type) -> None:
    hints = typing.get_type_hints(cls)
    for name, hint in hints.items():
        candidates = list(typing.get_args(hint)) or [hint]
        for candidate in candidates:
            assert not (inspect.isclass(candidate) and issubclass(candidate, enum.Enum)), (
                f"{cls.__name__}.{name} is an Enum"
            )
            assert candidate in (*PLAIN_SCALARS, type(None)), (
                f"{cls.__name__}.{name} is not a plain str/int"
            )


@pytest.mark.parametrize("cls", contract_classes(), ids=lambda c: c.__name__)
def test_no_contract_field_carries_phi_coverage_or_credentials(cls: type) -> None:
    for field in dataclasses.fields(cls):
        lowered = field.name.lower()
        assert not [frag for frag in FORBIDDEN_FIELD_FRAGMENTS if frag in lowered], (
            f"{cls.__name__}.{field.name} must not travel through Temporal"
        )


@pytest.mark.parametrize("module", CONTRACT_MODULES, ids=lambda m: m.__name__)
def test_contracts_module_is_pure(module: object) -> None:
    """Sandbox-safe: only dataclasses/typing imports, so workflow code can load it."""
    tree = ast.parse(inspect.getsource(module))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported <= ALLOWED_IMPORTS


def test_booking_workflow_id_is_deterministic() -> None:
    assert contracts.booking_workflow_id("abc") == "booking-abc"


def test_visit_completed_workflow_id_is_deterministic() -> None:
    assert visit_contracts.visit_completed_workflow_id("abc") == "visit-completed-abc"
