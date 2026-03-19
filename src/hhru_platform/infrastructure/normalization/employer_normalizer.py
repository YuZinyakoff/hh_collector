from __future__ import annotations

from hhru_platform.application.dto import NormalizedEmployerReference


class EmployerNormalizationError(ValueError):
    """Raised when an employer payload does not match the expected shape."""


def normalize_employer_reference(payload: object) -> NormalizedEmployerReference | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise EmployerNormalizationError("employer must be an object")

    hh_employer_id = _optional_string(payload.get("id"))
    if hh_employer_id is None:
        return None

    return NormalizedEmployerReference(
        hh_employer_id=hh_employer_id,
        name=_optional_string(payload.get("name")),
        alternate_url=_optional_string(payload.get("alternate_url")),
        site_url=_optional_string(payload.get("site_url")),
        area_hh_id=_lookup_id(payload.get("area")),
        is_trusted=_optional_bool(payload.get("trusted")),
    )


def _lookup_id(payload: object) -> str | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise EmployerNormalizationError("employer area must be an object")
    return _optional_string(payload.get("id"))


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped_value = value.strip()
        return stripped_value or None
    raise EmployerNormalizationError("expected string-compatible value")


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    raise EmployerNormalizationError("expected boolean-compatible value")
