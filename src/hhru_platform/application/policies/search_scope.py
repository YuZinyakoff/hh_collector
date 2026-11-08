from __future__ import annotations

from collections.abc import Mapping


class SearchAreaScopeError(ValueError):
    """The response does not confirm the complete requested area filter."""


def validate_search_area_scope(*, request_params: Mapping[str, object], payload: object) -> None:
    if request_params.get("area") is None:
        return

    requested_areas = _area_values(request_params["area"])
    if not isinstance(payload, dict) or not isinstance(payload.get("arguments"), list):
        raise SearchAreaScopeError(
            "vacancy search area scope is unconfirmed: response arguments are missing or invalid"
        )

    applied_areas: set[str] = set()
    for argument in payload["arguments"]:
        if not isinstance(argument, dict):
            raise SearchAreaScopeError("vacancy search response contains an invalid argument")
        if argument.get("argument") == "area":
            applied_areas.update(_area_values(argument.get("value")))

    if applied_areas != requested_areas:
        raise SearchAreaScopeError(
            "vacancy search area scope differs from request: "
            f"requested={sorted(requested_areas)!r} applied={sorted(applied_areas)!r}"
        )


def _area_values(value: object) -> set[str]:
    values = value if isinstance(value, list | tuple) else (value,)
    result: set[str] = set()
    for item in values:
        if isinstance(item, bool) or not isinstance(item, str | int) or not str(item).strip():
            raise SearchAreaScopeError("vacancy search area filter contains an invalid value")
        result.add(str(item).strip())
    if not result:
        raise SearchAreaScopeError("vacancy search area filter is empty")
    return result
