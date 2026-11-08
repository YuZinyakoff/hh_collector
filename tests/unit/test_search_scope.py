from __future__ import annotations

import pytest

from hhru_platform.application.policies.search_scope import (
    SearchAreaScopeError,
    validate_search_area_scope,
)


@pytest.mark.parametrize("area", ["1", 1, ["1", "2"], (1, 2), ["1", "1"]])
def test_area_scope_accepts_exact_applied_area_set(area: object) -> None:
    values = area if isinstance(area, list | tuple) else (area,)
    validate_search_area_scope(
        request_params={"area": area},
        payload={
            "arguments": [{"argument": "area", "value": str(item)} for item in reversed(values)]
            + [{"argument": "per_page", "value": "20"}],
            "items": [{"area": {"id": "2088"}, "published_at": "2026-11-01T12:04:59+0300"}],
        },
    )


@pytest.mark.parametrize("area", ["144", "17683", "1"])
@pytest.mark.parametrize("arguments", [None, [], [{"argument": "per_page", "value": "1"}]])
def test_area_scope_rejects_unconfirmed_area_even_for_empty_search(
    area: str, arguments: object
) -> None:
    with pytest.raises(SearchAreaScopeError):
        validate_search_area_scope(
            request_params={"area": area}, payload={"arguments": arguments, "items": []}
        )


@pytest.mark.parametrize(
    ("requested", "applied"),
    [("1", ["2"]), (["1", "2"], ["1"]), ("1", ["1", "2"])],
)
def test_area_scope_rejects_both_narrower_and_broader_filters(
    requested: object, applied: list[str]
) -> None:
    with pytest.raises(SearchAreaScopeError, match="differs from request"):
        validate_search_area_scope(
            request_params={"area": requested},
            payload={"arguments": [{"argument": "area", "value": item} for item in applied]},
        )


@pytest.mark.parametrize("value", ["", " ", True, None, {}, [], ["1", None]])
def test_area_scope_rejects_invalid_area_values(value: object) -> None:
    with pytest.raises(SearchAreaScopeError):
        validate_search_area_scope(
            request_params={"area": "1"},
            payload={"arguments": [{"argument": "area", "value": value}]},
        )


@pytest.mark.parametrize("payload", [None, {}, {"arguments": {}}, {"arguments": [None]}])
def test_area_scope_fails_closed_on_missing_or_malformed_arguments(payload: object) -> None:
    with pytest.raises(SearchAreaScopeError):
        validate_search_area_scope(request_params={"area": "1"}, payload=payload)


def test_unscoped_search_is_unchanged() -> None:
    validate_search_area_scope(request_params={"text": "python"}, payload={"items": []})
