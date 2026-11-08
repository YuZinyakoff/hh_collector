from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class WholeAreaTimeFallback:
    query_area_hh_id: str
    unsupported_descendant_area_ids: tuple[str, ...]

    def to_metadata(self) -> dict[str, object]:
        return {
            "name": "whole_area_time_fallback_v1",
            "query_area_hh_id": self.query_area_hh_id,
            "unsupported_descendant_area_ids": list(self.unsupported_descendant_area_ids),
            "reason": "descendant_area_filter_not_applied",
            "verified_at": "2026-11-08",
        }


# VPS probes confirmed parent filters while HH silently dropped the descendant filters.
_VERIFIED_WHOLE_AREA_FALLBACKS = (
    WholeAreaTimeFallback("2019", ("144",)),
    WholeAreaTimeFallback("1229", ("17683",)),
)


def get_whole_area_time_fallback(area_hh_id: str) -> WholeAreaTimeFallback | None:
    return next(
        (
            policy
            for policy in _VERIFIED_WHOLE_AREA_FALLBACKS
            if policy.query_area_hh_id == area_hh_id
        ),
        None,
    )
