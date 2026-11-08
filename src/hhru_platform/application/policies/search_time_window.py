from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hhru_platform.application.policies.planner import normalize_split_datetime

HH_SEARCH_TIME_RESOLUTION_SECONDS = 300
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def compute_search_time_window_midpoint(
    *, window_start: datetime, window_end: datetime
) -> datetime | None:
    start = normalize_split_datetime(window_start)
    end = normalize_split_datetime(window_end)
    if end <= start:
        return None

    start_seconds = int((start - _EPOCH).total_seconds())
    end_seconds = int((end - _EPOCH).total_seconds())
    resolution = HH_SEARCH_TIME_RESOLUTION_SECONDS
    # Keep both children at least one API bucket wide, including legacy unaligned bounds.
    minimum_midpoint = start_seconds + resolution
    maximum_midpoint = end_seconds - resolution
    first_boundary = ((minimum_midpoint + resolution - 1) // resolution) * resolution
    last_boundary = (maximum_midpoint // resolution) * resolution
    if first_boundary > last_boundary:
        return None

    middle_seconds = start_seconds + (end_seconds - start_seconds) // 2
    aligned_midpoint = (middle_seconds // resolution) * resolution
    boundary_seconds = min(max(aligned_midpoint, first_boundary), last_boundary)
    return _EPOCH + timedelta(seconds=boundary_seconds)
