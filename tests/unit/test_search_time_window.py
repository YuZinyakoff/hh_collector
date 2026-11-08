from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from hhru_platform.application.policies.search_time_window import (
    HH_SEARCH_TIME_RESOLUTION_SECONDS,
    compute_search_time_window_midpoint,
)


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        ("00:00:00", "00:10:00", "00:05:00"),
        ("00:00:00", "00:15:00", "00:05:00"),
        ("00:00:00", "00:20:00", "00:10:00"),
        ("00:00:23", "00:15:37", "00:10:00"),
        ("00:00:00", "00:10:25", "00:05:00"),
        ("00:00:25", "00:15:00", "00:10:00"),
    ],
)
def test_midpoint_is_on_five_minute_grid_with_full_bucket_children(
    start: str, end: str, expected: str
) -> None:
    start_at = datetime.fromisoformat(f"2026-11-08T{start}+00:00")
    end_at = datetime.fromisoformat(f"2026-11-08T{end}+00:00")
    midpoint = compute_search_time_window_midpoint(window_start=start_at, window_end=end_at)
    assert midpoint == datetime.fromisoformat(f"2026-11-08T{expected}+00:00")
    assert (midpoint - start_at).total_seconds() >= HH_SEARCH_TIME_RESOLUTION_SECONDS
    assert (end_at - midpoint).total_seconds() >= HH_SEARCH_TIME_RESOLUTION_SECONDS


@pytest.mark.parametrize("duration_seconds", [1, 2, 299, 300, 301, 599])
def test_sub_resolution_children_are_never_created(duration_seconds: int) -> None:
    start_at = datetime(2026, 11, 8, tzinfo=UTC)
    assert (
        compute_search_time_window_midpoint(
            window_start=start_at,
            window_end=start_at + timedelta(seconds=duration_seconds),
        )
        is None
    )


def test_unaligned_legacy_ten_minute_window_is_not_split_into_a_shorter_bucket() -> None:
    assert (
        compute_search_time_window_midpoint(
            window_start=datetime(2026, 11, 8, 0, 0, 1, tzinfo=UTC),
            window_end=datetime(2026, 11, 8, 0, 10, 1, tzinfo=UTC),
        )
        is None
    )


@pytest.mark.parametrize("end_offset", [0, -1])
def test_empty_or_reversed_window_has_no_midpoint(end_offset: int) -> None:
    start_at = datetime(2026, 11, 8, tzinfo=UTC)
    assert (
        compute_search_time_window_midpoint(
            window_start=start_at,
            window_end=start_at + timedelta(seconds=end_offset),
        )
        is None
    )


def test_equivalent_offset_bounds_produce_the_same_utc_midpoint() -> None:
    assert compute_search_time_window_midpoint(
        window_start=datetime.fromisoformat("2026-11-08T03:00:00+03:00"),
        window_end=datetime.fromisoformat("2026-11-08T03:10:00+03:00"),
    ) == datetime(2026, 11, 8, 0, 5, tzinfo=UTC)


def test_recursive_splits_keep_original_outer_bounds_without_gaps() -> None:
    original_start = datetime(2026, 11, 7, 12, 1, 13, tzinfo=UTC)
    original_end = datetime(2026, 11, 8, 17, 23, 59, tzinfo=UTC)
    pending = [(original_start, original_end)]
    leaves: list[tuple[datetime, datetime]] = []
    while pending:
        start_at, end_at = pending.pop()
        midpoint = compute_search_time_window_midpoint(window_start=start_at, window_end=end_at)
        if midpoint is None:
            leaves.append((start_at, end_at))
            continue
        assert midpoint.second == 0 and midpoint.minute % 5 == 0
        pending.extend([(start_at, midpoint), (midpoint, end_at)])

    leaves.sort()
    assert leaves[0][0] == original_start
    assert leaves[-1][1] == original_end
    assert all(left[1] == right[0] for left, right in zip(leaves, leaves[1:], strict=False))
    assert all((end_at - start_at).total_seconds() >= 300 for start_at, end_at in leaves)
    assert len(leaves) < 400
