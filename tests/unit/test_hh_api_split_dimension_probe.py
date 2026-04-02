from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from notebooks import hh_api_split_dimension_probe as probe


def test_build_time_window_specs_uses_anchor_end() -> None:
    anchor = datetime.fromisoformat("2026-04-01T00:00:00+03:00")

    specs = probe.build_time_window_specs(anchor)

    assert [spec.label for spec in specs] == [
        "trailing_1h",
        "trailing_6h",
        "trailing_24h",
        "trailing_7d",
    ]
    assert specs[0].date_from == "2026-03-31T23:00:00+03:00"
    assert specs[0].date_to == "2026-04-01T00:00:00+03:00"
    assert specs[3].date_from == "2026-03-25T00:00:00+03:00"


def test_summarize_role_overlap_tracks_multi_role_share_and_top_roles() -> None:
    items = [
        {
            "id": "1",
            "professional_roles": [
                {"id": "96", "name": "Developer"},
                {"id": "10", "name": "Analyst"},
            ],
        },
        {
            "id": "2",
            "professional_roles": [
                {"id": "96", "name": "Developer"},
            ],
        },
        {
            "id": "3",
            "professional_roles": [],
        },
    ]

    summary = probe.summarize_role_overlap(items)

    assert summary["sampled_vacancies"] == 3
    assert summary["multi_role_vacancies"] == 1
    assert summary["multi_role_share"] == 0.3333
    assert summary["max_roles_per_vacancy"] == 2
    assert summary["top_roles"][0] == {
        "professional_role_id": "96",
        "name": "Developer",
        "sample_hits": 2,
    }


def test_assess_dimensions_prefers_time_window_when_roles_overlap() -> None:
    assessment = probe.assess_dimensions(
        baseline={"pages": 100},
        role_overlap_summary={"multi_role_vacancies": 2, "multi_role_share": 0.5},
        time_results=[
            {"status_code": 200, "pages": 2},
            {"status_code": 200, "pages": 10},
        ],
        role_results=[
            {"status_code": 200, "pages": 20},
            {"status_code": 200, "pages": 40},
        ],
    )

    assert assessment["time_window"]["supported"] is True
    assert assessment["time_window"]["disjoint_capable"] is True
    assert assessment["time_window"]["best_pages"] == 2
    assert assessment["professional_role"]["disjoint_capable"] is False
    assert assessment["professional_role"]["best_pages"] == 20
    assert assessment["preferred_first_fallback"] == "time_window"
