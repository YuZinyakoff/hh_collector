from __future__ import annotations

from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from hhru_platform.infrastructure.observability.search_quality_audit import (
    address_city_relation,
    area_membership,
    build_address_city_index,
    inspect_derivatives,
    inspect_search_response,
    partition_stratum,
    payload_digest,
    select_partitions,
    summarize_evidence,
)
from hhru_platform.infrastructure.observability.search_quality_audit_db import SearchAuditReader

PARENTS = {"113": None, "1": "113", "2088": "1", "144": "113", "1002": None}


def response() -> dict:
    payload = {
        "items": [
            {
                "id": "139038397",
                "name": "Engineer",
                "area": {"id": "1002", "name": "Minsk"},
                "published_at": "2026-11-01T19:44:49+0300",
            }
        ],
        "found": 2324,
        "pages": 100,
        "page": 0,
        "per_page": 20,
        "alternate_url": "https://hh.ru/search/vacancy?date_from=01.11.2026",
    }
    return {
        "crawl_run_id": "run",
        "crawl_partition_id": "partition",
        "api_request_log_id": 8986186,
        "raw_api_payload_id": 8986030,
        "status_code": 200,
        "coverage_status": "covered",
        "params_json": {
            "area": "144",
            "page": 0,
            "per_page": 20,
            "date_from": "2026-11-01T16:40:01+00:00",
            "date_to": "2026-11-01T16:40:02+00:00",
        },
        "payload_json": payload,
        "payload_hash": payload_digest(payload),
    }


@pytest.mark.parametrize(
    ("actual", "requested", "expected"),
    [
        ("2088", "1", True),
        ("1", "1", True),
        ("1", "113", True),
        ("1002", "144", False),
        ("1002", "1", False),
        ("missing", "1", None),
        ("1", "missing", None),
    ],
)
def test_hierarchy_membership(actual, requested, expected) -> None:
    assert area_membership(actual, requested, PARENTS) is expected


def test_invalid_area_tree_is_unknown_not_clean() -> None:
    assert area_membership("a", "c", {"a": "b", "b": "a", "c": None}) is None
    assert area_membership("a", "c", {"a": "b", "c": None}) is None


def test_incident_is_filter_mismatch_not_storage_corruption() -> None:
    row = response()
    original = deepcopy(row)
    result = inspect_search_response(row, PARENTS)
    counts = result["counts"]
    assert counts["area_outside_current_tree"] == 1
    assert counts["outside_exact_window"] == 1
    assert counts["outside_window_over_300s"] == 0
    assert counts["payload_hash_checked"] == 1
    assert counts["payload_hash_mismatch"] == 0
    assert result["max_time_deviation_seconds"] == 287
    assert result["coverage_status"] == "covered"
    assert row == original


@pytest.mark.parametrize("published", ["2026-11-01T19:40:01+0300", "2026-11-01T16:40:02Z"])
def test_inclusive_bounds_offsets_and_descendant_regions(published) -> None:
    row = response()
    row["params_json"]["area"] = "1"
    row["payload_json"]["items"][0].update(area={"id": "2088"}, published_at=published)
    result = inspect_search_response(row, PARENTS)
    assert result["counts"]["area_in_current_tree"] == 1
    assert result["counts"]["outside_exact_window"] == 0


def test_multiple_areas_missing_dates_invalid_dates_and_bounds() -> None:
    row = response()
    row["params_json"]["area"] = ["144", "1002"]
    row["payload_json"]["items"][0]["published_at"] = "bad"
    result = inspect_search_response(row, PARENTS)
    assert result["counts"]["area_in_current_tree"] == 1
    assert result["counts"]["time_uncheckable"] == 1
    assert result["counts"]["normalization_rejected"] == 1
    row["params_json"]["date_from"] = "2026-11-01T16:40:01"
    assert inspect_search_response(row, PARENTS)["counts"]["invalid_request_bounds"] == 1


def test_empty_missing_oversize_and_bad_shapes_never_count_as_clean_items() -> None:
    row = response()
    row["payload_json"]["items"] = []
    counts = inspect_search_response(row, PARENTS)["counts"]
    assert counts["empty_pages"] == 1
    assert counts["area_in_current_tree"] == counts["time_checked"] == 0
    row["raw_api_payload_id"] = None
    assert inspect_search_response(row, PARENTS)["counts"] == {"raw_missing": 1}
    row["raw_api_payload_id"] = 123
    row["payload_omitted"] = True
    assert inspect_search_response(row, PARENTS)["counts"]["payload_size_limit"] == 1
    del row["payload_omitted"]
    row["payload_json"] = {"items": "not-a-list"}
    assert inspect_search_response(row, PARENTS)["counts"]["invalid_search_shape"] == 1


def test_hash_mismatch_duplicate_ids_echo_and_large_time_deviation() -> None:
    row = response()
    row["payload_json"].update(page=2, per_page=100)
    row["payload_json"]["items"][0]["published_at"] = "2026-11-02T16:40:02Z"
    row["payload_json"]["items"] *= 2
    counts = inspect_search_response(row, PARENTS)["counts"]
    assert counts["payload_hash_mismatch"] == 1
    assert counts["duplicate_item_id_in_page"] == 1
    assert counts["page_echo_mismatch"] == counts["per_page_echo_mismatch"] == 1
    assert counts["outside_window_over_300s"] == 2


def test_derivatives_separate_filter_error_from_persistence_corruption() -> None:
    row = response()
    item = row["payload_json"]["items"][0]
    seen = {
        "hh_vacancy_id": item["id"],
        "short_hash": payload_digest(item),
        "crawl_run_id": "run",
        "crawl_partition_id": "partition",
    }
    snapshot = {"hh_vacancy_id": item["id"], "short_hash": payload_digest(item), "payload": item}
    result = inspect_derivatives(row, {"seen": [seen], "snapshots": [snapshot]})
    assert result["seen_hash_mismatch"] == result["snapshot_payload_mismatch"] == 0
    seen.update(short_hash="bad", crawl_partition_id="wrong")
    snapshot["payload"] = {"id": "wrong"}
    result = inspect_derivatives(row, {"seen": [seen], "snapshots": [snapshot]})
    assert result["seen_hash_mismatch"] == result["seen_crawl_partition_id_mismatch"] == 1
    assert result["snapshot_payload_mismatch"] == 1
    assert inspect_derivatives(row, {"seen": [], "snapshots": []})["seen_absent_pages"] == 1


def test_schedule_spreads_runs_regions_and_leaf_statuses_deterministically() -> None:
    partitions = [
        {
            "id": f"{run}-{area}-{status}-{i}",
            "crawl_run_id": run,
            "params_json": {"params": {"area": area}},
            "coverage_status": status,
        }
        for run in ("old", "new")
        for area in ("1", "144")
        for status in ("split", "covered")
        for i in range(3)
    ]
    first = select_partitions(partitions, limit=8, seed="audit")
    assert len({partition_stratum(p) for p in first}) == 8
    assert first == select_partitions(list(reversed(partitions)), limit=8, seed="audit")
    assert len(select_partitions(partitions, limit=100, seed="audit")) == 24
    assert first != select_partitions(partitions, limit=8, seed="another")
    with pytest.raises(ValueError):
        select_partitions(partitions, limit=0, seed="audit")


def test_summary_never_labels_unknown_evidence_passed() -> None:
    missing = response()
    missing["raw_api_payload_id"] = None
    summary = summarize_evidence(
        [inspect_search_response(response(), PARENTS), inspect_search_response(missing, PARENTS)]
    )
    assert summary["counts"]["raw_missing"] == 1
    assert summary["counts"]["responses"] == 2
    assert summary["counts"]["area_outside_current_tree"] == 1
    assert "not a completeness" in summary["interpretation"]


def test_every_query_is_read_only_with_local_timeouts_and_no_headers_selected() -> None:
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value = []
    reader = SearchAuditReader(engine)
    reader.responses("00000000-0000-0000-0000-000000000001", max_bytes=100)
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert statements[:3] == [
        "SET TRANSACTION READ ONLY",
        "SET LOCAL statement_timeout = 30000",
        "SET LOCAL lock_timeout = '2s'",
    ]
    assert "LEFT JOIN raw_api_payload" in statements[3]
    assert "ORDER BY id DESC LIMIT 1" in statements[3]
    assert "ORDER BY id LIMIT 1" in statements[3]
    assert "request_headers_json" not in statements[3]


def test_reader_stops_before_query_when_time_budget_is_exhausted() -> None:
    engine = MagicMock()
    with pytest.raises(TimeoutError):
        SearchAuditReader(engine, deadline=0).areas()
    engine.connect.assert_not_called()


def test_legacy_snapshot_payload_is_unavailable_not_corrupt() -> None:
    row = response()
    item = row["payload_json"]["items"][0]
    result = inspect_derivatives(
        row,
        {
            "snapshots": [
                {"hh_vacancy_id": item["id"], "short_hash": payload_digest(item), "payload": None}
            ]
        },
    )
    assert result["snapshot_payload_unavailable"] == 1
    assert result.get("snapshot_payload_mismatch", 0) == 0


def test_ambiguous_duplicate_source_ids_do_not_imply_derivative_corruption() -> None:
    row = response()
    row["payload_json"]["items"] *= 2
    assert inspect_derivatives(row, {}) == {"derivatives_uncheckable": 1}


def test_request_lost_scope_is_distinct_from_wrong_upstream_response() -> None:
    row = response()
    row["partition_search_params"] = {**row["params_json"], "area": "1", "page": 10}
    row["partition_expected_id"] = "other"
    result = inspect_search_response(row, PARENTS)
    assert result["request_scope_diff_fields"] == ["area"]
    assert result["counts"]["request_scope_differs_from_partition"] == 1
    assert result["counts"]["request_crawl_partition_id_mismatch"] == 1


@pytest.mark.parametrize(
    ("requested", "actual", "city"),
    [
        ("2", "17", "Санкт-Петербург"),
        ("1", "3", "Москва"),
        ("1", "1117", "Москва"),
    ],
)
def test_incident_posting_area_differs_but_address_supports_query(requested, actual, city) -> None:
    parents = {**PARENTS, "2": "113", "17": "113", "3": "113", "1117": "113"}
    names = [{"hh_area_id": "1", "name": "Москва"}, {"hh_area_id": "2", "name": "Санкт-Петербург"}]
    row = response()
    row["params_json"]["area"] = requested
    row["payload_json"]["items"][0].update(
        area={"id": actual}, address={"city": city, "raw": "private street not for report"}
    )
    result = inspect_search_response(row, parents, build_address_city_index(names))
    assert result["counts"]["area_outside_current_tree"] == 1  # Preserve original diagnostic.
    assert result["counts"]["area_outside_address_supports_requested_area"] == 1
    assert result["area_examples"][0]["address_city"] == city
    assert "private street" not in str(result)


def test_address_ambiguity_missing_unknown_and_descendant_names() -> None:
    parents = {**PARENTS, "other": None, "lost": "missing"}
    index = build_address_city_index(
        [
            {"hh_area_id": "1", "name": "Москва"},
            {"hh_area_id": "2088", "name": "Зеленоград"},
            {"hh_area_id": "144", "name": "Озёрный"},
            {"hh_area_id": "other", "name": "Озерный"},
            {"hh_area_id": "lost", "name": "Lost"},
        ]
    )
    assert address_city_relation("  МОСКВА ", ["1"], parents, index) == "supports_requested_area"
    assert address_city_relation("Зеленоград", ["1"], parents, index) == "supports_requested_area"
    assert address_city_relation("Озёрный", ["113"], parents, index) == "ambiguous_city"
    assert address_city_relation("Москва", ["144"], parents, index) == "outside_current_tree"
    assert address_city_relation("Lost", ["1"], parents, index) == "unknown_tree"
    assert address_city_relation("unmapped", ["1"], parents, index) == "unknown_city"
    assert address_city_relation("Москва", ["1"], parents, {}) == "reference_unavailable"
    assert address_city_relation(None, ["1"], parents, index) == "missing"


def test_missing_address_does_not_clear_original_scope_issue() -> None:
    result = inspect_search_response(response(), PARENTS)
    assert result["counts"]["area_outside_current_tree"] == 1
    assert result["counts"]["area_outside_address_missing"] == 1


def test_area_examples_are_not_hidden_by_first_three_time_only_anomalies() -> None:
    row = response()
    row["params_json"]["area"] = "1"
    row["payload_json"]["items"] = [
        {
            "id": str(i),
            "name": "Engineer",
            "area": {"id": "1"},
            "published_at": "2026-11-01T16:42:00Z",
        }
        for i in range(3)
    ] + [
        {
            "id": "regional",
            "name": "Engineer",
            "area": {"id": "1002"},
            "published_at": "2026-11-01T16:40:01Z",
        },
        {
            "id": "large-time",
            "name": "Engineer",
            "area": {"id": "1"},
            "published_at": "2026-11-02T16:40:01Z",
        },
    ]
    result = inspect_search_response(row, PARENTS)
    assert len(result["examples"]) == len(result["time_examples"]) == 3
    assert [example["id"] for example in result["area_examples"]] == ["regional"]
    assert [example["id"] for example in result["time_over_300s_examples"]] == ["large-time"]
    assert result["counts"]["area_outside_current_tree"] == 1
    assert result["counts"]["outside_exact_window"] == 4


def test_applied_scope_arguments_are_checked_without_exporting_disable_urls() -> None:
    row = response()
    row["params_json"]["describe_arguments"] = "true"
    row["payload_json"]["arguments"] = [
        {"argument": "area", "value": "144", "disable_url": "SECRET"},
        {"argument": "text", "value": "PRIVATE"},
    ]
    result = inspect_search_response(row, PARENTS)
    assert result["counts"]["area_arguments_checked"] == 1
    assert result["counts"]["area_arguments_mismatch"] == 0
    assert result["applied_scope_arguments"] == [{"argument": "area", "value": "144"}]
    assert "SECRET" not in str(result) and "PRIVATE" not in str(result)
    row["payload_json"]["arguments"] = []
    assert inspect_search_response(row, PARENTS)["counts"]["area_arguments_mismatch"] == 1
    row["payload_json"]["arguments"] = None
    assert inspect_search_response(row, PARENTS)["counts"]["applied_arguments_unavailable"] == 1


def test_fresh_publication_evidence_does_not_assert_an_index_race() -> None:
    row = response()
    row["received_at"] = "2026-11-09T03:54:44.534737+00:00"
    row["payload_json"]["items"][0].update(
        published_at="2026-11-09T06:53:54+0300",
        created_at="2026-11-09T06:53:54+0300",
        premium=False,
        is_adv_vacancy=False,
        type={"id": "open"},
    )
    result = inspect_search_response(row, PARENTS)
    example = result["time_over_300s_examples"][0]
    assert example["published_age_at_response_seconds"] == pytest.approx(50.534737)
    assert example["premium"] is False and example["is_adv_vacancy"] is False
    assert example["vacancy_type_id"] == "open"
    assert "index_race" not in result
    row["payload_json"]["items"][0]["address"] = {"city": "x" * 1000}
    assert len(inspect_search_response(row, PARENTS)["area_examples"][0]["address_city"]) == 200


def test_derivative_checker_detects_a_missing_item_on_a_nonempty_seen_page() -> None:
    row = response()
    one = row["payload_json"]["items"][0]
    row["payload_json"]["items"].append({**one, "id": "second"})
    counts = inspect_derivatives(
        row,
        {
            "seen": [
                {
                    "hh_vacancy_id": one["id"],
                    "short_hash": payload_digest(one),
                    "crawl_run_id": "run",
                    "crawl_partition_id": "partition",
                }
            ]
        },
    )
    assert counts["seen_absent_pages"] == 0
    assert counts["raw_item_ids_without_seen"] == 1


def test_weighted_streaming_summary_matches_individual_response_summary() -> None:
    rows = [inspect_search_response(response(), PARENTS) for _ in range(3)]
    expected = summarize_evidence(rows)
    row = rows[0]
    weighted = {
        **row,
        "counts": {key: value * 3 for key, value in row["counts"].items()},
        "response_count": 3,
    }
    assert summarize_evidence([weighted]) == expected


def test_batched_reader_uses_request_keyset_and_explicit_watermark(monkeypatch) -> None:
    reader = SearchAuditReader(MagicMock())
    batches = [
        [{"api_request_log_id": 1}, {"api_request_log_id": 3}],
        [{"api_request_log_id": 5}],
    ]
    query = MagicMock(side_effect=batches)
    monkeypatch.setattr(reader, "query", query)
    result = list(
        reader.all_responses("partition", max_bytes=100, through_request_id=10, batch_size=2)
    )
    assert [row["api_request_log_id"] for row in result] == [1, 3, 5]
    assert [call.args[1]["after_id"] for call in query.call_args_list] == [0, 3]
    assert all(call.args[1]["through_id"] == 10 for call in query.call_args_list)
    sql = query.call_args.args[0]
    assert "id > :after_id AND id <= :through_id" in sql
    assert "request_headers_json" not in sql and "Authorization" not in sql


def test_batched_reader_rejects_stalled_cursor(monkeypatch) -> None:
    reader = SearchAuditReader(MagicMock())
    monkeypatch.setattr(reader, "query", lambda *args: [{"api_request_log_id": 0}])
    with pytest.raises(ValueError, match="did not advance"):
        list(reader.all_responses("partition", max_bytes=100, through_request_id=10, batch_size=1))


def test_inventory_is_scoped_to_run_and_partition_references(monkeypatch) -> None:
    reader = SearchAuditReader(MagicMock())
    query = MagicMock(return_value=[{"requests": 5}])
    monkeypatch.setattr(reader, "query", query)
    assert reader.request_inventory("run")["requests"] == 5
    sql = query.call_args.args[0]
    assert "request_run_mismatches" in sql and "partition_run_mismatches" in sql
    assert "multiple_raw_requests" in sql and "missing_200_raw" in sql
    assert "request_headers_json" not in sql
