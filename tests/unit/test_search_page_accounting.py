from __future__ import annotations

import pytest

from hhru_platform.infrastructure.observability.search_page_accounting import (
    PartitionPageAccounting,
)


def ledger(pages: list[object]) -> PartitionPageAccounting:
    result = PartitionPageAccounting()
    for page in pages:
        result.observe({"status_code": 200, "params_json": {"page": page}})
    return result


def test_complete_leaf_and_empty_result_have_consistent_accounting() -> None:
    result = ledger([0, 1, 2]).finish(
        {
            "coverage_status": "covered",
            "pages_processed": 3,
            "pages_total_expected": 3,
        }
    )
    assert result["partitions_checked"] == 1
    assert not any(value for key, value in result.items() if key != "partitions_checked")
    empty = ledger([0]).finish(
        {
            "coverage_status": "covered",
            "pages_processed": 1,
            "pages_total_expected": 0,
        }
    )
    assert empty["covered_expected_page_count_mismatches"] == 0


def test_saturated_parent_does_not_need_to_fetch_all_reported_pages() -> None:
    result = ledger([0]).finish(
        {
            "coverage_status": "split",
            "pages_processed": 1,
            "pages_total_expected": 100,
        }
    )
    assert result["processed_page_count_mismatches"] == 0
    assert result["covered_expected_page_count_mismatches"] == 0


def test_gap_count_and_duplicate_attempts_are_different_diagnostics() -> None:
    result = ledger([0, 0, 2]).finish(
        {
            "coverage_status": "covered",
            "pages_processed": 3,
            "pages_total_expected": 3,
        }
    )
    assert result["duplicate_http_200_page_attempts"] == 1
    assert result["noncontiguous_page_sequences"] == 1
    assert result["processed_page_count_mismatches"] == 1
    assert result["covered_expected_page_count_mismatches"] == 1


@pytest.mark.parametrize("page", [-1, True, None, "-1", "x", {}, "1" * 100])
def test_bad_page_parameters_do_not_count_as_processed(page: object) -> None:
    result = ledger([page]).finish({"coverage_status": "covered", "pages_processed": 1})
    assert result["invalid_request_pages"] == 1
    assert result["partitions_without_http_200"] == 1
    assert result["processed_page_count_mismatches"] == 1
    assert result["covered_unknown_expected_page_count"] == 1


def test_transport_attempts_are_not_pages_and_string_pages_are_supported() -> None:
    result = ledger(["0"])
    result.observe({"status_code": 0, "params_json": {"page": 0}})
    assert result.pages == {0: 1}
    assert result.finish({})["unknown_processed_page_count"] == 1
