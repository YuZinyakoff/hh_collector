from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from typing import Any


class PartitionPageAccounting:
    """Request/progress consistency, not a certificate of external search completeness."""

    def __init__(self) -> None:
        self.pages: Counter[int] = Counter()
        self.invalid_pages = 0

    def observe(self, row: Mapping[str, Any]) -> None:
        if row.get("status_code") != 200:
            return
        params = row.get("params_json")
        value = params.get("page", 0) if isinstance(params, dict) else None
        if isinstance(value, str) and value.isascii() and value.isdigit() and len(value) <= 8:
            value = int(value)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            self.invalid_pages += 1
        else:
            self.pages[value] += 1

    def finish(self, partition: Mapping[str, Any]) -> dict[str, int]:
        count = len(self.pages)
        processed = partition.get("pages_processed")
        expected = partition.get("pages_total_expected")
        covered = partition.get("coverage_status") == "covered"
        processed_known = (
            isinstance(processed, int) and not isinstance(processed, bool) and processed >= 0
        )
        expected_pages = (
            max(expected, 1)
            if isinstance(expected, int) and not isinstance(expected, bool) and expected >= 0
            else None
        )
        return {
            "partitions_checked": 1,
            "partitions_without_http_200": int(not self.pages),
            "invalid_request_pages": self.invalid_pages,
            "duplicate_http_200_page_attempts": sum(self.pages.values()) - count,
            "unknown_processed_page_count": int(not processed_known),
            "processed_page_count_mismatches": int(processed_known and processed != count),
            "covered_unknown_expected_page_count": int(covered and expected_pages is None),
            "covered_expected_page_count_mismatches": int(
                covered and expected_pages is not None and count != expected_pages
            ),
            "noncontiguous_page_sequences": int(
                bool(self.pages) and (min(self.pages) != 0 or max(self.pages) != count - 1)
            ),
        }
