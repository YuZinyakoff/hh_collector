from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlsplit

from hhru_platform.infrastructure.normalization.vacancy_short_normalizer import (
    VacancySearchNormalizationError,
    normalize_vacancy_search_page,
)


def aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        return None
    return result if result.utcoffset() is not None else None


def payload_digest(payload: object) -> str:
    document = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


def _example_value(value: object) -> str | int | float | bool | None:
    if isinstance(value, str):
        return value[:200]
    return value if isinstance(value, int | float | bool) else None


def area_membership(actual: str, requested: str, parents: Mapping[str, str | None]) -> bool | None:
    """Use the supplied dictionary, not an assumed equality of parent and leaf IDs."""
    if actual not in parents or requested not in parents:
        return None
    visited: set[str] = set()
    current: str | None = actual
    while current is not None:
        if current in visited or current not in parents:
            return None
        if current == requested:
            return True
        visited.add(current)
        current = parents[current]
    return False


def _areas(params: Mapping[str, Any]) -> list[str]:
    value = params.get("area")
    values = value if isinstance(value, list) else [value]
    return [str(item) for item in values if isinstance(item, str | int)]


def build_address_city_index(areas: Iterable[Mapping[str, Any]]) -> dict[str, tuple[str, ...]]:
    cities: dict[str, set[str]] = defaultdict(set)
    for area in areas:
        name, area_id = area.get("name"), area.get("hh_area_id")
        if isinstance(name, str) and isinstance(area_id, str):
            cities[_city_key(name)].add(area_id)
    return {name: tuple(sorted(ids)) for name, ids in cities.items()}


def _city_key(value: str) -> str:
    return " ".join(value.casefold().replace("ё", "е").split())


def address_city_relation(
    city: object,
    requested_areas: Sequence[str],
    parents: Mapping[str, str | None],
    city_index: Mapping[str, Sequence[str]],
) -> str:
    """Supporting evidence only: no geocoding or certification of the API filter."""
    if not isinstance(city, str) or not city.strip():
        return "missing"
    if not city_index:
        return "reference_unavailable"
    candidates = city_index.get(_city_key(city))
    if not candidates:
        return "unknown_city"
    results: set[bool | None] = set()
    for candidate in candidates:
        memberships = [area_membership(candidate, area, parents) for area in requested_areas]
        results.add(True if True in memberships else None if None in memberships else False)
    if None in results:
        return "unknown_tree"
    if results == {True}:
        return "supports_requested_area"
    if results == {False}:
        return "outside_current_tree"
    return "ambiguous_city"


def inspect_search_response(
    row: Mapping[str, Any],
    parents: Mapping[str, str | None],
    city_index: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Return evidence, never a certificate of completeness or instructions to repair data."""
    params = row.get("params_json")
    params = params if isinstance(params, dict) else {}
    requested_areas = _areas(params)
    result: dict[str, Any] = {
        key: str(row[key]) if row.get(key) is not None else None
        for key in (
            "crawl_run_id",
            "crawl_partition_id",
            "api_request_log_id",
            "raw_api_payload_id",
            "requested_at",
            "received_at",
            "coverage_status",
        )
    }
    result["requested_areas"] = requested_areas
    result["requested_window"] = {k: params[k] for k in ("date_from", "date_to") if k in params}
    result["request_page"] = params.get("page", 0)
    result["status_code"] = row.get("status_code")
    result["error_type"] = row.get("error_type")
    counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    result["counts"] = counts
    result["examples"] = examples
    area_examples: list[dict[str, Any]] = []
    time_examples: list[dict[str, Any]] = []
    large_time_examples: list[dict[str, Any]] = []
    result["area_examples"] = area_examples
    result["time_examples"] = time_examples
    result["time_over_300s_examples"] = large_time_examples
    planned = row.get("partition_search_params")
    if isinstance(planned, dict):
        fields = ("area", "date_from", "date_to", "professional_role", "text")
        differences = [key for key in fields if planned.get(key) != params.get(key)]
        result["request_scope_diff_fields"] = differences
        counts["request_scope_checked"] += 1
        counts["request_scope_differs_from_partition"] += int(bool(differences))
        for expected_key, actual_key in (
            ("partition_expected_run_id", "crawl_run_id"),
            ("partition_expected_id", "crawl_partition_id"),
        ):
            if expected_key in row:
                counts[f"request_{actual_key}_mismatch"] += int(
                    str(row[expected_key]) != str(row.get(actual_key))
                )
    if row.get("raw_api_payload_id") is None:
        counts["raw_missing"] += 1
        return result
    counts["raw_present"] += 1
    if row.get("payload_omitted"):
        counts["payload_size_limit"] += 1
        return result
    payload = row.get("payload_json")
    expected_hash = row.get("payload_hash")
    if expected_hash:
        counts["payload_hash_checked"] += 1
        counts["payload_hash_mismatch"] += int(payload_digest(payload) != expected_hash)
    else:
        counts["payload_hash_missing"] += 1
    if row.get("status_code") != 200:
        counts["non_200"] += 1
        return result
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        counts["invalid_search_shape"] += 1
        return result
    if str(params.get("describe_arguments", "")).lower() == "true":
        arguments = payload.get("arguments")
        if not isinstance(arguments, list) or any(not isinstance(a, dict) for a in arguments):
            counts["applied_arguments_unavailable"] += 1
        else:
            counts["area_arguments_checked"] += 1
            applied = [a.get("value") for a in arguments if a.get("argument") == "area"]
            valid = all(isinstance(v, str | int) and not isinstance(v, bool) for v in applied)
            counts["area_arguments_mismatch"] += int(
                not valid or {str(v).strip() for v in applied} != set(requested_areas)
            )
            result["applied_scope_arguments"] = [
                {"argument": a["argument"], "value": _example_value(a.get("value"))}
                for a in arguments
                if a.get("argument") in {"area", "date_from", "date_to"}
            ]
    try:
        normalize_vacancy_search_page(payload)
    except (VacancySearchNormalizationError, TypeError, ValueError):
        counts["normalization_rejected"] += 1
    items = payload["items"]
    counts["empty_pages" if not items else "nonempty_pages"] += 1
    result["found"] = payload.get("found")
    result["response_pages"] = payload.get("pages")
    # alternate_url is diagnostic only; omission is not itself proof a filter was ignored.
    alternate = payload.get("alternate_url")
    if isinstance(alternate, str):
        query = parse_qs(urlsplit(alternate).query)
        result["alternate_area"] = query.get("area", [])
        if requested_areas and not query.get("area"):
            counts["alternate_area_absent"] += 1
    if "page" in payload and payload["page"] != params.get("page", 0):
        counts["page_echo_mismatch"] += 1
    if "per_page" in params and payload.get("per_page") is not None:
        counts["per_page_echo_mismatch"] += int(payload["per_page"] != params["per_page"])
    bounds = {key: aware_datetime(params[key]) for key in ("date_from", "date_to") if key in params}
    invalid_bounds = any(value is None for value in bounds.values())
    counts["invalid_request_bounds"] += int(invalid_bounds)
    item_ids: set[str] = set()
    max_deviation = 0.0
    for item in items:
        counts["items"] += 1
        if not isinstance(item, dict):
            counts["invalid_items"] += 1
            continue
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id:
            counts["missing_item_id"] += 1
        elif item_id in item_ids:
            counts["duplicate_item_id_in_page"] += 1
        else:
            item_ids.add(item_id)
        area = item.get("area")
        actual_area = str(area["id"]) if isinstance(area, dict) and area.get("id") else None
        address = item.get("address")
        city = address.get("city") if isinstance(address, dict) else None
        published = aware_datetime(item.get("published_at"))
        received = aware_datetime(str(row.get("received_at")))
        vacancy_type = item.get("type")
        example = {
            "id": _example_value(item_id),
            "area_id": _example_value(actual_area),
            "published_at": _example_value(item.get("published_at")),
            "address_city": _example_value(city),
            "created_at": _example_value(item.get("created_at")),
            "premium": _example_value(item.get("premium")),
            "is_adv_vacancy": _example_value(item.get("is_adv_vacancy")),
            "vacancy_type_id": _example_value(vacancy_type.get("id"))
            if isinstance(vacancy_type, dict)
            else None,
            "published_age_at_response_seconds": (
                (received - published).total_seconds() if received and published else None
            ),
        }
        issue = False
        if requested_areas:
            memberships = [
                area_membership(actual_area, a, parents) if actual_area else None
                for a in requested_areas
            ]
            if True in memberships:
                counts["area_in_current_tree"] += 1
            elif None in memberships:
                counts["area_uncheckable"] += 1
            else:
                counts["area_outside_current_tree"] += 1
                relation = address_city_relation(city, requested_areas, parents, city_index or {})
                counts[f"area_outside_address_{relation}"] += 1
                example["address_city_relation"] = relation
                if len(area_examples) < 3:
                    area_examples.append(example)
                issue = True
        if bounds:
            if invalid_bounds or published is None:
                counts["time_uncheckable"] += 1
            else:
                counts["time_checked"] += 1
                lower, upper = bounds.get("date_from"), bounds.get("date_to")
                deviation = max(
                    (lower - published).total_seconds() if lower else 0,
                    (published - upper).total_seconds() if upper else 0,
                    0,
                )
                if deviation:
                    counts["outside_exact_window"] += 1
                    counts["outside_window_over_300s"] += int(deviation > 300)
                    max_deviation = max(max_deviation, deviation)
                    example["time_deviation_seconds"] = deviation
                    if len(time_examples) < 3:
                        time_examples.append(example)
                    if deviation > 300 and len(large_time_examples) < 3:
                        large_time_examples.append(example)
                    issue = True
        if issue and len(examples) < 3:
            examples.append(example)
    result["max_time_deviation_seconds"] = max_deviation
    return result


def inspect_derivatives(
    row: Mapping[str, Any], derivatives: Mapping[str, list[dict[str, Any]]]
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    payload = row.get("payload_json")
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        return {"derivatives_uncheckable": 1}
    items = {
        item["id"]: item
        for item in payload["items"]
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    if len(items) != len(payload["items"]):
        return {"derivatives_uncheckable": 1}
    counts["derivative_pages_checked"] = 1
    for kind in ("seen", "snapshots"):
        records = derivatives.get(kind, [])
        counts[f"{kind}_rows_truncated"] += int(len(records) > 1000)
        counts[f"{kind}_absent_pages"] += int(not records)
        for record in records[:1000]:
            counts[f"{kind}_rows_checked"] += 1
            source = items.get(record.get("hh_vacancy_id"))
            if source is None:
                counts[f"{kind}_id_not_in_raw"] += 1
                continue
            counts[f"{kind}_hash_mismatch"] += int(
                record.get("short_hash") != payload_digest(source)
            )
            if kind == "seen":
                for key in ("crawl_run_id", "crawl_partition_id"):
                    counts[f"seen_{key}_mismatch"] += int(str(record.get(key)) != str(row.get(key)))
            else:
                if not isinstance(record.get("payload"), dict):
                    counts["snapshot_payload_unavailable"] += 1
                else:
                    counts["snapshot_payload_mismatch"] += int(record["payload"] != source)
        if kind == "seen" and len(records) <= 1000:
            observed_ids = {record.get("hh_vacancy_id") for record in records}
            counts["raw_item_ids_without_seen"] += len(set(items) - observed_ids)
    return dict(counts)


def select_partitions(
    partitions: Sequence[dict[str, Any]], *, limit: int, seed: str
) -> list[dict[str, Any]]:
    """Round-robin run/area/coverage strata; hash-order within each stratum."""
    if limit < 1:
        raise ValueError("limit must be positive")
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for partition in partitions:
        groups[partition_stratum(partition)].append(partition)

    def priority(value: object) -> str:
        return hashlib.sha256(f"{seed}:{value}".encode()).hexdigest()

    queues = deque(
        deque(sorted(groups[key], key=lambda p: priority(p["id"])))
        for key in sorted(groups, key=priority)
    )
    result: list[dict[str, Any]] = []
    while queues and len(result) < limit:
        group = queues.popleft()
        result.append(group.popleft())
        if group:
            queues.append(group)
    return result


def partition_stratum(partition: Mapping[str, Any]) -> tuple[str, str, str]:
    params = partition.get("params_json") or {}
    params = params.get("params", params)
    return (
        str(partition["crawl_run_id"]),
        ",".join(sorted(_areas(params))),
        str(partition.get("coverage_status", "unknown")),
    )


def summarize_evidence(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    totals: Counter[str] = Counter()
    for row in rows:
        key = (
            str(row.get("crawl_run_id")),
            ",".join(row.get("requested_areas", [])),
            str(row.get("coverage_status")),
        )
        counts = Counter(row.get("counts", {}))
        counts["responses"] += int(row.get("response_count", 1))
        groups[key].update(counts)
        totals.update(counts)
    return {
        "interpretation": "diagnostic evidence only; not a completeness certificate",
        "sampled_regions": sorted({key[1] for key in groups if key[1]}),
        "regions_with_area_mismatches": sorted(
            {key[1] for key, value in groups.items() if value["area_outside_current_tree"] > 0}
        ),
        "regions_with_exact_time_mismatches": sorted(
            {key[1] for key, value in groups.items() if value["outside_exact_window"] > 0}
        ),
        "counts": dict(totals),
        "by_run_area_coverage": [
            {"run_id": key[0], "area_id": key[1], "coverage_status": key[2], "counts": dict(value)}
            for key, value in sorted(groups.items())
        ],
    }
