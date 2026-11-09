#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from hhru_platform.infrastructure.observability.search_page_accounting import (  # noqa: E402
    PartitionPageAccounting,
)
from hhru_platform.infrastructure.observability.search_quality_audit import (  # noqa: E402
    build_address_city_index,
    inspect_derivatives,
    inspect_search_response,
    partition_stratum,
    payload_digest,
    select_partitions,
    summarize_evidence,
)


def positive(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


class AuditIncompleteError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only, bounded search evidence audit; no HTTP."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--database", action="store_true")
    source.add_argument("--archive-dir", type=Path)
    parser.add_argument("--area-reference", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", action="append", type=UUID, default=[])
    parser.add_argument("--max-runs", type=positive, default=12)
    parser.add_argument("--max-partitions", type=positive, default=4000)
    parser.add_argument("--metadata-limit-per-run", type=positive, default=100_000)
    parser.add_argument("--max-provenance-pages", type=positive, default=100)
    parser.add_argument("--all-responses", action="store_true")
    parser.add_argument("--max-requests", type=positive, default=100_000)
    parser.add_argument("--response-batch-size", type=positive, default=20)
    parser.add_argument("--statement-timeout-ms", type=positive, default=30_000)
    parser.add_argument("--max-seconds", type=positive, default=1800)
    parser.add_argument("--max-payload-bytes", type=positive, default=2_000_000)
    parser.add_argument("--max-manifests", type=positive, default=1000)
    parser.add_argument("--max-rows", type=positive, default=100_000)
    parser.add_argument("--max-chunk-bytes", type=positive, default=64_000_000)
    parser.add_argument("--seed", default="search-quality-2026-11-07")
    args = parser.parse_args(argv)
    if args.archive_dir and not args.area_reference:
        parser.error("--archive-dir requires --area-reference from a database audit")
    if args.all_responses:
        if not args.database or len(set(args.run_id)) != 1:
            parser.error("--all-responses requires --database and exactly one explicit --run-id")
        if args.response_batch_size > 50:
            parser.error("--response-batch-size must not exceed 50")
        if args.response_batch_size * args.max_payload_bytes > 64_000_000:
            parser.error("response batch payload budget must not exceed 64 MB")
    return args


class AuditReport:
    def __init__(self, directory: Path) -> None:
        # Never overwrite previous evidence or dump full raw bodies / credentials.
        directory.mkdir(parents=True, exist_ok=False, mode=0o700)
        self.directory = directory
        self.events = (directory / "evidence.jsonl").open("x", encoding="utf-8")
        self.response_count = 0
        self.groups: dict[tuple[str, tuple[str, ...], str], dict[str, Any]] = {}
        self.metadata: dict[str, Any] = {
            "report_schema": "search-quality-audit-v2",
            "started_at": datetime.now(UTC).isoformat(),
            "read_only": True,
            "live_http_requests": 0,
            "status": "running",
            "limitations": [
                "Not a full-corpus or semantic truth certificate; sampling can miss defects.",
                "Current area hierarchy may differ from historical hierarchy.",
                "Matching address.city supports geography but does not certify a search filter.",
                "Exact-window deviations are evidence, not a claim about API time precision.",
                "Empty pages and missing raw cannot certify filter correctness.",
                "No inactivity repair, detail-body audit, or coverage recomputation is performed.",
            ],
        }

    def event(self, event: str, **fields: Any) -> None:
        self.events.write(
            json.dumps({"event": event, **fields}, default=str, ensure_ascii=False) + "\n"
        )
        self.events.flush()

    def inspect(
        self,
        row: dict[str, Any],
        parents: dict[str, str | None],
        city_index: dict[str, tuple[str, ...]] | None = None,
    ) -> None:
        evidence = inspect_search_response(row, parents, city_index)
        self.event("response", **evidence)
        key = (
            str(evidence["crawl_run_id"]),
            tuple(evidence["requested_areas"]),
            str(evidence["coverage_status"]),
        )
        group = self.groups.setdefault(
            key,
            {
                "crawl_run_id": evidence["crawl_run_id"],
                "requested_areas": evidence["requested_areas"],
                "coverage_status": evidence["coverage_status"],
                "counts": Counter(),
                "response_count": 0,
            },
        )
        group["counts"].update(evidence["counts"])
        group["response_count"] += 1
        self.response_count += 1

    def finish(self, *, status: str) -> None:
        self.metadata.update(summarize_evidence(self.groups.values()))
        self.metadata.update(status=status, finished_at=datetime.now(UTC).isoformat())
        self.events.close()
        (self.directory / "summary.json").write_text(
            json.dumps(self.metadata, default=str, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def collect_database(args: argparse.Namespace, report: AuditReport) -> None:
    from sqlalchemy import create_engine

    from hhru_platform.config.settings import get_settings
    from hhru_platform.infrastructure.observability.search_quality_audit_db import SearchAuditReader

    engine = create_engine(get_settings().database_url, connect_args={"connect_timeout": 10})
    reader = SearchAuditReader(
        engine, timeout_ms=args.statement_timeout_ms, deadline=monotonic() + args.max_seconds
    )
    try:
        areas = reader.areas()
        parents = {row["hh_area_id"]: row["parent_hh_area_id"] for row in areas}
        city_index = build_address_city_index(areas)
        (report.directory / "area-reference.json").write_text(
            json.dumps(
                {"captured_at": datetime.now(UTC).isoformat(), "areas": areas},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        runs = reader.runs(limit=args.max_runs, run_ids=[str(value) for value in args.run_id])
        report.metadata["runs"] = runs
        if not runs:
            raise ValueError("no selected runs")
        if args.run_id and len(runs) != len(set(args.run_id)):
            raise ValueError("some requested runs were not found")
        if args.all_responses and (
            runs[0]["status"] == "created" or not runs[0].get("finished_at")
        ):
            raise AuditIncompleteError("run_not_finished")
        partitions = []
        for run in runs:
            rows = reader.partitions(str(run["id"]), limit=args.metadata_limit_per_run + 1)
            truncated = len(rows) > args.metadata_limit_per_run
            report.event(
                "partition_inventory",
                run_id=run["id"],
                rows_loaded=min(len(rows), args.metadata_limit_per_run),
                truncated=truncated,
            )
            if truncated:
                # An ordered prefix would silently bias all subsequent run/area strata.
                raise ValueError("partition metadata limit reached; no unbiased schedule available")
            partitions.extend(rows)
        selected = select_partitions(partitions, limit=args.max_partitions, seed=args.seed)
        inventory: dict[str, Any] | None = None
        seen_snapshot: list[dict[str, Any]] = []
        seen_counts: dict[tuple[str, str], int] = {}
        observation_counts: Counter[str] = Counter()
        if args.all_responses:
            report.metadata["all_request_rows_visited"] = False
            report.metadata["derivative_selection"] = (
                "first nonempty response per partition in seeded stratum order, until cap"
            )
            if len(selected) != len(partitions):
                raise AuditIncompleteError("partition_selection_truncated")
            inventory = reader.request_inventory(str(runs[0]["id"]))
            report.metadata["request_inventory_before"] = inventory
            if not inventory["requests"] or inventory["requests"] > args.max_requests:
                raise AuditIncompleteError("request_inventory_empty_or_over_budget")
            if any(
                inventory[k]
                for k in (
                    "request_run_mismatches",
                    "partition_run_mismatches",
                    "multiple_raw_requests",
                )
            ):
                raise AuditIncompleteError("request_reference_or_raw_multiplicity_mismatch")
            seen_snapshot = reader.seen_inventory(
                str(runs[0]["id"]), max_groups=args.max_requests * 2
            )
            if len(seen_snapshot) > args.max_requests * 2:
                raise AuditIncompleteError("seen_group_budget_exceeded")
            seen_counts = {
                (str(r["short_payload_ref_id"]), str(r["crawl_partition_id"])): r["seen_rows"]
                for r in seen_snapshot
            }
            report.metadata["observation_count_checks"] = observation_counts
        population = Counter(partition_stratum(p) for p in partitions)
        sampled = Counter(partition_stratum(p) for p in selected)
        report.metadata["sampling"] = {
            "method": (
                "all requests through inventory watermark; hash-seeded partition order"
                if args.all_responses
                else "hash-seeded round-robin run/area/coverage strata; first and last requests"
            ),
            "seed": args.seed,
            "partitions_available": len(partitions),
            "partitions_selected": len(selected),
            "strata_available": len(population),
            "strata_selected": len(sampled),
            "is_full_response_audit": False,
        }
        report.metadata["strata"] = [
            {
                "run_id": k[0],
                "area_id": k[1],
                "coverage_status": k[2],
                "partitions_available": n,
                "partitions_selected": sampled[k],
            }
            for k, n in sorted(population.items())
        ]
        provenance = Counter()
        report.metadata["derivative_checks"] = provenance
        page_accounting: Counter[str] = Counter()
        report.metadata["page_accounting"] = page_accounting
        seen_request_ids: set[int] = set()
        content_omissions = 0
        no_requests = 0
        report.event("schedule", **report.metadata["sampling"])
        for index, partition in enumerate(selected, 1):
            if inventory is None:
                rows = iter(
                    reader.responses(str(partition["id"]), max_bytes=args.max_payload_bytes)
                )
            else:
                rows = reader.all_responses(
                    str(partition["id"]),
                    max_bytes=args.max_payload_bytes,
                    through_request_id=inventory["max_request_id"],
                    batch_size=args.response_batch_size,
                )
            accounting = PartitionPageAccounting()
            has_requests = False
            derivative_checked = False
            for row in rows:
                has_requests = True
                if inventory is not None:
                    request_id = int(row["api_request_log_id"])
                    if request_id in seen_request_ids or len(seen_request_ids) >= args.max_requests:
                        raise AuditIncompleteError("repeated_request_or_budget_exhausted")
                    seen_request_ids.add(request_id)
                    report.metadata["requests_checked"] = len(seen_request_ids)
                    accounting.observe(row)
                    content_omissions += int(
                        row.get("status_code") == 200
                        and (
                            row.get("raw_api_payload_id") is None
                            or bool(row.get("payload_omitted"))
                        )
                    )
                    payload = row.get("payload_json")
                    if row.get("status_code") == 200 and isinstance(payload, dict):
                        items = payload.get("items")
                        if isinstance(items, list) and all(
                            isinstance(item, dict) and isinstance(item.get("id"), str)
                            for item in items
                        ):
                            expected_seen = len({item["id"] for item in items})
                            actual_seen = seen_counts.pop(
                                (str(row["raw_api_payload_id"]), str(row["crawl_partition_id"])), 0
                            )
                            observation_counts["response_pages_checked"] += 1
                            observation_counts["expected_unique_items"] += expected_seen
                            observation_counts["seen_rows"] += actual_seen
                            observation_counts["response_count_mismatches"] += int(
                                expected_seen != actual_seen
                            )
                    if len(seen_request_ids) % 1000 == 0:
                        print(
                            f"requests_checked={len(seen_request_ids)}/{inventory['requests']}",
                            flush=True,
                        )
                row["coverage_status"] = partition["coverage_status"]
                planned = partition.get("params_json") or {}
                row["partition_search_params"] = planned.get("params", planned)
                row["partition_expected_run_id"] = partition["crawl_run_id"]
                row["partition_expected_id"] = partition["id"]
                report.inspect(row, parents, city_index)
                if (
                    row.get("raw_api_payload_id")
                    and not row.get("payload_omitted")
                    and row.get("status_code") == 200
                    and isinstance(row.get("payload_json"), dict)
                    and isinstance(row["payload_json"].get("items"), list)
                    and row["payload_json"]["items"]
                    and provenance["derivative_pages_checked"] < args.max_provenance_pages
                    and (inventory is None or not derivative_checked)
                ):
                    counts = inspect_derivatives(row, reader.derivatives(row["raw_api_payload_id"]))
                    provenance.update(counts)
                    report.event("derivatives", raw_id=row["raw_api_payload_id"], counts=counts)
                    derivative_checked = True
            if not has_requests:
                no_requests += 1
                report.event(
                    "partition_without_request",
                    partition_id=partition["id"],
                    run_id=partition["crawl_run_id"],
                    coverage_status=partition["coverage_status"],
                )
            if inventory is not None:
                accounting_counts = accounting.finish(partition)
                page_accounting.update(accounting_counts)
                report.event(
                    "page_accounting", partition_id=partition["id"], counts=accounting_counts
                )
            if index % 100 == 0:
                print(f"partitions_checked={index}/{len(selected)}", flush=True)
        report.metadata["partitions_without_request"] = no_requests
        report.metadata["derivative_checks"] = dict(provenance)
        report.metadata["page_accounting"] = dict(page_accounting)
        if inventory is not None:
            after = reader.request_inventory(str(runs[0]["id"]))
            report.metadata["request_inventory_after"] = after
            report.metadata["requests_checked"] = len(seen_request_ids)
            unchanged = (
                after == inventory
                and reader.runs(limit=args.max_runs, run_ids=[str(runs[0]["id"])]) == runs
                and reader.partitions(str(runs[0]["id"]), limit=args.metadata_limit_per_run + 1)
                == partitions
                and reader.seen_inventory(str(runs[0]["id"]), max_groups=args.max_requests * 2)
                == seen_snapshot
            )
            if not unchanged or len(seen_request_ids) != inventory["requests"]:
                raise AuditIncompleteError("source_changed_or_requests_not_fully_traversed")
            report.metadata["all_request_rows_visited"] = True
            observation_counts["unreconciled_seen_groups"] = len(seen_counts)
            observation_counts["unreconciled_seen_rows"] = sum(seen_counts.values())
            report.metadata["observation_count_checks"] = dict(observation_counts)
            report.metadata["successful_response_content_omissions"] = content_omissions
            report.metadata["sampling"]["is_full_response_audit"] = content_omissions == 0
    finally:
        engine.dispose()


def safe_chunk_path(root: Path, value: str) -> Path:
    candidate = (root / value).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError("archive path escapes root")
    return candidate


def collect_archive(args: argparse.Namespace, report: AuditReport) -> None:
    deadline = monotonic() + args.max_seconds
    reference = json.loads(args.area_reference.read_text(encoding="utf-8"))
    parents = {row["hh_area_id"]: row["parent_hh_area_id"] for row in reference["areas"]}
    city_index = build_address_city_index(reference["areas"])
    report.metadata["area_reference_captured_at"] = reference["captured_at"]
    root = args.archive_dir.resolve()
    manifests = list((root / "v1" / "bronze" / "raw_api_payload").rglob("*.manifest.json"))
    if not manifests:
        raise ValueError("no local raw manifests; cannot assess historical archive")
    manifests.sort(
        key=lambda p: hashlib.sha256(f"{args.seed}:{p.relative_to(root)}".encode()).hexdigest()
    )
    report.metadata["local_manifests_available"] = len(manifests)
    report.metadata["archive_sampling"] = "seeded chunk order; sequential rows within chunk"
    report.metadata["archive_limits"] = {
        "max_rows": args.max_rows,
        "max_manifests": args.max_manifests,
        "max_chunk_bytes": args.max_chunk_bytes,
        "is_full_corpus_audit": False,
    }
    counters: Counter[str] = Counter()
    report.metadata["archive_counts"] = counters
    seen: dict[str, str] = {}
    selected_runs = {str(value) for value in args.run_id}
    for manifest_file in manifests[: args.max_manifests]:
        if monotonic() >= deadline:
            raise TimeoutError("audit time budget exhausted")
        if counters["rows_read"] >= args.max_rows:
            break
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        chunk = safe_chunk_path(root, manifest["data_file"])
        counters["manifests_selected"] += 1
        if not chunk.is_file():
            counters["local_chunks_missing"] += 1
            continue
        if chunk.stat().st_size > args.max_chunk_bytes:
            counters["chunks_over_size_limit"] += 1
            continue
        with chunk.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        if digest != manifest["data_sha256"]:
            counters["chunk_checksum_mismatch"] += 1
            report.event("chunk_checksum_mismatch", manifest=str(manifest_file.relative_to(root)))
            continue
        counters["chunks_verified"] += 1
        with gzip.open(chunk, "rt", encoding="utf-8") as handle:
            while counters["rows_read"] < args.max_rows:
                if monotonic() >= deadline:
                    raise TimeoutError("audit time budget exhausted")
                line = handle.readline(args.max_payload_bytes + 1)
                if not line:
                    break
                if len(line.encode("utf-8")) > args.max_payload_bytes:
                    counters["chunks_stopped_on_large_line"] += 1
                    break
                counters["rows_read"] += 1
                row = json.loads(line)
                if row.get("request_type") != "vacancy_search":
                    counters["non_search_rows"] += 1
                    continue
                if selected_runs and str(row.get("crawl_run_id")) not in selected_runs:
                    counters["rows_outside_selected_runs"] += 1
                    continue
                raw_id = row.get("raw_api_payload_id")
                if raw_id is None:
                    counters["rows_without_raw_id"] += 1
                    continue
                digest = (
                    str(row.get("payload_hash")) + ":" + payload_digest(row.get("payload_json"))
                )
                if str(raw_id) in seen:
                    counters["duplicate_archive_raw_ids"] += 1
                    counters["duplicate_archive_raw_hash_conflicts"] += int(
                        seen[str(raw_id)] != digest
                    )
                    continue
                seen[str(raw_id)] = digest
                report.inspect(row, parents, city_index)
        print(
            f"archive_rows_read={counters['rows_read']} chunks={counters['chunks_verified']}",
            flush=True,
        )
    report.metadata["archive_counts"] = dict(counters)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    os.umask(0o077)
    report = AuditReport(args.output_dir)
    report.metadata["source"] = "database" if args.database else "local_archive"
    report.metadata["budgets"] = {
        "max_seconds": args.max_seconds,
        "max_partitions": args.max_partitions,
        "all_responses": args.all_responses,
        "max_requests": args.max_requests,
        "response_batch_size": args.response_batch_size,
        "max_provenance_pages": args.max_provenance_pages,
        "max_payload_bytes": args.max_payload_bytes,
        "statement_timeout_ms": args.statement_timeout_ms,
    }
    try:
        if args.database:
            collect_database(args, report)
        else:
            collect_archive(args, report)
    except Exception as error:
        # SQLAlchemy exceptions can include connection parameters: do not print their bodies.
        if isinstance(error, AuditIncompleteError):
            report.metadata["incomplete_reason"] = error.reason
        report.event("audit_incomplete", error_type=type(error).__name__)
        report.finish(status="incomplete")
        print(f"status=incomplete error_type={type(error).__name__} report={args.output_dir}")
        return 2
    report.finish(status="evidence_collected")
    print(f"status=evidence_collected responses={report.response_count} report={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
