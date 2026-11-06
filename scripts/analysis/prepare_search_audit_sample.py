#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from time import monotonic
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

MIB = 1024 * 1024
PREFIX = "v1/bronze/raw_api_payload/request_type=vacancy_search/"
MANIFEST_LIMIT = 64 * 1024
SCHEMA = "search-audit-sample-v1"


class SampleError(ValueError):
    """Messages are controlled diagnostic codes, never remote exception strings."""


def positive(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def safe_key(value: object) -> str:
    if not isinstance(value, str) or "\\" in value:
        raise SampleError("invalid_object_key")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or ".." in path.parts
        or str(path) != value
        or not value.startswith(PREFIX)
    ):
        raise SampleError("invalid_object_key")
    return value


def safe_path(root: Path, key: str) -> Path:
    result = (root / safe_key(key)).resolve()
    if not result.is_relative_to(root.resolve()):
        raise SampleError("path_escapes_sample_root")
    return result


def digest_string(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise SampleError("invalid_sha256")
    return value


def size_value(value: object) -> int:
    if type(value) is not int or value < 1:
        raise SampleError("invalid_size_or_row_count")
    return value


def make_plan(
    root: Path, *, per_month: int, max_bytes: int, max_chunk_bytes: int, seed: str
) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    available: Counter[str] = Counter()
    excluded: Counter[str] = Counter()
    seen_data: set[str] = set()
    source = root / PREFIX
    for path in sorted(source.rglob("*.manifest.json")):
        key = path.relative_to(root).as_posix()
        if not path.resolve().is_relative_to(root.resolve()):
            raise SampleError("manifest_escapes_source_root")
        if path.stat().st_size > MANIFEST_LIMIT:
            raise SampleError("manifest_over_size_limit")
        with path.open("rb") as handle:
            raw = handle.read(MANIFEST_LIMIT + 1)
        if len(raw) > MANIFEST_LIMIT:
            raise SampleError("manifest_over_size_limit")
        document = json.loads(raw)
        if (
            document.get("dataset_key") != "bronze/raw_api_payload"
            or document.get("partition", {}).get("request_type") != "vacancy_search"
        ):
            raise SampleError("unexpected_manifest_dataset")
        observed = datetime.fromisoformat(document["source_min_observed_at"])
        if observed.utcoffset() is None:
            raise SampleError("manifest_observation_timezone_missing")
        month = observed.strftime("%Y-%m")
        available[month] += 1
        data_key = safe_key(document["data_file"])
        if data_key in seen_data:
            raise SampleError("duplicate_data_key_in_catalog")
        seen_data.add(data_key)
        size = size_value(document["data_size_bytes"])
        if size > max_chunk_bytes:
            excluded[month] += 1
            continue
        groups[month].append(
            {
                "month": month,
                "manifest_key": safe_key(key),
                "data_key": data_key,
                "manifest_bytes": len(raw),
                "manifest_sha256": hashlib.sha256(raw).hexdigest(),
                "data_bytes": size,
                "data_sha256": digest_string(document["data_sha256"]),
                "row_count": size_value(document["row_count"]),
            }
        )
    if not available:
        raise SampleError("no_search_manifests_found")
    pools = {
        month: deque(
            sorted(
                entries,
                key=lambda e: hashlib.sha256(f"{seed}:{e['manifest_key']}".encode()).hexdigest(),
            )
        )
        for month, entries in groups.items()
    }
    selected = []
    total = 0
    budget_skips: Counter[str] = Counter()
    for _ in range(per_month):
        for month in sorted(pools):
            while pools[month]:
                entry = pools[month].popleft()
                cost = entry["manifest_bytes"] + entry["data_bytes"]
                if total + cost <= max_bytes:
                    selected.append(entry)
                    total += cost
                    break
                budget_skips[month] += 1
    counts = Counter(entry["month"] for entry in selected)
    return {
        "schema": SCHEMA,
        "created_at": datetime.now(UTC).isoformat(),
        "method": "month round-robin; seeded chunk order; size and total byte caps",
        "seed": seed,
        "live_network_requests": 0,
        "max_total_bytes": max_bytes,
        "max_chunk_bytes": max_chunk_bytes,
        "chunks_per_month": per_month,
        "planned_download_bytes": total,
        "planned_rows": sum(e["row_count"] for e in selected),
        "months": [
            {
                "month": m,
                "available_chunks": n,
                "selected_chunks": counts[m],
                "over_chunk_size_limit": excluded[m],
                "skipped_for_budget": budget_skips[m],
            }
            for m, n in sorted(available.items())
        ],
        "entries": selected,
        "limitations": "Local manifest catalog only; chunk sample, not a full-history certificate.",
    }


def validate_plan(plan: dict[str, Any], *, max_bytes: int) -> None:
    if plan.get("schema") != SCHEMA or not plan.get("entries"):
        raise SampleError("invalid_or_empty_plan")
    total = 0
    keys: set[str] = set()
    for entry in plan["entries"]:
        for kind in ("manifest", "data"):
            key = safe_key(entry[f"{kind}_key"])
            if key in keys:
                raise SampleError("duplicate_plan_object_key")
            keys.add(key)
            digest_string(entry[f"{kind}_sha256"])
            size = size_value(entry[f"{kind}_bytes"])
            if kind == "manifest" and size > MANIFEST_LIMIT:
                raise SampleError("manifest_over_size_limit")
            if kind == "data" and size > size_value(plan["max_chunk_bytes"]):
                raise SampleError("chunk_over_plan_size_limit")
            total += size
    if total != plan.get("planned_download_bytes") or total > min(
        max_bytes, size_value(plan["max_total_bytes"])
    ):
        raise SampleError("plan_byte_budget_mismatch")


def build_remote() -> tuple[Any, str, str]:
    import boto3
    from botocore.config import Config

    from hhru_platform.config.settings import get_settings

    settings = get_settings()
    if settings.research_archive_offsite_backend.strip().lower() != "s3":
        raise SampleError("unsupported_archive_backend")

    def setting(suffix: str) -> str:
        return (
            str(getattr(settings, f"research_archive_offsite_s3_{suffix}") or "").strip()
            or str(getattr(settings, f"backup_offsite_s3_{suffix}") or "").strip()
        )

    endpoint, bucket = setting("endpoint_url").rstrip("/"), setting("bucket")
    access, secret = setting("access_key_id"), setting("secret_access_key")
    if not all((endpoint, bucket, access, secret)):
        raise SampleError("missing_archive_s3_settings")
    if not endpoint.startswith("https://"):
        raise SampleError("archive_endpoint_requires_https")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=setting("region") or "ru-1",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=60,
            retries={"mode": "standard", "total_max_attempts": 3},
        ),
    )
    prefix = "/".join(
        part for part in settings.research_archive_offsite_root.strip().split("/") if part
    )
    return client, bucket, prefix


def download_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    destination: Path,
    expected_bytes: int,
    expected_sha256: str,
    deadline: float,
) -> None:
    if monotonic() >= deadline:
        raise SampleError("download_time_budget_exhausted")
    if destination.exists():
        raise SampleError("destination_already_exists")
    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != expected_bytes:
        raise SampleError("remote_head_size_mismatch")
    if monotonic() >= deadline:
        raise SampleError("download_time_budget_exhausted")
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".partial")
    body = None
    created_partial = False
    try:
        # Streaming reads, not the transfer manager: enforce byte caps even if HEAD races GET.
        with partial.open("xb") as handle:
            created_partial = True
            response = client.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            if response["ContentLength"] != expected_bytes:
                raise SampleError("remote_get_size_mismatch")
            size, digest = 0, hashlib.sha256()
            while True:
                if monotonic() >= deadline:
                    raise SampleError("download_time_budget_exhausted")
                block = body.read(min(64 * 1024, expected_bytes - size + 1))
                if not block:
                    break
                size += len(block)
                if size > expected_bytes:
                    raise SampleError("remote_body_over_size_limit")
                digest.update(block)
                handle.write(block)
            if size != expected_bytes or digest.hexdigest() != expected_sha256:
                raise SampleError("download_size_or_sha256_mismatch")
        partial.replace(destination)
    finally:
        try:
            if body is not None:
                body.close()
        finally:
            if created_partial:
                partial.unlink(missing_ok=True)


def download_plan(
    plan: dict[str, Any], root: Path, *, max_bytes: int, seconds: int
) -> dict[str, Any]:
    validate_plan(plan, max_bytes=max_bytes)
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    report: dict[str, Any] = {"status": "incomplete", "verified_chunks": 0, "verified_bytes": 0}
    deadline = monotonic() + seconds
    try:
        client, bucket, prefix = build_remote()
        for entry in plan["entries"]:
            for kind in ("manifest", "data"):
                relative = safe_key(entry[f"{kind}_key"])
                destination = safe_path(root, relative)
                download_object(
                    client,
                    bucket=bucket,
                    key=f"{prefix}/{relative}" if prefix else relative,
                    destination=destination,
                    expected_bytes=entry[f"{kind}_bytes"],
                    expected_sha256=entry[f"{kind}_sha256"],
                    deadline=deadline,
                )
                report["verified_bytes"] += entry[f"{kind}_bytes"]
                if kind == "manifest":
                    document = json.loads(destination.read_bytes())
                    if (
                        document.get("data_file") != entry["data_key"]
                        or document.get("data_sha256") != entry["data_sha256"]
                        or document.get("data_size_bytes") != entry["data_bytes"]
                    ):
                        raise SampleError("remote_manifest_differs_from_plan")
            report["verified_chunks"] += 1
            print(
                f"verified_chunks={report['verified_chunks']}/{len(plan['entries'])} "
                f"month={entry['month']}",
                flush=True,
            )
        report["status"] = "download_verified"
    except Exception as error:
        report["error_type"] = type(error).__name__
        if isinstance(error, SampleError):
            report["error_code"] = str(error)
        else:
            response = getattr(error, "response", None)
            if isinstance(response, dict):
                details = response.get("Error")
                code = details.get("Code") if isinstance(details, dict) else None
                if isinstance(code, str) and code in {
                    "NoSuchKey",
                    "NoSuchBucket",
                    "AccessDenied",
                    "404",
                    "403",
                    "SignatureDoesNotMatch",
                }:
                    report["error_code"] = code
    (root.parent / "download-report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plan or download a bounded read-only S3 audit sample."
    )
    modes = parser.add_subparsers(dest="mode", required=True)
    plan_parser = modes.add_parser("plan", help="Local metadata only, no credentials or network.")
    plan_parser.add_argument("--archive-dir", type=Path, required=True)
    plan_parser.add_argument("--output-dir", type=Path, required=True)
    plan_parser.add_argument("--chunks-per-month", type=positive, default=12)
    plan_parser.add_argument("--max-total-mib", type=positive, default=256)
    plan_parser.add_argument("--max-chunk-mib", type=positive, default=32)
    plan_parser.add_argument("--seed", default="historical-search-audit-v1")
    download_parser = modes.add_parser(
        "download", help="Explicit S3 HEAD/GET only; no uploads/deletes."
    )
    download_parser.add_argument("--plan-file", type=Path, required=True)
    download_parser.add_argument("--max-total-mib", type=positive, default=256)
    download_parser.add_argument("--max-seconds", type=positive, default=900)
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        if args.mode == "plan":
            plan = make_plan(
                args.archive_dir.resolve(),
                per_month=args.chunks_per_month,
                max_bytes=args.max_total_mib * MIB,
                max_chunk_bytes=args.max_chunk_mib * MIB,
                seed=args.seed,
            )
            args.output_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
            (args.output_dir / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
            print(
                json.dumps(
                    {key: value for key, value in plan.items() if key != "entries"}, indent=2
                )
            )
            return 0
        plan = json.loads(args.plan_file.read_text())
        report = download_plan(
            plan,
            args.plan_file.resolve().parent / "archive",
            max_bytes=args.max_total_mib * MIB,
            seconds=args.max_seconds,
        )
        print(json.dumps(report, indent=2))
        return 0 if report["status"] == "download_verified" else 2
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "incomplete",
                    "error_type": type(error).__name__,
                    "error_code": str(error) if isinstance(error, SampleError) else "not_disclosed",
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
