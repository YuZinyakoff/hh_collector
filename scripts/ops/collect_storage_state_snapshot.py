#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


DEFAULT_BOUNDARY_UTC = "2026-06-01T00:00:00+00:00"
TIMER_UNITS = (
    "hhru-daily-backup.timer",
    "hhru-research-archive.timer",
    "hhru-weekly-backup-restore-drill.timer",
    "hhru-weekly-backup-offsite-cleanup.timer",
)
SERVICE_UNITS = (
    "hhru-daily-backup.service",
    "hhru-research-archive.service",
    "hhru-weekly-backup-restore-drill.service",
    "hhru-weekly-backup-offsite-cleanup.service",
)


@dataclass(frozen=True)
class BackupGeneration:
    dump_name: str
    backup_size_bytes: int
    part_count: int
    has_local_dump: bool
    has_upload_receipt: bool
    has_verified_receipt: bool
    has_keep_marker: bool


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Collect a read-only VPS storage, backup and corpus state snapshot."
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root on the target host. Defaults to current directory.",
    )
    parser.add_argument(
        "--boundary-utc",
        default=DEFAULT_BOUNDARY_UTC,
        help=(
            "Timestamp boundary for post-pilot corpus counts. "
            f"Defaults to {DEFAULT_BOUNDARY_UTC}."
        ),
    )
    parser.add_argument(
        "--since",
        default="2026-06-21 00:00:00 UTC",
        help="systemd journal lower bound for recent service output.",
    )
    parser.add_argument(
        "--skip-systemd",
        action="store_true",
        help="Skip systemd sections.",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip PostgreSQL queries.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    if not repo_root.exists():
        print(f"repo_root_not_found={repo_root}", file=sys.stderr)
        return 2

    print_section("snapshot")
    print(f"repo_root={repo_root}")
    print(f"boundary_utc={args.boundary_utc}")

    print_basic_host_state(repo_root=repo_root, since=args.since, skip_systemd=args.skip_systemd)
    print_local_storage(repo_root)
    print_backup_generation_summary(repo_root / ".state" / "backups")
    print_latest_archive_summary(repo_root)
    print_latest_cleanup_summary(repo_root)

    if not args.skip_db:
        print_database_summary(repo_root=repo_root, boundary_utc=args.boundary_utc)

    return 0


def print_basic_host_state(*, repo_root: Path, since: str, skip_systemd: bool) -> None:
    print_section("now")
    run_and_print(["date", "-Is"], cwd=repo_root)
    run_and_print(["uptime"], cwd=repo_root)

    if skip_systemd:
        return

    print_section("timers")
    run_and_print(["systemctl", "list-timers", *TIMER_UNITS, "--all", "--no-pager"], cwd=repo_root)

    print_section("failed units")
    run_and_print(["systemctl", "--failed", "--no-pager"], cwd=repo_root)

    print_section("running app containers")
    run_and_print(
        [
            "docker",
            "ps",
            "--filter",
            "name=hh_collector-app-run",
            "--format",
            "table {{.Names}}\t{{.Status}}",
        ],
        cwd=repo_root,
    )

    print_section("service results")
    for unit in SERVICE_UNITS:
        print(f"--- {unit} ---")
        run_and_print(
            [
                "systemctl",
                "show",
                unit,
                "--property=Result",
                "--property=ExecMainStatus",
                "--property=ActiveState",
                "--no-pager",
            ],
            cwd=repo_root,
        )

    print_section("service journal")
    for unit in SERVICE_UNITS:
        print(f"--- {unit} ---")
        run_and_print(
            ["journalctl", "-u", unit, "--since", since, "-n", "120", "--no-pager"],
            cwd=repo_root,
        )


def print_local_storage(repo_root: Path) -> None:
    print_section("host disk")
    run_and_print(["df", "-h", str(repo_root)], cwd=repo_root)
    for path in (
        repo_root / ".state",
        repo_root / ".state" / "backups",
        repo_root / ".state" / "archive",
        repo_root / ".state" / "archive" / "research-production-v2",
        repo_root / ".state" / "logs",
    ):
        if path.exists():
            run_and_print(["du", "-sh", str(path)], cwd=repo_root)

    print_section("docker volumes")
    volume_result = run_command(
        ["docker", "volume", "ls", "--format", "{{.Name}}"],
        cwd=repo_root,
        check=False,
    )
    if volume_result.returncode != 0:
        print(volume_result.stderr.strip())
        return
    for volume_name in sorted(volume_result.stdout.splitlines()):
        if not volume_name.startswith("hh_collector_"):
            continue
        volume_path = Path("/var/lib/docker/volumes") / volume_name / "_data"
        if volume_path.exists():
            result = run_command(["du", "-sh", str(volume_path)], cwd=repo_root, check=False)
            if result.stdout.strip():
                print(f"{result.stdout.strip()} volume={volume_name}")


def print_backup_generation_summary(backup_dir: Path) -> None:
    print_section("local backup generations")
    generations = collect_backup_generations(backup_dir)
    total_manifest_size = sum(generation.backup_size_bytes for generation in generations)
    uploaded_generations = [
        generation for generation in generations if generation.has_upload_receipt
    ]
    verified_generations = [
        generation for generation in generations if generation.has_verified_receipt
    ]
    uploaded_size = sum(generation.backup_size_bytes for generation in uploaded_generations)
    verified_size = sum(generation.backup_size_bytes for generation in verified_generations)

    print(f"backup_dir={backup_dir}")
    print(f"generation_count={len(generations)}")
    print(f"manifest_total_size_bytes={total_manifest_size}")
    print(f"manifest_total_size_gib={bytes_to_gib(total_manifest_size):.2f}")
    print(f"uploaded_receipt_generation_count={len(uploaded_generations)}")
    print(f"expected_remote_backup_size_bytes={uploaded_size}")
    print(f"expected_remote_backup_size_gib={bytes_to_gib(uploaded_size):.2f}")
    print(f"verified_receipt_generation_count={len(verified_generations)}")
    print(f"verified_remote_backup_size_bytes={verified_size}")
    print(f"verified_remote_backup_size_gib={bytes_to_gib(verified_size):.2f}")

    for generation in generations:
        print(
            f"backup_generation={generation.dump_name} "
            f"local_dump={'yes' if generation.has_local_dump else 'no'} "
            f"uploaded={'yes' if generation.has_upload_receipt else 'no'} "
            f"verified={'yes' if generation.has_verified_receipt else 'no'} "
            f"keep={'yes' if generation.has_keep_marker else 'no'} "
            f"backup_size_bytes={generation.backup_size_bytes} "
            f"backup_size_gib={bytes_to_gib(generation.backup_size_bytes):.2f} "
            f"part_count={generation.part_count}"
        )


def collect_backup_generations(backup_dir: Path) -> list[BackupGeneration]:
    generations: list[BackupGeneration] = []
    if not backup_dir.exists():
        return generations

    for manifest_path in sorted(backup_dir.glob("*.dump.manifest.json")):
        manifest = load_json_object(manifest_path)
        dump_name = manifest_path.name.removesuffix(".manifest.json")
        dump_path = backup_dir / dump_name
        generations.append(
            BackupGeneration(
                dump_name=dump_name,
                backup_size_bytes=int(manifest.get("backup_size_bytes") or 0),
                part_count=manifest_part_count(manifest),
                has_local_dump=dump_path.exists(),
                has_upload_receipt=Path(f"{dump_path}.offsite.json").exists(),
                has_verified_receipt=Path(f"{dump_path}.offsite.verified.json").exists(),
                has_keep_marker=Path(f"{dump_path}.offsite.keep").exists(),
            )
        )
    return generations


def manifest_part_count(manifest: dict[str, Any]) -> int:
    part_count = manifest.get("part_count")
    if isinstance(part_count, int):
        return part_count
    parts = manifest.get("parts")
    if isinstance(parts, list):
        return len(parts)
    return 0


def print_latest_archive_summary(repo_root: Path) -> None:
    print_section("latest research archive")
    archive_run = latest_child_dir(repo_root / ".state" / "logs" / "research-archive-daily")
    if archive_run is None:
        print("archive_run=-")
        return

    print(f"archive_run={archive_run}")
    local_verify = latest_json_event(
        archive_run / "local-verify.log",
        "verify_research_archive.succeeded",
    )
    offsite_verify = latest_json_event(
        archive_run / "offsite-verify.log",
        "verify_research_archive_offsite.succeeded",
    )
    offsite_sync = latest_json_event(
        archive_run / "offsite-sync.log",
        "sync_research_archive_offsite.succeeded",
    )
    housekeeping = read_key_values(archive_run / "housekeeping-preview.log")

    print_selected_json("archive_local_verify", local_verify)
    print_selected_json("archive_offsite_sync", offsite_sync)
    print_selected_json("archive_offsite_verify", offsite_verify)
    for key in (
        "status",
        "coverage_status",
        "coverage_issue_count",
        "total_candidates",
        "total_action_count",
    ):
        if key in housekeeping:
            print(f"archive_housekeeping_{key}={housekeeping[key]}")


def print_latest_cleanup_summary(repo_root: Path) -> None:
    print_section("latest s3 backup cleanup")
    cleanup_run = latest_child_dir(repo_root / ".state" / "logs" / "backup-offsite-cleanup")
    if cleanup_run is None:
        print("cleanup_run=-")
        return

    print(f"cleanup_run={cleanup_run}")
    cleanup_values = read_key_values(cleanup_run / "cleanup.log")
    cleanup_json = latest_json_event(
        cleanup_run / "cleanup.log",
        "cleanup_backup_offsite.succeeded",
    )
    print_selected_json("backup_cleanup", cleanup_json)
    for key in (
        "status",
        "apply",
        "keep_latest",
        "keep_weekly",
        "scanned_receipt_count",
        "delete_candidate_count",
        "deleted_generation_count",
        "retained_generation_count",
        "skipped_generation_count",
        "remote_deleted_object_count",
        "local_deleted_sidecar_count",
    ):
        if key in cleanup_values:
            print(f"backup_cleanup_{key}={cleanup_values[key]}")


def print_database_summary(*, repo_root: Path, boundary_utc: str) -> None:
    print_section("database size")
    psql(
        repo_root,
        """
select current_database() as db_name,
       pg_size_pretty(pg_database_size(current_database())) as db_size,
       pg_database_size(current_database()) as db_size_bytes;
""",
    )

    print_section("core table sizes")
    psql(
        repo_root,
        """
select relname as table_name,
       pg_size_pretty(pg_total_relation_size(oid)) as total_size,
       pg_total_relation_size(oid) as total_size_bytes,
       pg_size_pretty(pg_relation_size(oid)) as heap_size,
       pg_relation_size(oid) as heap_size_bytes
from pg_class
where relkind = 'r'
  and relname in (
    'crawl_run',
    'crawl_partition',
    'api_request_log',
    'raw_api_payload',
    'vacancy',
    'vacancy_seen_event',
    'vacancy_current_state',
    'vacancy_snapshot',
    'detail_fetch_attempt'
  )
order by pg_total_relation_size(oid) desc;
""",
    )

    print_section("corpus counts all")
    psql(
        repo_root,
        """
select 'vacancy_total' as metric, count(*)::text as value from vacancy
union all select 'current_state_total', count(*)::text from vacancy_current_state
union all select 'current_state_with_detail_fetched', count(*)::text
  from vacancy_current_state where last_detail_fetched_at is not null
union all select 'current_state_detail_status_succeeded', count(*)::text
  from vacancy_current_state where detail_fetch_status = 'succeeded'
union all select 'vacancies_with_detail_snapshot', count(distinct vacancy_id)::text
  from vacancy_snapshot where snapshot_type = 'detail'
union all select 'vacancy_snapshot_total', count(*)::text from vacancy_snapshot
union all select 'vacancy_snapshot_short', count(*)::text
  from vacancy_snapshot where snapshot_type = 'short'
union all select 'vacancy_snapshot_detail', count(*)::text
  from vacancy_snapshot where snapshot_type = 'detail'
union all select 'raw_api_payload_total', count(*)::text from raw_api_payload
union all select 'raw_payload_vacancy_detail', count(*)::text
  from raw_api_payload where endpoint_type = 'vacancy_detail'
union all select 'detail_fetch_attempt_total', count(*)::text from detail_fetch_attempt
union all select 'detail_fetch_attempt_succeeded', count(*)::text
  from detail_fetch_attempt where status = 'succeeded'
union all select 'vacancies_with_success_detail_attempt', count(distinct vacancy_id)::text
  from detail_fetch_attempt where status = 'succeeded'
union all select 'seen_event_total', count(*)::text from vacancy_seen_event
order by metric;
""",
    )

    boundary_literal = sql_literal(boundary_utc)
    print_section("corpus counts since boundary")
    psql(
        repo_root,
        f"""
select 'boundary_utc' as metric, {boundary_literal} as value
union all select 'vacancies_first_seen_since_boundary', count(*)::text
  from vacancy_current_state where first_seen_at >= {boundary_literal}::timestamptz
union all select 'vacancies_last_seen_since_boundary', count(*)::text
  from vacancy_current_state where last_seen_at >= {boundary_literal}::timestamptz
union all select 'vacancies_detail_fetched_since_boundary', count(distinct vacancy_id)::text
  from detail_fetch_attempt
  where status = 'succeeded' and finished_at >= {boundary_literal}::timestamptz
union all select 'detail_attempts_since_boundary', count(*)::text
  from detail_fetch_attempt where requested_at >= {boundary_literal}::timestamptz
union all select 'detail_attempts_succeeded_since_boundary', count(*)::text
  from detail_fetch_attempt
  where status = 'succeeded' and requested_at >= {boundary_literal}::timestamptz
union all select 'short_snapshots_since_boundary', count(*)::text
  from vacancy_snapshot
  where snapshot_type = 'short' and captured_at >= {boundary_literal}::timestamptz
union all select 'detail_snapshots_since_boundary', count(*)::text
  from vacancy_snapshot
  where snapshot_type = 'detail' and captured_at >= {boundary_literal}::timestamptz
union all select 'raw_payloads_since_boundary', count(*)::text
  from raw_api_payload where received_at >= {boundary_literal}::timestamptz
union all select 'raw_detail_payloads_since_boundary', count(*)::text
  from raw_api_payload
  where endpoint_type = 'vacancy_detail' and received_at >= {boundary_literal}::timestamptz
order by metric;
""",
    )

    print_section("detail status distribution")
    psql(
        repo_root,
        """
select detail_fetch_status, count(*)::bigint
from vacancy_current_state
group by detail_fetch_status
order by count(*) desc, detail_fetch_status;
""",
    )

    print_section("crawl run summary")
    psql(
        repo_root,
        """
select to_char(started_at at time zone 'UTC', 'YYYY-MM') as started_month,
       run_type,
       status,
       count(*)::bigint as run_count,
       min(started_at) as first_started_at,
       max(started_at) as last_started_at
from crawl_run
group by started_month, run_type, status
order by started_month, run_type, status;
""",
    )


def psql(repo_root: Path, sql: str) -> None:
    db_user = os.environ.get("HHRU_DB_USER", "hhru")
    db_name = os.environ.get("HHRU_DB_NAME", "hhru_platform")
    run_and_print(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            db_user,
            "-d",
            db_name,
            "-P",
            "pager=off",
            "-c",
            sql.strip(),
        ],
        cwd=repo_root,
    )


def run_and_print(args: Sequence[str], *, cwd: Path) -> None:
    result = run_command(args, cwd=cwd, check=False)
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)
    if result.returncode != 0:
        print(f"command_exit_code={result.returncode}", file=sys.stderr)


def run_command(args: Sequence[str], *, cwd: Path, check: bool) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(args),
            cwd=cwd,
            check=check,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError as exc:
        return subprocess.CompletedProcess(
            args=list(args),
            returncode=127,
            stdout="",
            stderr=str(exc),
        )


def latest_child_dir(parent: Path) -> Path | None:
    if not parent.exists():
        return None
    children = sorted(path for path in parent.iterdir() if path.is_dir())
    return children[-1] if children else None


def latest_json_event(path: Path, event_name: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not path.exists():
        return result
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("event") == event_name:
            result = payload
    return result


def print_selected_json(prefix: str, payload: dict[str, Any]) -> None:
    if not payload:
        print(f"{prefix}_event=-")
        return
    for key in (
        "status",
        "timestamp",
        "duration_ms",
        "backup_size_bytes",
        "scanned_backup_count",
        "uploaded_backup_count",
        "skipped_backup_count",
        "verified_object_count",
        "scanned_manifest_count",
        "verified_manifest_count",
        "verified_checkpoint_count",
        "verification_receipt_count",
        "total_row_count",
        "total_data_size_bytes",
        "checkpoint_uploaded_count",
        "candidate_manifest_count",
        "uploaded_manifest_count",
        "skipped_manifest_count",
        "scanned_receipt_count",
        "delete_candidate_count",
        "deleted_generation_count",
        "retained_generation_count",
        "skipped_generation_count",
        "remote_deleted_object_count",
        "local_deleted_sidecar_count",
    ):
        if key in payload:
            print(f"{prefix}_{key}={payload[key]}")


def read_key_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line or line.startswith("{"):
            continue
        key, value = line.split("=", 1)
        if key and " " not in key:
            values[key] = value
    return values


def load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def bytes_to_gib(value: int) -> float:
    return value / (1024**3)


def print_section(title: str) -> None:
    print(f"=== {title} ===")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


if __name__ == "__main__":
    raise SystemExit(main())
