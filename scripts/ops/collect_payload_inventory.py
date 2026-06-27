#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SAMPLE_ROWS = 100_000
DEFAULT_TOP_LIMIT = 80
DEFAULT_STATEMENT_TIMEOUT_MS = 600_000


@dataclass(frozen=True)
class InventoryQuery:
    name: str
    sql: str


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Collect a read-only raw payload and snapshot inventory before "
            "changing live DB retention policy."
        )
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root on the target host. Defaults to current directory.",
    )
    parser.add_argument(
        "--sample-rows",
        type=positive_int,
        default=DEFAULT_SAMPLE_ROWS,
        help=(
            "Recent rows to sample for expensive JSON key scans. "
            f"Defaults to {DEFAULT_SAMPLE_ROWS}."
        ),
    )
    parser.add_argument(
        "--top-limit",
        type=positive_int,
        default=DEFAULT_TOP_LIMIT,
        help=f"Maximum key rows to print per sampled inventory. Defaults to {DEFAULT_TOP_LIMIT}.",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=non_negative_int,
        default=DEFAULT_STATEMENT_TIMEOUT_MS,
        help=(
            "PostgreSQL statement timeout for every query. "
            f"Defaults to {DEFAULT_STATEMENT_TIMEOUT_MS} ms."
        ),
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Print header only and skip PostgreSQL queries.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    if not repo_root.exists():
        print(f"repo_root_not_found={repo_root}", file=sys.stderr)
        return 2

    print_section("payload inventory")
    print(f"repo_root={repo_root}")
    print("read_only=yes")
    print(f"sample_rows={args.sample_rows}")
    print(f"top_limit={args.top_limit}")
    print(f"statement_timeout_ms={args.statement_timeout_ms}")

    if args.skip_db:
        return 0

    for query in build_payload_inventory_queries(
        sample_rows=args.sample_rows,
        top_limit=args.top_limit,
    ):
        print_section(query.name)
        psql(
            repo_root,
            query.sql,
            statement_timeout_ms=args.statement_timeout_ms,
        )

    return 0


def build_payload_inventory_queries(
    *,
    sample_rows: int,
    top_limit: int,
) -> tuple[InventoryQuery, ...]:
    return (
        InventoryQuery(
            name="raw payload endpoint summary",
            sql="""
select coalesce(l.request_type, '-') as request_type,
       coalesce(p.endpoint_type, '-') as endpoint_type,
       count(*)::bigint as row_count,
       pg_size_pretty(sum(pg_column_size(p.payload_json))::bigint) as payload_json_size,
       sum(pg_column_size(p.payload_json))::bigint as payload_json_size_bytes,
       count(distinct p.payload_hash)::bigint as distinct_payload_hash_count,
       min(p.received_at) as min_received_at,
       max(p.received_at) as max_received_at
from raw_api_payload p
left join api_request_log l on l.id = p.api_request_log_id
group by coalesce(l.request_type, '-'), coalesce(p.endpoint_type, '-')
order by payload_json_size_bytes desc, row_count desc;
""",
        ),
        InventoryQuery(
            name="api request log summary",
            sql="""
select request_type,
       coalesce(status_code::text, '-') as status_code,
       count(*)::bigint as row_count,
       min(requested_at) as min_requested_at,
       max(requested_at) as max_requested_at,
       round(avg(latency_ms)::numeric, 2) as avg_latency_ms
from api_request_log
group by request_type, status_code
order by row_count desc, request_type, status_code;
""",
        ),
        InventoryQuery(
            name="raw vacancy detail top-level keys sampled",
            sql=f"""
with detail_payloads as materialized (
    select p.id,
           p.payload_json
    from raw_api_payload p
    left join api_request_log l on l.id = p.api_request_log_id
    where (
        l.request_type = 'vacancy_detail'
        or p.endpoint_type in ('vacancy_detail', 'vacancies.detail')
    )
      and jsonb_typeof(p.payload_json) = 'object'
    order by p.id desc
    limit {sample_rows}
),
sample_total as (
    select count(*)::numeric as total_rows
    from detail_payloads
),
key_counts as (
    select keys.key,
           count(*)::bigint as payload_count
    from detail_payloads p
    cross join lateral jsonb_object_keys(p.payload_json) as keys(key)
    group by keys.key
)
select key,
       payload_count,
       round(100.0 * payload_count::numeric / nullif(
           (select total_rows from sample_total), 0
       ), 2) as sample_percent
from key_counts
order by payload_count desc, key
limit {top_limit};
""",
        ),
        InventoryQuery(
            name="raw vacancy search response top-level keys sampled",
            sql=f"""
with search_payloads as materialized (
    select p.id,
           p.payload_json
    from raw_api_payload p
    left join api_request_log l on l.id = p.api_request_log_id
    where (
        l.request_type = 'vacancy_search'
        or p.endpoint_type in ('vacancy_search', 'vacancies.search')
    )
      and jsonb_typeof(p.payload_json) = 'object'
    order by p.id desc
    limit {sample_rows}
),
sample_total as (
    select count(*)::numeric as total_rows
    from search_payloads
),
key_counts as (
    select keys.key,
           count(*)::bigint as payload_count
    from search_payloads p
    cross join lateral jsonb_object_keys(p.payload_json) as keys(key)
    group by keys.key
)
select key,
       payload_count,
       round(100.0 * payload_count::numeric / nullif(
           (select total_rows from sample_total), 0
       ), 2) as sample_percent
from key_counts
order by payload_count desc, key
limit {top_limit};
""",
        ),
        InventoryQuery(
            name="raw vacancy search item top-level keys sampled",
            sql=f"""
with search_payloads as materialized (
    select p.id,
           p.payload_json
    from raw_api_payload p
    left join api_request_log l on l.id = p.api_request_log_id
    where (
        l.request_type = 'vacancy_search'
        or p.endpoint_type in ('vacancy_search', 'vacancies.search')
    )
      and jsonb_typeof(p.payload_json) = 'object'
    order by p.id desc
    limit {sample_rows}
),
search_items as materialized (
    select items.item
    from search_payloads p
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(p.payload_json -> 'items') = 'array'
                then p.payload_json -> 'items'
            else '[]'::jsonb
        end
    ) as items(item)
    where jsonb_typeof(items.item) = 'object'
),
sample_total as (
    select count(*)::numeric as total_rows
    from search_items
),
key_counts as (
    select keys.key,
           count(*)::bigint as item_count
    from search_items i
    cross join lateral jsonb_object_keys(i.item) as keys(key)
    group by keys.key
)
select key,
       item_count,
       round(100.0 * item_count::numeric / nullif(
           (select total_rows from sample_total), 0
       ), 2) as sample_percent
from key_counts
order by item_count desc, key
limit {top_limit};
""",
        ),
        InventoryQuery(
            name="detail snapshot payload top-level keys sampled",
            sql=f"""
with detail_snapshots as materialized (
    select id,
           normalized_json -> 'payload' as payload
    from vacancy_snapshot
    where snapshot_type = 'detail'
      and jsonb_typeof(normalized_json -> 'payload') = 'object'
    order by id desc
    limit {sample_rows}
),
sample_total as (
    select count(*)::numeric as total_rows
    from detail_snapshots
),
key_counts as (
    select keys.key,
           count(*)::bigint as snapshot_count
    from detail_snapshots s
    cross join lateral jsonb_object_keys(s.payload) as keys(key)
    group by keys.key
)
select key,
       snapshot_count,
       round(100.0 * snapshot_count::numeric / nullif(
           (select total_rows from sample_total), 0
       ), 2) as sample_percent
from key_counts
order by snapshot_count desc, key
limit {top_limit};
""",
        ),
        InventoryQuery(
            name="short snapshot payload top-level keys sampled",
            sql=f"""
with short_snapshots as materialized (
    select id,
           normalized_json -> 'payload' as payload
    from vacancy_snapshot
    where snapshot_type = 'short'
      and jsonb_typeof(normalized_json -> 'payload') = 'object'
    order by id desc
    limit {sample_rows}
),
sample_total as (
    select count(*)::numeric as total_rows
    from short_snapshots
),
key_counts as (
    select keys.key,
           count(*)::bigint as snapshot_count
    from short_snapshots s
    cross join lateral jsonb_object_keys(s.payload) as keys(key)
    group by keys.key
)
select key,
       snapshot_count,
       round(100.0 * snapshot_count::numeric / nullif(
           (select total_rows from sample_total), 0
       ), 2) as sample_percent
from key_counts
order by snapshot_count desc, key
limit {top_limit};
""",
        ),
        InventoryQuery(
            name="vacancy snapshot summary",
            sql="""
select snapshot_type,
       coalesce(change_reason, '-') as change_reason,
       count(*)::bigint as row_count,
       count(distinct vacancy_id)::bigint as vacancy_count,
       pg_size_pretty(sum(pg_column_size(normalized_json))::bigint) as normalized_json_size,
       sum(pg_column_size(normalized_json))::bigint as normalized_json_size_bytes,
       min(captured_at) as min_captured_at,
       max(captured_at) as max_captured_at
from vacancy_snapshot
group by snapshot_type, coalesce(change_reason, '-')
order by row_count desc, snapshot_type, change_reason;
""",
        ),
        InventoryQuery(
            name="vacancy snapshot raw reference summary",
            sql="""
select snapshot_type,
       count(*)::bigint as row_count,
       count(short_payload_ref_id)::bigint as short_payload_ref_count,
       count(detail_payload_ref_id)::bigint as detail_payload_ref_count,
       count(*) filter (
           where normalized_json ->> 'schema_version' = '2'
             and jsonb_typeof(normalized_json -> 'payload') = 'object'
       )::bigint as full_payload_snapshot_count
from vacancy_snapshot
group by snapshot_type
order by snapshot_type;
""",
        ),
        InventoryQuery(
            name="live vacancy field coverage",
            sql="""
select 'vacancy.name_current' as field_name,
       count(*) filter (where name_current is not null)::bigint as filled_count,
       count(*)::bigint as total_count,
       round(
           100.0 * count(*) filter (where name_current is not null)::numeric
           / nullif(count(*), 0),
           2
       ) as filled_percent
from vacancy
union all
select 'vacancy.employer_id',
       count(*) filter (where employer_id is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where employer_id is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.area_id',
       count(*) filter (where area_id is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where area_id is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.published_at',
       count(*) filter (where published_at is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where published_at is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.created_at_hh',
       count(*) filter (where created_at_hh is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where created_at_hh is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.archived_at_hh',
       count(*) filter (where archived_at_hh is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where archived_at_hh is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.alternate_url',
       count(*) filter (where alternate_url is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where alternate_url is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.employment_type_code',
       count(*) filter (where employment_type_code is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where employment_type_code is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.schedule_type_code',
       count(*) filter (where schedule_type_code is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where schedule_type_code is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.experience_code',
       count(*) filter (where experience_code is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where experience_code is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy.source_type',
       count(*) filter (where source_type is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where source_type is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy
union all
select 'vacancy_current_state.last_short_hash',
       count(*) filter (where last_short_hash is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where last_short_hash is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy_current_state
union all
select 'vacancy_current_state.last_detail_hash',
       count(*) filter (where last_detail_hash is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where last_detail_hash is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy_current_state
union all
select 'vacancy_current_state.last_detail_fetched_at',
       count(*) filter (where last_detail_fetched_at is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where last_detail_fetched_at is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy_current_state
union all
select 'vacancy_current_state.detail_fetch_status',
       count(*) filter (where detail_fetch_status is not null)::bigint,
       count(*)::bigint,
       round(
           100.0 * count(*) filter (where detail_fetch_status is not null)::numeric
           / nullif(count(*), 0),
           2
       )
from vacancy_current_state
order by field_name;
""",
        ),
    )


def psql(repo_root: Path, sql: str, *, statement_timeout_ms: int) -> None:
    db_user = os.environ.get("HHRU_DB_USER", "hhru")
    db_name = os.environ.get("HHRU_DB_NAME", "hhru_platform")
    run_and_print(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "-e",
            f"PGOPTIONS=-c statement_timeout={statement_timeout_ms}",
            "postgres",
            "psql",
            "-U",
            db_user,
            "-d",
            db_name,
            "-P",
            "pager=off",
            "-v",
            "ON_ERROR_STOP=1",
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


def print_section(title: str) -> None:
    print(f"=== {title} ===")


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be greater than or equal to zero")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
