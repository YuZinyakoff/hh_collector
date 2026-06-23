# Collection Recovery - 2026-06-23

This runbook fixes the collection incident found on `2026-06-23`.

Storage, backup, restore, archive and S3 cleanup automation were healthy. The
collection layer was not: `scheduler` and `detail-worker` containers were not
running, and the latest completed `crawl_run` before recovery was the May
`vps-search-baseline`.

## Current Incident State

Known facts:

- May pilot/test corpus exists and is useful for operational evidence:
  `865868` vacancies, `848056` successful detail snapshots.
- Post-May production epoch with boundary `2026-06-01T00:00:00+00:00` had
  `0` collection rows before recovery.
- A supervised fresh search was started on `2026-06-23`:
  `run_id=bcf9ef54-27b0-4a90-bd33-728775053ea4`.
- That run did create fresh list data after `2026-06-23T12:00:00+00:00`, but
  the run finished as `failed` at `2026-06-23 12:15:58 UTC`.
- The immediate failure was an hh.ru HTTP `503` on one search page. Before the
  fix, `503` was treated as a normalization failure instead of a retryable
  transient upstream failure.

Do not start detail catch-up until the `5xx` fix is deployed and this failed
search run is resumed or otherwise intentionally closed. Detail only makes sense
after search coverage is either successful or intentionally bounded and
understood.

## Fixed Code Path

`status_code=0` and HTTP `5xx` responses are now classified as transport
responses. This makes search, detail and dictionary requests retry `5xx`
responses and lets list-engine/run-resume requeue failed partitions as transient
upstream failures.

HTTP `4xx` behavior is unchanged: captcha, not-found and bad-request style
responses remain hard/terminal outcomes.

## VPS Diagnostic Commands

Preferred command after pulling this repository revision to VPS:

```bash
cd /opt/hh_collector
make inspect-collection-run ARGS="--run-id bcf9ef54-27b0-4a90-bd33-728775053ea4 --start-ts 2026-06-23T12:00:00+00:00 --log /tmp/hhru-production-search-20260623.log"
```

Manual fallback:

Use variables first. This avoids broken UUID/timestamp copy-paste.

```bash
cd /opt/hh_collector
RUN_ID=bcf9ef54-27b0-4a90-bd33-728775053ea4
START_TS=2026-06-23T12:00:00+00:00
LOG=/tmp/hhru-production-search-20260623.log
```

Inspect the failed run log.

```bash
tail -120 "$LOG"
```

```bash
PATTERN='failed|error|exception|traceback|captcha|forbidden|429|403|502|completed|summary|status'
grep -Ei "$PATTERN" "$LOG" | tail -120
```

Check run and partition state.

```bash
docker compose exec -T postgres psql -U hhru -d hhru_platform -P pager=off -v ON_ERROR_STOP=1 -c "select id, run_type, status, triggered_by, started_at, finished_at from crawl_run where id = '$RUN_ID'::uuid;"
```

```bash
docker compose exec -T postgres psql -U hhru -d hhru_platform -P pager=off -v ON_ERROR_STOP=1 -c "select status, coverage_status, count(*) from crawl_partition where crawl_run_id = '$RUN_ID'::uuid group by status, coverage_status order by status, coverage_status;"
```

Use the built-in coverage reporters.

```bash
docker compose --profile ops run --rm app show-run-coverage --run-id "$RUN_ID"
```

```bash
docker compose --profile ops run --rm app show-run-tree --run-id "$RUN_ID" --max-rows 80
```

Check that fresh rows were actually written.

```bash
docker compose exec -T postgres psql -U hhru -d hhru_platform -P pager=off -v ON_ERROR_STOP=1 -c "select count(*) as raw_payloads_since_start from raw_api_payload where received_at >= '$START_TS'::timestamptz;"
```

```bash
docker compose exec -T postgres psql -U hhru -d hhru_platform -P pager=off -v ON_ERROR_STOP=1 -c "select count(*) as seen_events_since_start from vacancy_seen_event where seen_at >= '$START_TS'::timestamptz;"
```

```bash
docker compose exec -T postgres psql -U hhru -d hhru_platform -P pager=off -v ON_ERROR_STOP=1 -c "select count(*) as new_vacancies_since_start from vacancy_current_state where first_seen_at >= '$START_TS'::timestamptz;"
```

```bash
docker compose exec -T postgres psql -U hhru -d hhru_platform -P pager=off -v ON_ERROR_STOP=1 -c "select count(*) as short_snapshots_since_start from vacancy_snapshot where snapshot_type = 'short' and captured_at >= '$START_TS'::timestamptz;"
```

## Search Recovery Order

1. Deploy the `5xx` transport-classification fix to VPS.
2. Re-check the failed run from log, partition state and coverage report.
3. Resume the same run instead of starting a
   duplicate production sweep.
4. If the failure is not resumable after the fix, fix the blocker first, then start a new
   supervised search run with a new `triggered_by`.
5. Verify new rows by timestamp and verify `show-run-coverage`.
6. Only after search is understood, start detail smoke/catch-up.

Resume command, only after the diagnostic output shows this is safe:

```bash
tmux new -s prod-search-resume-20260623
```

Inside tmux:

```bash
cd /opt/hh_collector
RUN_ID=bcf9ef54-27b0-4a90-bd33-728775053ea4
LOG=/tmp/hhru-production-search-resume-20260623.log
date -Is | tee "$LOG"
time docker compose --profile ops run --rm app resume-run-v2 --run-id "$RUN_ID" --detail-limit 0 --triggered-by production-search-resume-20260623 2>&1 | tee -a "$LOG"
```

## Detail Recovery Order

Start detail only after search coverage is successful or explicitly accepted.

First smoke:

```bash
cd /opt/hh_collector
LOG=/tmp/hhru-production-detail-smoke-20260623.log
date -Is | tee "$LOG"
time docker compose --profile ops run --rm app drain-first-detail-backlog --limit 100 --include-inactive no --triggered-by production-detail-smoke-20260623 2>&1 | tee -a "$LOG"
```

If the smoke is clean, use supervised batches before enabling the long-running
service. The already validated catch-up candidate is `batch=100`, `interval=60`,
up to `scale=3`, but that validation was on the May pilot corpus. Reconfirm it
on fresh production rows before treating it as steady state.

## Background Collection Plan

The current compose `scheduler` default is not a production calendar policy:

- default interval is `3600` seconds;
- default `sync_dictionaries=yes`;
- default `detail_limit=100`;
- default `run_type=weekly_sweep`.

Do not run it for months with defaults.

The production path should be staged:

1. Supervised search run succeeds or its failure mode is understood.
2. Supervised detail smoke succeeds.
3. Explicit scheduler env is chosen and written to deployment config.
4. `scheduler` is enabled and checked after the first expected tick.
5. `detail-worker` is enabled only after the detail catch-up rate and search
   interference are acceptable.

Conservative scheduler shape for the first background proof:

```bash
cd /opt/hh_collector
printf '%s\n' 'HHRU_SCHEDULER_INTERVAL_SECONDS=604800' >> .env
printf '%s\n' 'HHRU_SCHEDULER_SYNC_DICTIONARIES=no' >> .env
printf '%s\n' 'HHRU_SCHEDULER_DETAIL_LIMIT=0' >> .env
printf '%s\n' 'HHRU_SCHEDULER_RUN_TYPE=production_weekly_sweep' >> .env
printf '%s\n' 'HHRU_SCHEDULER_TRIGGERED_BY=production-scheduler' >> .env
docker compose --profile ops up -d scheduler
```

Conservative detail-worker shape for the first background proof:

```bash
cd /opt/hh_collector
printf '%s\n' 'HHRU_DETAIL_WORKER_BATCH_SIZE=100' >> .env
printf '%s\n' 'HHRU_DETAIL_WORKER_INTERVAL_SECONDS=300' >> .env
printf '%s\n' 'HHRU_DETAIL_WORKER_INCLUDE_INACTIVE=no' >> .env
printf '%s\n' 'HHRU_DETAIL_WORKER_TRIGGERED_BY=production-detail-worker' >> .env
docker compose --profile ops up -d detail-worker
```

Before appending to `.env`, check whether these keys already exist and edit
existing values instead of duplicating them.

## Acceptance Criteria

Collection recovery is not complete until all are true:

- a post-`2026-06-23T12:00:00+00:00` search run is terminal and understood;
- `raw_api_payload`, `vacancy_seen_event`, `vacancy_snapshot` and
  `vacancy_current_state` show fresh timestamps;
- coverage is successful or the incomplete scope is explicitly accepted;
- first detail smoke has run on fresh rows;
- `scheduler` and `detail-worker` background policy is explicit, not default;
- `storage-state-snapshot` reports current collection containers and latest
  crawl runs so this failure mode is visible in future checks.
