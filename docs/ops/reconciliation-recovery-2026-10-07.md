# Reconciliation recovery (2026-10-07)

## Incident

Production run `38280dea-5262-4e92-ac9c-876bc15373e2` started on
2026-09-22. Search finished on 2026-09-24 at 11:07 UTC: 18,385 covered
terminal partitions and 3,961 split partitions; no pending/failed partitions.
At 12:04 UTC the kernel OOM killer terminated the Python process during
reconciliation (about 14.4 GiB anonymous RSS on a 15 GiB VPS without swap).
The run remained `created`, blocking weekly admission and detail catch-up.
On October 7 the first-detail backlog was 204,245.

The daily archive completed S3 verification, coverage audit, housekeeping and
local prune on October 5-7. This does not prove collection health: an archive
run with zero exported rows can succeed while collection is blocked.

## Fix

Reconciliation reads current states in UUID keyset pages of 1,000, queries
observed ids only for that page, and writes Core executemany updates instead
of accumulating dirty ORM instances. The observation count is computed in SQL.
Missing/inactive policy is unchanged. State updates and run completion remain
in the same transaction; pages are never committed individually. A failed
transaction can therefore be retried without double-incrementing missing counts.
Direct reconciliation of a run with `finished_at` already set is rejected.
No schema migration is required.

Local validation: Python 3.12.15, SQLAlchemy 2.0.48; full suite 253 passed,
19 skipped without services. The reconciliation integration test also passed
against an isolated PostgreSQL with one-row pages. New tests cover bounded
pages/ORM state, deduplicated observations, empty corpus, completed-run replay
rejection, and rollback/retry after a second-page failure. Mypy and Ruff
(`src tests scripts`) passed. Production-scale RSS/time must still be measured
during the supervised recovery.

## Recovery after deployment approval

First confirm the exact run is still `created`, all terminal partitions are
covered, and no process is executing it. Confirm a recent successful offsite
DB backup. Deploy the fix through the usual reviewed commit/push/pull flow.
Do not mark the run successful with SQL: reconciliation must actually finish.

Resume the same run with no selective detail stage:

```bash
cd /opt/hh_collector
docker compose --profile ops run --rm app resume-run-v2 \
  --run-id 38280dea-5262-4e92-ac9c-876bc15373e2 \
  --detail-limit 0 --detail-refresh-ttl-days 30 \
  --triggered-by reconciliation-recovery-20261007
```

Run this in a persistent terminal session. Monitor container memory and logs.
After success, check the DB run status and first-detail backlog. Existing
detail controller timers should start the three workers; search admission
waits for backlog drain. Verify the next weekly sweep actually starts.

Production recovery has NOT yet been performed. A separate follow-up is an
alert for a stale active run with no executor; successful `skipped_active_run`
controller ticks currently hide this failure.
