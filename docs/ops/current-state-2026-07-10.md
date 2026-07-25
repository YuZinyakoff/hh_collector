# Current State Snapshot - 2026-07-10

Read-only operational snapshot from VPS `/opt/hh_collector` after the first
automatic weekly production collection cycle.

Primary observation time: `2026-07-10T08:33:49+00:00`.
Corpus accounting time: `2026-07-09T17:42:48+00:00`.
Production-cycle boundary: `2026-07-07T08:19:25+00:00`.

## Operational Health

- `systemctl --failed --no-pager`: `0` failed units.
- No transient `hh_collector-app-run` containers remained.
- Daily backup completed successfully at `2026-07-10 02:47:34 UTC`.
- Daily research archive completed successfully at `2026-07-10 03:45:53 UTC`.
- Active first-detail backlog was `0`; the controller reported
  `idle_empty_backlog`.
- All six production timers were scheduled: detail catch-up, production search,
  daily backup, daily archive, weekly restore drill and weekly S3 cleanup.

## Proven Production Cycle

`crawl_run` `707e2420-f3c6-4268-ba15-34bde49a16c1`:

- run type: `production_weekly_sweep`;
- status: `succeeded`;
- started: `2026-07-07T08:19:25.064867+00:00`;
- finished: `2026-07-08T11:44:05.719905+00:00`.

The search created `206824` first-detail candidates. Automatic detail catch-up
at scale `3` drained this active backlog to zero.

The July 9 archive run reached `max_export_batches_exhausted=50`. A rerun with
a larger limit completed all steps, and the next scheduled archive run on July
10 completed normally, including guarded housekeeping apply. This demonstrates
recovery, but the batch-limit scenario remains a follow-up observation point.

## Backup And Archive Evidence

Scheduled backup run `20260710T003021Z-1421527` completed backup creation,
local verification, S3 sync, exact S3 verification, then
`prune-local-verified-dumps`. Its
`hhru-platform_hhru_platform_20260710T003023Z.dump` was removed locally only
after the exact remote verification; manifests and receipts remain for S3
integrity and restore drills.

Scheduled archive run `20260710T024734Z-1507335` completed export, local
verify, S3 sync, S3 verify, coverage audit, housekeeping preview and
housekeeping apply. Export settled after `400000`, `9102`, then `0` rows.

At the preceding storage snapshot, local manifest/receipt evidence estimated
`8` known backup generations, `138.25 GiB` expected uploaded offsite backup
data and `136.13 GiB` verified offsite backup data. One older `20260517`
generation remained unverified and was fail-safe retained.

## Capacity And Corpus

| Resource | Size |
| --- | ---: |
| Root filesystem | `154G` total, `99G` used, `56G` free (`64%` used) |
| `.state` | `25G` |
| PostgreSQL volume | `64G` |
| PostgreSQL database | `63 GB` |

The largest database tables were `vacancy_snapshot` (`31 GB`) and
`raw_api_payload` (`22 GB`), so historical collection data is the primary
capacity driver.

| Corpus metric | Value |
| --- | ---: |
| Vacancies | `2,037,738` |
| Successful details | `2,013,747` |
| Terminal 404 details | `23,991` |
| Vacancy snapshots | `4,710,844` |
| Seen events | `9,260,066` |
| Raw API payloads | `1,537,174` |

Since the production-cycle boundary, the system recorded `206824` newly
first-seen vacancies, `205809` successful detail snapshots and `451385` short
snapshots.

## Conclusion

The first complete unattended cycle succeeded: search, detail catch-up,
verified archive, guarded housekeeping, verified S3 backup and automatic local
dump pruning all completed without remaining backlog or failed units.

The operating mode can now be alert-driven with a short weekly review. A
months-long hands-off claim still needs several more weekly cycles, the next
restore-drill/S3-cleanup pair, and a measured capacity growth rate.
