# Current State Snapshot - 2026-06-23

Read-only snapshot from VPS `/opt/hh_collector`.

Snapshot time: `2026-06-23T11:31:31+00:00`.

Corpus boundary for production accounting:
`2026-06-01T00:00:00+00:00`.

Source command:

```bash
make storage-state-snapshot \
  ARGS="--boundary-utc 2026-06-01T00:00:00+00:00"
```

## 1. Operational Health

Storage/archive systemd services are healthy:

- failed units: `0`;
- running one-shot `hh_collector-app-run` containers: `0`;
- `hhru-daily-backup.service`: `Result=success`, `ExecMainStatus=0`;
- `hhru-research-archive.service`: `Result=success`, `ExecMainStatus=0`;
- `hhru-weekly-backup-restore-drill.service`: `Result=success`, `ExecMainStatus=0`;
- `hhru-weekly-backup-offsite-cleanup.service`: `Result=success`, `ExecMainStatus=0`.

Active timers:

| Timer | Last Run UTC | Next Run UTC | Purpose |
| --- | --- | --- | --- |
| `hhru-daily-backup.timer` | `2026-06-23 00:32:37` | `2026-06-24 00:38:30` | DB dump, local verify, S3 sync, S3 verify |
| `hhru-research-archive.timer` | `2026-06-23 02:34:31` | `2026-06-24 02:38:10` | settled export, verify, S3 sync, audit, preview |
| `hhru-weekly-backup-restore-drill.timer` | `2026-06-21 06:26:41` | `2026-06-28 06:00:37` | offsite restore drill |
| `hhru-weekly-backup-offsite-cleanup.timer` | `2026-06-21 08:56:51` | `2026-06-28 08:44:47` | bounded S3 backup retention cleanup |

Collection is not healthy as of this snapshot:

- `docker compose ps scheduler detail-worker` shows no running collection
  containers;
- `crawl_run` contains only one run:
  `c7e7d8c6-6813-454c-845e-ca44539da1e8`, `weekly_sweep`, `succeeded`,
  `triggered_by=vps-search-baseline`, started `2026-05-13`, finished
  `2026-05-14`;
- corpus timestamps stop in May: latest `raw_api_payload.received_at` and
  latest detail snapshot are `2026-05-27`.

Conclusion: backup/archive/restore/cleanup automation is working, but new
vacancy collection has not been running since the May pilot flow.

Latest weekly restore drill on `2026-06-21` restored
`.state/backups/hhru-platform_hhru_platform_20260621T003633Z.dump` from S3,
verified schema `5/5`, and cleaned up the temporary restore database.

Latest weekly S3 backup cleanup on `2026-06-21` succeeded after the restore-drill
success marker:

- `apply=yes`;
- `keep_latest=3`;
- `keep_weekly=4`;
- `scanned_receipt_count=12`;
- `deleted_generation_count=5`;
- `retained_generation_count=6`;
- `skipped_generation_count=1`;
- `remote_deleted_object_count=995`;
- `local_deleted_sidecar_count=20`.

## 2. Local Disk And Database

Host filesystem:

- `/dev/sda1`: `154G` total, `101G` used, `54G` available, `66%`;
- `/opt/hh_collector/.state`: `63G`;
- `.state/backups`: `50G`;
- `.state/archive`: `13G`;
- `.state/archive/research-production-v2`: `7.1G`;
- `.state/logs`: `50M`.

Docker volumes:

| Volume | Size |
| --- | ---: |
| `hh_collector_postgres_data` | `29G` |
| `hh_collector_prometheus_data` | `158M` |
| `hh_collector_grafana_data` | `53M` |
| `hh_collector_redis_data` | `16K` |
| `hh_collector_alertmanager_data` | `4K` |

PostgreSQL database:

- database: `hhru_platform`;
- size: `27 GB` / `29119724567` bytes.

Largest core tables by total size:

| Table | Total Size | Heap Size |
| --- | ---: | ---: |
| `vacancy_snapshot` | `13 GB` | `334 MB` |
| `raw_api_payload` | `12 GB` | `161 MB` |
| `vacancy_seen_event` | `733 MB` | `384 MB` |
| `vacancy_current_state` | `440 MB` | `303 MB` |
| `api_request_log` | `427 MB` | `343 MB` |
| `vacancy` | `392 MB` | `248 MB` |
| `detail_fetch_attempt` | `243 MB` | `112 MB` |

Interpretation: the DB is dominated by indexed historical payload/snapshot
tables, not by Prometheus, Grafana, Redis, or logs.

## 3. S3 / Offsite Footprint

Backup prefix estimate from local manifests and receipts:

- backup generation count known locally: `9`;
- uploaded receipt generations: `9`;
- verified receipt generations: `8`;
- expected uploaded remote backup bytes: `108125785263` / `100.70 GiB`;
- verified remote backup bytes: `105856784620` / `98.59 GiB`.

Current known backup generations:

| Generation | Local Dump | Uploaded | Verified | Size |
| --- | --- | --- | --- | ---: |
| `20260517T151543Z` | no | yes | no | `2.11 GiB` |
| `20260528T112018Z` | no | yes | yes | `12.32 GiB` |
| `20260607T004343Z` | no | yes | yes | `12.32 GiB` |
| `20260614T004152Z` | no | yes | yes | `12.32 GiB` |
| `20260619T003853Z` | no | yes | yes | `12.32 GiB` |
| `20260620T003839Z` | yes | yes | yes | `12.32 GiB` |
| `20260621T003633Z` | yes | yes | yes | `12.32 GiB` |
| `20260622T004308Z` | yes | yes | yes | `12.32 GiB` |
| `20260623T003239Z` | yes | yes | yes | `12.32 GiB` |

The old `20260517T151543Z` generation remains fail-safe skipped because there is
no matching successful verification receipt.

Research archive offsite state:

- canonical archive: `.state/archive/research-production-v2`;
- local verified manifests: `1557/1557`;
- logical data size: `7508484645` bytes;
- total row count: `6885371`;
- S3 sync is idempotent at this point: `uploaded_manifest_count=0`,
  `skipped_manifest_count=1557`;
- latest offsite verify: `verified_object_count=3162`,
  `verified_checkpoint_count=47`, `verification_receipt_count=1557`.

## 4. Corpus Accounting

All current live DB counts:

| Metric | Count |
| --- | ---: |
| `vacancy_total` | `865868` |
| `current_state_total` | `865868` |
| `current_state_with_detail_fetched` | `865868` |
| `current_state_detail_status_succeeded` | `848056` |
| `detail_fetch_attempt_total` | `865884` |
| `detail_fetch_attempt_succeeded` | `848056` |
| `vacancies_with_success_detail_attempt` | `848056` |
| `vacancies_with_detail_snapshot` | `848056` |
| `vacancy_snapshot_total` | `1720257` |
| `vacancy_snapshot_short` | `872201` |
| `vacancy_snapshot_detail` | `848056` |
| `seen_event_total` | `2309443` |
| `raw_api_payload_total` | `994886` |

Detail status distribution:

| Status | Count |
| --- | ---: |
| `succeeded` | `848056` |
| `terminal_404` | `17812` |

Post-boundary counts since `2026-06-01T00:00:00+00:00`:

| Metric | Count |
| --- | ---: |
| `vacancies_first_seen_since_boundary` | `0` |
| `vacancies_last_seen_since_boundary` | `0` |
| `detail_attempts_since_boundary` | `0` |
| `detail_attempts_succeeded_since_boundary` | `0` |
| `short_snapshots_since_boundary` | `0` |
| `detail_snapshots_since_boundary` | `0` |
| `raw_payloads_since_boundary` | `0` |

Interpretation: the existing `865868` vacancies and `848056` successful detail
snapshots are operational pilot/test corpus evidence from the May baseline and
detail drain. Under the current `2026-06-01` timestamp boundary, the new
production collection epoch has no rows because collection was not running.

This does not mean the project has no data. The local notebook/S3 analysis smoke
used the existing pilot/test archive data. The boundary only answers a narrower
question: "how many rows were observed or written after the agreed
post-May-test cutoff?"

## 5. Current Conclusion

Storage, backup, restore, archive, S3 sync, S3 verify, and guarded S3 backup
cleanup are production-capable as an operational foundation.

The active gap is collection: start a clearly bounded production search/detail
epoch and then verify fresh `crawl_run`, `raw_api_payload`,
`vacancy_seen_event`, `vacancy_snapshot` and detail attempt timestamps.
