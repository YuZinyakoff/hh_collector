# Research Archive v1

Цель: сохранить долгосрочный датасет hh.ru так, чтобы он был одновременно
компактным, проверяемым и пригодным для анализа без восстановления live PostgreSQL.

Этот контур не заменяет PostgreSQL backup. Backup нужен для disaster recovery.
Research archive является data product.

## 1. Design Decision

Archive v1 строится как lake-style контур:

1. `bronze`: canonical raw layer.
2. `silver`: normalized/index layer для анализа и поиска нужных raw payloads.
3. `gold`: optional derived analytical datasets, explicitly out of scope for
   the first implementation slice.

Главный принцип: raw payloads сохраняются, но не остаются единственным способом
работы с данными. Аналитик должен начинать с compact index/normalized datasets и
переходить к raw JSON только для replay, расследования или извлечения новых полей.

## 2. What Not To Do

Не делать:

- просто `pg_dump` как research archive;
- один гигантский JSON файл;
- S3 directory с произвольными старыми JSON без manifest/inventory;
- удаление live DB rows только потому, что "что-то загружено в S3";
- Parquet для raw API payload как первый обязательный формат, пока схема HH JSON
  и наши normalized schemas ещё меняются.

## 3. Storage Layout

Recommended S3 layout:

```text
s3://<bucket>/hhru-platform/research-archive/v1/
  bronze/raw_api_payload/
    request_type=vacancy_search/year=2026/month=05/day=26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json
    request_type=vacancy_detail/year=2026/month=05/day=26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json

  silver/api_request_log/
    year=2026/month=05/day=26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json
  silver/vacancy/
    snapshot_date=2026-05-26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json
  silver/vacancy_snapshot/
    snapshot_type=short/year=2026/month=05/day=26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json
    snapshot_type=detail/year=2026/month=05/day=26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json
  silver/vacancy_seen_event/
    year=2026/month=05/day=26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json
  silver/vacancy_current_state/
    snapshot_date=2026-05-26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json
  silver/detail_fetch_attempt/
    year=2026/month=05/day=26/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json

  inventory/
    archive-inventory.jsonl

  checkpoints/
    archive_kind=production/
      20260526T000000000000Z.checkpoint.json
```

Use `jsonl.gz` for v1 because it is compact enough, streamable, inspectable with
standard tools, and requires no new runtime dependency. Add Parquet later as a
derived analytical layer, not as the canonical raw archive.

## 4. Bronze Raw Record Contract

Each `bronze/raw_api_payload` line is an envelope, not just naked HH JSON.

Required fields:

```json
{
  "archive_schema_version": "research-archive-v1",
  "dataset": "raw_api_payload",
  "raw_api_payload_id": 123,
  "api_request_log_id": 456,
  "crawl_run_id": "uuid-or-null",
  "crawl_partition_id": "uuid-or-null",
  "request_type": "vacancy_detail",
  "endpoint_type": "vacancy_detail",
  "endpoint": "/vacancies/123",
  "method": "GET",
  "params_json": {},
  "status_code": 200,
  "latency_ms": 96,
  "requested_at": "2026-05-26T10:00:00+00:00",
  "response_received_at": "2026-05-26T10:00:00.096000+00:00",
  "entity_hh_id": "123",
  "payload_hash": "sha256-or-current-hash",
  "received_at": "2026-05-26T10:00:00.100000+00:00",
  "payload_json": {}
}
```

Rationale:

- The archive is self-describing without joining back to PostgreSQL.
- `payload_hash` allows integrity checks and future content-addressed dedup.
- Request metadata makes raw payloads searchable by endpoint, request type,
  status, crawl run, vacancy id and time.

## 5. Silver Dataset Contracts

Silver datasets should be flat enough for analysis and small enough to scan
frequently. They should reference raw payloads by ids/hashes, not duplicate the
full payload JSON.

### `silver/api_request_log`

Purpose: request-level performance, errors and coverage analysis.

Fields:

- `api_request_log_id`
- `crawl_run_id`
- `crawl_partition_id`
- `request_type`
- `endpoint`
- `method`
- `params_json`
- `status_code`
- `latency_ms`
- `attempt`
- `requested_at`
- `response_received_at`
- `error_type`
- `error_message`
- `raw_api_payload_id`
- `payload_hash`

### `silver/vacancy`

Purpose: canonical vacancy dimension as observed by the collector.

Fields:

- `vacancy_id`
- `hh_vacancy_id`
- `name_current`
- `employer_id`
- `area_id`
- `published_at`
- `created_at_hh`
- `archived_at_hh`
- `alternate_url`
- `employment_type_code`
- `schedule_type_code`
- `experience_code`
- `source_type`
- `created_at`
- `updated_at`

### `silver/vacancy_snapshot`

Purpose: history of short/detail snapshot changes.

Fields:

- `snapshot_id`
- `vacancy_id`
- `hh_vacancy_id`
- `snapshot_type`
- `captured_at`
- `crawl_run_id`
- `short_hash`
- `detail_hash`
- `short_payload_ref_id`
- `detail_payload_ref_id`
- `short_payload_hash`
- `detail_payload_hash`
- `change_reason`
- selected normalized fields useful for common filters

Do not inline full `normalized_json` blindly into every analytical export unless
there is a clear consumer. If included in v1, keep it as one JSON field and rely
on bronze raw for full replay.

### `silver/vacancy_seen_event`

Purpose: search observation history.

Fields:

- `seen_event_id`
- `vacancy_id`
- `hh_vacancy_id`
- `crawl_run_id`
- `crawl_partition_id`
- `seen_at`
- `list_position`
- `short_hash`
- `short_payload_ref_id`
- `short_payload_hash`

### `silver/vacancy_current_state`

Purpose: point-in-time current-state snapshot for analysis.

Partition by `snapshot_date`, not by row update time.

Fields:

- `snapshot_date`
- `vacancy_id`
- `hh_vacancy_id`
- `first_seen_at`
- `last_seen_at`
- `seen_count`
- `consecutive_missing_runs`
- `is_probably_inactive`
- `last_seen_run_id`
- `last_short_hash`
- `last_detail_hash`
- `last_detail_fetched_at`
- `detail_fetch_status`
- `updated_at`

Do not export active lease owner/expires as research data except in operational
debug bundles. Leases are runtime state, not domain observation.

### `silver/detail_fetch_attempt`

Purpose: detail-fetch quality and completeness analysis.

Fields:

- `detail_fetch_attempt_id`
- `vacancy_id`
- `hh_vacancy_id`
- `crawl_run_id`
- `reason`
- `attempt`
- `status`
- `requested_at`
- `finished_at`
- `error_message`

The current PostgreSQL table does not persist `request_log_id`, `raw_payload_id`
or `payload_hash` on the attempt row. Add those links only through an explicit
schema evolution if attempt-to-request lineage becomes a requirement.

## 6. Manifest Contract

Every chunk has a sidecar manifest.

Required manifest fields:

- `archive_schema_version`
- `dataset`
- `layer`: `bronze` or `silver`
- `partition`
- `chunk_index`
- `created_at`
- `source_database`
- `source_git_revision`
- `source_command`
- `row_count`
- `source_min_id`
- `source_max_id`
- `source_min_observed_at`
- `source_max_observed_at`
- `data_file`
- `data_size_bytes`
- `data_sha256`
- `compression`: `gzip`
- `format`: `jsonl`
- `columns`: for silver datasets

Manifests are small control-plane files and should be kept with the data file
both locally and in S3.

## 7. Inventory Contract

`inventory/archive-inventory.jsonl` is append-only. One line per exported chunk.

Fields:

- `archive_schema_version`
- `dataset`
- `layer`
- `partition`
- `manifest_file`
- `data_file`
- `row_count`
- `data_size_bytes`
- `data_sha256`
- `source_min_id`
- `source_max_id`
- `source_min_observed_at`
- `source_max_observed_at`
- `created_at`
- `status`: `exported`, `uploaded`, `verified`, `deprecated`

Inventory makes the archive discoverable without listing every S3 prefix and
lets an operator answer: "where are detail payloads for this date/run?".

## 8. Verification And Readback

An archive bundle is not trusted until:

1. Local export completed.
2. Local manifest row count and sha256 match the data file.
3. S3 upload completed.
4. Remote manifest and data object exist.
5. Remote sizes match manifest.
6. Readback check downloads at least the manifest and either:
   - full data file for small chunks; or
   - ranged/sample read plus checksum for larger chunks when supported.
7. Inventory was updated with `status=verified`.

Only verified bundles can be used as a prerequisite for live DB housekeeping.

Incremental exports additionally write checkpoint files. A checkpoint records
the per-dataset source cursor transition and referenced chunk manifests for one
run. Full S3 sync uploads checkpoints after chunks and inventory. Full offsite
verify writes `<checkpoint>.offsite.verified.json`; the coverage audit requires
matching checkpoint and chunk receipts before reporting `status=complete`.

## 9. Compaction And Space Policy

Archive v1 reduces storage pressure by avoiding unnecessary duplication:

- raw payload is stored once in bronze;
- silver stores flat metadata and payload references, not full payload copies;
- chunks are gzip-compressed;
- partitions are date/request-type based to support targeted reads;
- future content-addressed raw dedup by `payload_hash` is allowed, but not required
  for v1.

Expected practical tradeoff:

- Bronze is larger but canonical.
- Silver is smaller and used for most analysis.
- Gold/Parquet can be regenerated from bronze + silver.

If space becomes the bottleneck, the next optimization should be content-addressed
raw payload dedup:

```text
bronze/raw_payload_blob/hash_prefix=ab/hash=<payload_hash>.json.gz
bronze/raw_payload_observation_index/year=2026/month=05/day=26/chunk-000001.jsonl.gz
```

This should not be v1 unless measured duplicate raw payload volume justifies the
extra complexity.

## 10. Pilot Corpus Policy

The current VPS corpus is pilot/test data. It should not silently become the first
production archive.

Allowed choices:

1. Preserve one explicitly labeled pilot evidence bundle:
   `archive_kind=pilot_evidence`.
2. Keep only DB backup/offsite evidence and start production from a clean DB.
3. Export a small sampled archive for tool validation only.

Do not mix pilot corpus with canonical production archive unless every bundle is
explicitly labeled as pilot/test.

## 11. Implementation Plan

Terminology note: do not call this an MVP in project planning. The intended
target is a production-grade initial archive contract. "v1" means a stable first
format version that future tooling must remain able to read.

### Stage A: archive foundation

Scope:

- Add `export-research-archive` command.
- Stream rows from PostgreSQL in batches.
- Write `.jsonl.gz` data chunks.
- Write `.manifest.json` sidecars.
- Write append-only local `archive-inventory.jsonl`.
- Do not delete live DB rows.
- Add unit tests for manifest, inventory and row serialization.

Initial datasets:

- `bronze/raw_api_payload`
- `silver/api_request_log`
- `silver/vacancy`
- `silver/vacancy_snapshot`
- `silver/vacancy_seen_event`
- `silver/vacancy_current_state`

- `silver/detail_fetch_attempt`

Do not implement:

- text feature extraction;
- AI exposure scoring;
- region/week panels;
- Parquet;
- live DB cleanup based on archive receipts.

### Stage B: local validation

- Add `verify-research-archive` command.
- Check gzip readability.
- Check JSONL parse.
- Check row count against manifest.
- Check data file sha256 against manifest.
- Check required manifest fields.
- Check local inventory includes the chunk.

Local smoke commands:

```bash
make export-research-archive ARGS="--limit-per-dataset 1000 --chunk-size 500 --archive-kind tool_validation"
make verify-research-archive
```

For direct local execution without Compose:

```bash
make run-export-research-archive ARGS="--limit-per-dataset 1000 --chunk-size 500 --archive-kind tool_validation"
make run-verify-research-archive
```

Status after Stage B on 2026-05-27:

- `export-research-archive` exists for the Stage A foundation datasets.
- `verify-research-archive` exists for local manifest, gzip, checksum, row count
  and inventory validation.
- S3 upload/readback was not implemented yet at this stage.
- Research analytics, Parquet and gold datasets remain intentionally out of scope.

VPS smoke on 2026-05-28:

```bash
make export-research-archive ARGS="--limit-per-dataset 1000 --chunk-size 500 --archive-kind tool_validation --triggered-by vps-archive-smoke"
make verify-research-archive ARGS="--limit 20 --triggered-by vps-archive-smoke-verify"
```

Result:

- export succeeded with `6000` rows, `13` chunks and `5212503` data bytes;
- local verification succeeded for `13/13` manifests;
- archive size was about `5.2M`, with `27` files;
- dataset coverage was the Stage A foundation set:
  `bronze/raw_api_payload`, `silver/api_request_log`, `silver/vacancy`,
  `silver/vacancy_snapshot`, `silver/vacancy_seen_event`,
  `silver/vacancy_current_state`.

This smoke proves local Archive v1 export/verify mechanics. It does not make the
pilot corpus canonical production data.

### Stage C: S3 upload and verification

- Implemented commands:
  - `sync-research-archive-offsite`
  - `verify-research-archive-offsite`
- S3 client patterns are reused from backup offsite tooling.
- Sync uploads data files and manifests; full sync also uploads inventory.
- Partial sync via `--limit` or explicit `--manifest-file` deliberately does not
  upload inventory, because inventory would point at chunks that may not yet be
  present remotely.
- Verify checks remote object sizes for selected manifests.
- Verify performs bounded readback through `--readback-limit`: downloaded chunks
  are checked for size, sha256 and gzip JSONL row count.
- Uploads are idempotent through local per-manifest receipts:
  `<chunk>.manifest.json.offsite.json`.
- Successful remote verification writes a separate local proof receipt:
  `<chunk>.manifest.json.offsite.verified.json`. It records the verified remote
  object paths, local hashes and whether that chunk received full readback.

VPS S3 smoke command for the existing small tool-validation bundle:

```bash
make sync-research-archive-offsite ARGS="--triggered-by vps-archive-offsite-smoke"
make verify-research-archive-offsite ARGS="--readback-limit 2 --triggered-by vps-archive-offsite-smoke-verify"
```

For a bounded remote smoke on a large archive, use `--limit N`; note that this
will not upload or verify inventory:

```bash
make sync-research-archive-offsite ARGS="--limit 5 --triggered-by vps-archive-offsite-smoke"
make verify-research-archive-offsite ARGS="--limit 5 --readback-limit 2 --triggered-by vps-archive-offsite-smoke-verify"
```

Status on 2026-05-31:

- Code and unit tests for S3 sync/verify/readback are implemented.
- VPS full S3 sync passed for the existing `tool_validation` bundle:
  `uploaded_manifest_count=13`; data files, manifests and inventory were
  uploaded under `/hhru-platform/research-archive`.
- VPS remote verify passed: `verified_manifest_count=13`,
  `verified_object_count=27`.
- Bounded readback passed for `2/2` selected chunks: remote download, size,
  sha256, gzip JSONL parse and row count were checked.
- Repeated full sync was idempotent:
  `candidate_manifest_count=0`, `uploaded_manifest_count=0`,
  `skipped_manifest_count=13`. Full sync still refreshes inventory by design.
- Per-chunk offsite verification receipts are implemented. Re-run the VPS verify
  after deploying this change to create them for the existing tool-validation
  bundle.

VPS receipt smoke on 2026-06-01:

- remote verification succeeded for `13/13` manifests;
- `verification_receipt_count=13`;
- `verified_object_count=27`;
- bounded full readback succeeded for `2/2` selected chunks.

### Stage D: proof-of-read smoke

This is not analytics product work. It only proves the archive is usable as a
dataset. `verify-research-archive-offsite --readback-limit` is the first bounded
proof-of-read smoke.

Minimum object-level proof passed on VPS on 2026-05-31 with
`--readback-limit 2`. A later silver semantic sanity read may be added when the
first production archive bundle exists; it is not a blocker for the current
archive foundation.

- Read one `silver` chunk.
- Print row count, min/max observed date and one or two distinct-count sanity
  metrics, e.g. distinct areas or vacancies.
- No panels, no text models, no gold layer.

### Stage E: production archive safety

- Implemented: write per-chunk "archive verified" receipts after S3 object-size
  verification and record whether full readback was performed.
- Implemented: non-destructive incremental export mode for append-only datasets.
  The watermark is derived only from completed local checkpoints with the same
  `archive_kind`; chunk manifests written by an interrupted export do not
  advance it.
  Export stops before the first source row newer than the explicit settled cutoff,
  so a fresh row cannot be skipped by later watermark advancement.
- Implemented: chunk buffering is bounded by both the requested row count and a
  `32 MiB` serialized-byte ceiling. Large raw payloads therefore create smaller
  chunks instead of growing the exporter process until OOM.
- Implemented: every incremental export writes a control-plane checkpoint under
  `v1/checkpoints/archive_kind=<kind>/`. It records the per-dataset cursor
  transition, chunk list and settled cutoff even when a dataset has no new rows.
- Implemented: full S3 sync uploads checkpoints together with inventory, and full
  offsite verification checks their remote sizes and writes matching local
  checkpoint receipts. Partial sync/verify deliberately omit these completeness
  artifacts.
- Implemented: `audit-research-archive-coverage` validates the checkpoint chain
  from source cursor `0` and requires a matching S3 verification receipt for every
  referenced chunk and checkpoint. It is non-destructive and exits non-zero with
  `status=incomplete` on any gap or missing receipt.
- Implemented: `preview-research-archive-housekeeping` first runs the complete
  coverage audit and only then reports age-based `raw_api_payload` and
  `vacancy_snapshot` candidates bounded by each dataset's verified
  `source_id_covered`. It is read-only and fail-closed.
- Implemented: the same read-only preview reports old finished-run candidates
  separately. A run is excluded from the action list while it owns any
  `vacancy_seen_event.id` above the verified seen-event cursor; selected
  partition and seen-event cascade counts are reported explicitly.
- Implemented: `silver/detail_fetch_attempt` participates in the append-only
  settled incremental checkpoint chain and the read-only preview bounds
  detail-attempt retention candidates by its verified source-id cursor.
- Implemented: destructive apply is exposed only through the separate
  `apply-research-archive-housekeeping --apply` command. It requires
  `archive_kind=production`, the canonical
  `/hhru-platform/research-archive` offsite root, reruns the verified-coverage
  preview inside the delete transaction, replans exact bounded ids and locks
  selected run-tree roots before cascade deletion. Isolated smoke bundles cannot
  authorize apply.
- Add operator runbook for archive-before-delete.

Initial operator cadence:

1. Once per day, export the append-only suffix older than a `24h` safety window:

   ```bash
   make export-research-archive \
     ARGS="--incremental --settled-delay-hours 24 --archive-kind production --triggered-by daily-production-archive"
   ```

2. Verify locally, sync to S3 and verify remotely:

   ```bash
   make verify-research-archive ARGS="--triggered-by daily-production-archive-local-verify"
   make sync-research-archive-offsite ARGS="--triggered-by daily-production-archive-offsite"
   make verify-research-archive-offsite \
     ARGS="--readback-limit 2 --triggered-by daily-production-archive-offsite-verify"
   make audit-research-archive-coverage \
     ARGS="--archive-kind production --triggered-by daily-production-archive-coverage-audit"
   ```

3. Export `silver/vacancy` and `silver/vacancy_current_state` separately as
   point-in-time snapshots after a completed production sweep:

   ```bash
   make export-research-archive \
     ARGS="--dataset silver/vacancy --dataset silver/vacancy_current_state --archive-kind production --triggered-by completed-sweep-snapshot"
   ```

### Automated daily production cadence

After the manual production routine is proven, use
`scripts/ops/run_daily_research_archive.sh`. The host-side driver:

- takes a non-blocking `flock` so two daily pipelines cannot overlap;
- waits on the shared `.state/locks/heavy-ops.lock` so it does not overlap with
  daily backup or weekly restore drill;
- runs bounded incremental exports until a zero-row checkpoint is written;
- fails if the configured maximum number of export batches is exhausted;
- then runs full local verify, idempotent S3 sync, full offsite verify, coverage
  audit and read-only housekeeping preview;
- never invokes `apply-research-archive-housekeeping`.

Run one supervised driver smoke before enabling the timer:

```bash
make daily-research-archive
```

Step logs are written under `.state/logs/research-archive-daily/<UTC run id>/`.
Optional host-only settings can be placed in
`/etc/hhru-platform/research-archive-daily.env`:

```bash
HHRU_RESEARCH_ARCHIVE_DAILY_MAX_EXPORT_BATCHES=20
HHRU_RESEARCH_ARCHIVE_DAILY_LOG_RETENTION_DAYS=30
HHRU_RESEARCH_ARCHIVE_DAILY_LIMIT_PER_DATASET=100000
HHRU_RESEARCH_ARCHIVE_DAILY_CHUNK_SIZE=100000
HHRU_RESEARCH_ARCHIVE_DAILY_BATCH_SIZE=1000
HHRU_RESEARCH_ARCHIVE_DAILY_SETTLED_DELAY_HOURS=24
HHRU_RESEARCH_ARCHIVE_DAILY_READBACK_LIMIT=2
HHRU_HEAVY_OPS_LOCK_WAIT_SECONDS=21600
```

Install the supplied systemd units on the single production VPS:

```bash
install -m 0644 deploy/systemd/hhru-research-archive.service \
  /etc/systemd/system/hhru-research-archive.service
install -m 0644 deploy/systemd/hhru-research-archive.timer \
  /etc/systemd/system/hhru-research-archive.timer
systemctl daemon-reload
systemctl enable --now hhru-research-archive.timer
systemctl list-timers hhru-research-archive.timer
```

The default timer runs daily at `02:30 UTC` with up to `15m` randomized delay.
Inspect status and concise driver events through:

```bash
systemctl status hhru-research-archive.service
journalctl -u hhru-research-archive.service --since today
```

VPS supervised driver smoke on 2026-06-04 succeeded end-to-end against the
canonical `.state/archive/research-production-v2` archive:

- the first export wrote a zero-row production checkpoint;
- local verify, idempotent S3 sync, full offsite verify, coverage audit and
  read-only housekeeping preview all succeeded;
- coverage remained complete with `issue_count=0` across `28/28` production
  checkpoints;
- no destructive housekeeping command was invoked.

This proves the host-side driver. The timer was enabled on the VPS on
2026-06-04. The first unattended run and several subsequent successful daily
runs remain operational gates.

Incremental mode intentionally defaults only to append-only datasets:

- `bronze/raw_api_payload`
- `silver/api_request_log`
- `silver/vacancy_snapshot`
- `silver/vacancy_seen_event`
- `silver/detail_fetch_attempt`

The existing `archive_kind=tool_validation` smoke manifests do not advance
`archive_kind=production` watermarks. No live PostgreSQL rows may be deleted by
this routine yet.

### Interrupted initial export recovery

An interrupted export may leave data files, manifests and inventory entries
without a completed checkpoint. Keep that directory for forensic inspection,
but do not sync it to S3 and do not use it for the canonical retry. Start the
production chain in a fresh local directory and pass that same `--archive-dir`
to local verify, offsite sync, offsite verify, coverage audit and housekeeping.

For a large first catch-up, create bounded checkpoints instead of one monolithic
run:

```bash
PROD_ARCHIVE_DIR=.state/archive/research-production

make export-research-archive \
  ARGS="--archive-dir $PROD_ARCHIVE_DIR --incremental --settled-delay-hours 24 --limit-per-dataset 100000 --chunk-size 100000 --batch-size 10000 --archive-kind production --triggered-by vps-production-archive-catch-up"
```

Repeat the bounded export until the per-dataset cursor transitions stop
advancing, then run the standard local verify, S3 sync, remote verify and
coverage audit sequence against `--archive-dir $PROD_ARCHIVE_DIR`.

Before the first production export, validate incremental watermark advancement
inside an isolated local directory:

```bash
SMOKE_DIR=.state/archive/research-incremental-smoke

make export-research-archive \
  ARGS="--archive-dir $SMOKE_DIR --incremental --settled-delay-hours 24 --limit-per-dataset 10 --chunk-size 10 --archive-kind incremental_validation --triggered-by vps-incremental-archive-smoke-1"

make export-research-archive \
  ARGS="--archive-dir $SMOKE_DIR --incremental --settled-delay-hours 24 --limit-per-dataset 10 --chunk-size 10 --archive-kind incremental_validation --triggered-by vps-incremental-archive-smoke-2"

make verify-research-archive \
  ARGS="--archive-dir $SMOKE_DIR --triggered-by vps-incremental-archive-smoke-verify"

du -sh "$SMOKE_DIR"
```

The second export must report each first run `source_id_after` as its matching
`source_id_before`, then advance to the next bounded source-id prefix. Do not
sync this isolated validation directory to S3.

VPS watermark smoke passed on 2026-06-01 after three bounded exports:

- `bronze/raw_api_payload`: `0 -> 71 -> 81 -> 91`;
- `silver/api_request_log`: `0 -> 71 -> 81 -> 91`;
- `silver/vacancy_snapshot`: `0 -> 1230 -> 1240 -> 1250`;
- `silver/vacancy_seen_event`: `0 -> 1230 -> 1240 -> 1250`;
- local verification: `13/13` manifests, `120` rows.

This isolated pre-checkpoint smoke proves watermark advancement, not S3 coverage.
Use a fresh archive directory when validating the checkpoint audit: older
incremental manifests created before checkpoint support intentionally cannot be
treated as a complete verified chain.

For an end-to-end checkpoint audit smoke, use both a fresh local directory and a
separate S3 prefix. Pass `HHRU_RESEARCH_ARCHIVE_OFFSITE_ROOT` inline for sync and
remote verify; do not overwrite the main research archive inventory with an
isolated test bundle.

Isolated VPS checkpoint audit smoke passed on 2026-06-01 under
`/hhru-platform/research-archive-smoke/checkpoint-20260601T201007Z`:

- before offsite sync and verify, the audit exited non-zero with
  `status=incomplete`, `issue_count=4` and missing checkpoint receipt issues;
- full sync uploaded `9` manifests, inventory and `2` checkpoints;
- remote verify confirmed `9/9` manifests, `2` checkpoints, `21` objects and
  completed bounded readback for `2/2` selected chunks;
- after remote verify, the audit returned `status=complete`, `issue_count=0`;
- receipt counts matched the bundle: `9` chunk verification receipts and `2`
  checkpoint verification receipts.

This proves the non-destructive coverage gate mechanics. It does not yet permit
live DB deletion: housekeeping still needs explicit wiring and a dry-run preview
before any destructive apply.

Historical read-only preview validation used this isolated verified bundle
before `silver/detail_fetch_attempt` became a required checkpoint dataset:

```bash
SMOKE_TAG=checkpoint-20260601T201007Z
SMOKE_DIR=".state/archive/research-coverage-smoke-$SMOKE_TAG"
SMOKE_ROOT="/hhru-platform/research-archive-smoke/$SMOKE_TAG"

HHRU_RESEARCH_ARCHIVE_OFFSITE_ROOT="$SMOKE_ROOT" \
  make preview-research-archive-housekeeping \
  ARGS="--archive-dir $SMOKE_DIR --archive-kind incremental_validation --raw-api-payload-retention-days 1 --vacancy-snapshot-retention-days 1 --finished-crawl-run-retention-days 1 --delete-limit-per-target 20 --triggered-by vps-coverage-housekeeping-preview"
```

The command must report `status=ready`, `coverage_status=complete`, candidate
ranges no higher than their matching `source_id_covered`, and a separate
`run_tree_summary`. Runs owning seen events above `seen_event_source_id_covered`
must be counted as coverage-blocked and excluded from the run-tree action list.
The command must not delete or archive rows. The existing
`run-housekeeping --execute` path remains separate until every destructive
target is covered.

Initial VPS preview on 2026-06-01 proved these safety semantics:

- `status=ready`, `coverage_status=complete`, `coverage_issue_count=0`;
- raw payload cap `81`, `candidate_count=20`, selected range `1..81`;
- vacancy snapshot cap `1240`, `candidate_count=0`;
- no rows were deleted or archived.

The initial preview took `446861 ms`, which was too slow for routine operation.
Migration `0005_snapshot_payload_ref_idx` adds missing payload-reference indexes,
bounded raw protection subqueries now use the verified cursor, and latest
snapshot protection uses an indexed `NOT EXISTS newer snapshot` check instead of
a global window rank. The repeat VPS preview after migration took `159 ms`
(`2.897s` wall time including Docker startup) and returned `20` raw plus `20`
snapshot candidates.

The follow-up run-tree preview on 2026-06-01 returned
`seen_event_source_id_covered=1240`, `candidate_count=1`,
`coverage_blocked_candidate_count=1`, `coverage_safe_candidate_count=0` and
`action_count=0`. This proves fail-closed run exclusion when a run still owns an
unverified seen event. Preview duration was `131 ms`.

After adding `silver/detail_fetch_attempt`, use a fresh isolated local directory
and S3 prefix for the next smoke. Existing checkpoints intentionally become
`incomplete` under the expanded append-only dataset set because they do not
contain a detail-attempt cursor.

The fresh smoke preview must include
`--detail-fetch-attempt-retention-days 1` and report a
`target_summary target=detail_fetch_attempt` line bounded by the verified
`silver/detail_fetch_attempt` cursor.

Fresh isolated five-dataset VPS smoke passed on 2026-06-01 under
`/hhru-platform/research-archive-smoke/detail-attempt-20260601T214443Z`:

- two bounded exports advanced raw/request-log cursors `0 -> 71 -> 81`,
  snapshot/seen-event cursors `0 -> 1230 -> 1240`, and detail-attempt cursor
  `0 -> 10 -> 20`;
- local verification confirmed `11/11` manifests with `100` rows;
- remote verification confirmed `11/11` manifests, `2` checkpoints, `25`
  remote objects and `2/2` bounded readbacks;
- coverage audit returned `status=complete`, `issue_count=0` for all five
  append-only datasets;
- read-only preview returned `status=ready`, complete coverage, `20` raw and
  `20` vacancy-snapshot actions, `0` detail-attempt actions, and excluded the
  one old run-tree candidate because it remained coverage-blocked.

This isolated proof still does not permit live deletion. Before the first apply,
build and verify a separate `archive_kind=production` chain under the canonical
production prefix, take and verify a pre-delete DB backup, run the restore drill,
review a production preview, then invoke the guarded command with explicit
`--apply`.

VPS guard-smoke passed on 2026-06-02 after deploying the guarded apply entrypoint:
running `apply-research-archive-housekeeping` without `--apply` exited non-zero
with `--apply confirmation is required for destructive housekeeping`. No
coverage audit or deletion was started.

Canonical production bootstrap passed on 2026-06-04:

- checkpoint-only crash recovery and the `32 MiB` writer ceiling replaced the
  initial OOM-killed monolithic attempt;
- `27` production checkpoints cover all five append-only datasets;
- local verification passed for `1557/1557` manifests and `6,885,371` rows;
- canonical S3 sync uploaded `1557` manifests, `27` checkpoints and inventory;
- offsite verification passed for `1557` manifests, `27` checkpoints, `3142`
  objects and `2` readbacks;
- coverage audit returned `status=complete`, `issue_count=0`;
- default-path production preview returned `status=ready` with `0` candidates
  and `0` actions.

### Stage F: analytical layer, later

- Add optional Parquet export once schemas are stable and dependency choice is
  accepted.
- Candidate dependency: `pyarrow` for Parquet writing; optional local analysis
  tool: DuckDB.
- Parquet is generated from silver/bronze and can be rebuilt.
- Text extraction, AI exposure scoring, panels and econometrics are intentionally
  later research layers, not archive foundation requirements.

## 12. Open Decisions

- Exact long-term compressed chunk size target is still open. Export buffering is
  already capped at `32 MiB` serialized bytes independently of the requested row
  limit so large payloads cannot exhaust process memory.
- Whether the initial daily append-only cadence should later be supplemented by
  per-completed-run exports after production telemetry is available.
- Whether current pilot corpus should be archived as evidence or discarded after
  verified backup/offsite.
- Whether to implement content-addressed raw dedup in v1. Default: no.
