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
    year=2026/month=05/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json

  inventory/
    archive-inventory.jsonl
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
- `request_log_id`
- `raw_payload_id`
- `payload_hash`

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

Optional if cheap in the same slice:

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

Status on 2026-05-27:

- `export-research-archive` exists for the Stage A foundation datasets.
- `verify-research-archive` exists for local manifest, gzip, checksum, row count
  and inventory validation.
- S3 upload/readback is not implemented yet.
- Research analytics, Parquet and gold datasets remain intentionally out of scope.

### Stage C: S3 upload and verification

- Reuse S3 client patterns from backup offsite tooling.
- Upload data files, manifests and inventory.
- Verify remote existence and sizes.
- Add readback command or readback mode.
- Keep uploads idempotent through local receipts.

### Stage D: proof-of-read smoke

This is not analytics product work. It only proves the archive is usable as a
dataset.

- Read one `silver` chunk.
- Print row count, min/max observed date and one or two distinct-count sanity
  metrics, e.g. distinct areas or vacancies.
- No panels, no text models, no gold layer.

### Stage E: production archive safety

- Add "archive verified" receipts.
- Make housekeeping require verified archive receipts before deleting raw/snapshot
  rows from production data.
- Add operator runbook for archive-before-delete.

### Stage F: analytical layer, later

- Add optional Parquet export once schemas are stable and dependency choice is
  accepted.
- Candidate dependency: `pyarrow` for Parquet writing; optional local analysis
  tool: DuckDB.
- Parquet is generated from silver/bronze and can be rebuilt.
- Text extraction, AI exposure scoring, panels and econometrics are intentionally
  later research layers, not archive foundation requirements.

## 12. Open Decisions

- Exact chunk size target: start with `100k` rows or `64-256MB` compressed files,
  whichever is easier to implement safely.
- Whether production archive cadence is per completed run, daily partition, or
  both.
- Whether current pilot corpus should be archived as evidence or discarded after
  verified backup/offsite.
- Whether to implement content-addressed raw dedup in v1. Default: no.
