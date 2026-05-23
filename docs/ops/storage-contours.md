# Storage Contours

This document separates the storage problems that must not be mixed:

1. provider-level VM recovery;
2. PostgreSQL backup/restore;
3. long-term research archive.

The project can use the same S3 account for multiple contours, but each contour
needs its own object layout, verification rules, retention policy, and operator
commands.

## 1. Contour A: provider snapshot

Purpose: recover the whole VPS after host, disk, or operator failure.

Owned by: VPS provider.

Contains:

- Docker volumes;
- local `.state`;
- application checkout;
- local config.

This is useful as a coarse disaster-recovery layer, but it is not enough for
research data preservation:

- restore is all-or-nothing;
- provider snapshots are tied to the provider;
- they are not a stable analytical data format;
- they do not prove that PostgreSQL logical restore works.

## 2. Contour B: PostgreSQL backup offsite

Purpose: restore the operational live database.

Canonical local artifact:

- `.state/backups/*.dump`
- `.state/backups/*.dump.manifest.json`

Canonical remote artifact:

```text
s3://<bucket>/hhru-platform/backups/
  <dump>.parts/
    000001.part
    000002.part
    ...
  <dump>.manifest.json
```

Current status:

- S3 upload works with `HHRU_BACKUP_OFFSITE_BACKEND=s3`.
- A 2.2 GiB dump uploaded to Timeweb cold S3 in about 82 seconds on 2026-05-23.
- Idempotency is proven: a repeated run skipped the same dump through `.offsite.json`.
- Remote size verification exists through `verify-backup-offsite`.
- Offsite restore drill tooling exists through `run-backup-offsite-restore-drill` /
  `make backup-offsite-restore-drill`.
- One real VPS S3 drill succeeded on 2026-05-23: remote parts were downloaded,
  assembled dump passed `backup_sha256`, and core tables were restored into
  `hhru_platform_restore_drill`.

The DB backup contour is considered adequate only after these checks are in place:

- local `verify-backup-file` passes;
- local `run-restore-drill` passes into a separate database;
- offsite upload succeeds;
- repeated offsite upload is idempotent;
- remote object existence and sizes are verified against the manifest;
- at least one offsite restore drill downloads the remote parts, assembles the dump,
  verifies `backup_sha256`, and restores into a separate database;
- backup retention is explicit for both local and S3 copies.

Do not use Parquet for DB backups. PostgreSQL backup is an operational restore
artifact, so the right format is PostgreSQL custom dump plus manifest and checksums.

## 3. Contour C: research archive

Purpose: preserve the long-term dataset for future analysis independently from
the live operational database.

This is the data product of the project. It must be:

- append-only or immutable by convention;
- self-describing through manifests;
- partitioned by dataset and observation time;
- readable without restoring the live PostgreSQL database;
- independent from a specific VPS or Postgres version.

Candidate S3 layout:

```text
s3://<bucket>/hhru-platform/research-archive/v1/
  raw_api_payload/
    year=2026/month=05/day=23/
      chunk-000001.jsonl.gz
      chunk-000001.manifest.json
  vacancy_snapshot/
    year=2026/month=05/day=23/
      chunk-000001.parquet
      chunk-000001.manifest.json
  vacancy_current_state/
    snapshot_date=2026-05-23/
      chunk-000001.parquet
      chunk-000001.manifest.json
  detail_fetch_attempt/
    year=2026/month=05/
      chunk-000001.parquet
      chunk-000001.manifest.json
  inventory/
    archive-inventory.jsonl
```

The archive should keep raw payloads as first-class data. Normalized analytical
tables can be regenerated if raw payloads and transformation code are preserved.

## 4. Parquet policy

Parquet is useful for analytical datasets, not for opaque recovery artifacts.

Use Parquet for:

- `vacancy_snapshot`;
- `vacancy_current_state` periodic snapshots;
- `vacancy_seen_event`;
- `detail_fetch_attempt`;
- derived analytical tables with stable schemas.

Keep JSONL.GZ for:

- raw API payloads as the canonical raw archive;
- early archive v1 while schemas are still moving;
- small control files and inventories.

Reasoning:

- JSONL.GZ is simpler, robust, and preserves arbitrary HH JSON without schema loss.
- Parquet is much better for column scans, compression, and future research queries.
- Raw JSON can be mirrored into Parquet later, but the raw archive should not depend
  on a fragile first Parquet schema.

The pragmatic target is dual-format:

- canonical raw layer: `jsonl.gz`;
- analytical layer: `parquet`, generated from canonical raw and normalized DB rows.

## 5. Deletion safety rule

Live DB rows may be deleted by housekeeping only after the archive contour proves:

- local export completed;
- offsite upload completed;
- remote manifest exists;
- remote sizes and checksums match local manifest or a full readback drill passes;
- archive inventory was updated;
- operator can locate the archive bundle by date, target, and source row range.

Until this exists, S3 should be treated as backup/offsite storage, not as the
trusted research archive.

## 6. Immediate next steps

For DB backup contour:

1. Run `verify-backup-offsite` after every `backup-offsite`.
2. Run `backup-offsite-restore-drill` periodically, especially after changing backup
   format, S3 settings, Postgres version, or retention policy.
3. Define local backup retention after successful S3 upload and restore drill.

For research archive contour:

1. Write `research-archive-v1` format contract.
2. Keep current JSONL.GZ retention export as the canonical raw archive MVP.
3. Add S3 backend to retention archive offsite sync.
4. Add archive inventory.
5. Add Parquet export only after the v1 dataset schemas are named and stable.
