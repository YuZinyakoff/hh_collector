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
- Post-detail-drain milestone backup on 2026-05-28 succeeded locally and offsite:
  dump `13232097458` bytes, sha256
  `46d485f21765df90dec9edbdef1362f5bacfd4848008d48e5941c2c5c456de86`,
  `198` S3 data parts and `verified_object_count=199`.
- Local dump retention exists through `HHRU_BACKUP_RETENTION_DAYS` and runs when
  a new backup is created.
- S3/offsite retention cleanup tooling exists through
  `cleanup-backup-offsite`: matching upload/verification receipts are required,
  dry-run is the default, apply is explicit, and protected/latest/weekly
  generations are retained.
- VPS dry-run passed on 2026-05-31 against real S3 state: `2` upload receipts
  scanned, post-detail-drain milestone retained through `.offsite.keep`, older
  dump skipped fail-safe as unverified, `delete_candidate_count=0`. Destructive
  apply smoke is intentionally deferred until a real safe candidate exists.

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

Format contract: [research-archive-v1.md](/home/yurizinyakov/projects/hh_collector/docs/ops/research-archive-v1.md).

This is the data product of the project. It must be:

- append-only or immutable by convention;
- self-describing through manifests;
- partitioned by dataset and observation time;
- readable without restoring the live PostgreSQL database;
- independent from a specific VPS or Postgres version.

Candidate S3 layout:

```text
s3://<bucket>/hhru-platform/research-archive/v1/
  bronze/raw_api_payload/
    request_type=vacancy_detail/year=2026/month=05/day=23/
      20260523T000000Z-chunk-000001.jsonl.gz
      20260523T000000Z-chunk-000001.manifest.json
  silver/vacancy_snapshot/
    snapshot_type=detail/year=2026/month=05/day=23/
      20260523T000000Z-chunk-000001.jsonl.gz
      20260523T000000Z-chunk-000001.manifest.json
  silver/vacancy_current_state/
    snapshot_date=2026-05-23/
      20260523T000000Z-chunk-000001.jsonl.gz
      20260523T000000Z-chunk-000001.manifest.json
  inventory/
    archive-inventory.jsonl
```

The archive should keep raw payloads as first-class data. Normalized analytical
tables can be regenerated if raw payloads and transformation code are preserved.

Current status:

- Local Archive v1 export/verify smoke passed on VPS on 2026-05-28 with
  `archive_kind=tool_validation`.
- Smoke bundle: `6000` rows, `13` chunks, `5212503` data bytes, `13/13`
  manifests verified.
- S3 upload, remote verification and bounded readback smoke passed on VPS on
  2026-05-31 for the same `tool_validation` bundle: `13/13` manifests verified,
  `27` remote objects verified, `2/2` selected chunks downloaded and checked for
  size, sha256, gzip JSONL parse and row count.
- Repeated full S3 sync was idempotent: `candidate_manifest_count=0`,
  `uploaded_manifest_count=0`, `skipped_manifest_count=13`. Full sync refreshes
  the inventory by design.
- Inventory is uploaded only on full sync. Partial syncs with `--limit` or
  explicit manifests upload data/manifest objects only, to avoid a remote
  inventory that references chunks not yet present in S3.

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

## 6. S3 retention policy decision

Decision on 2026-05-26: S3 must be automated, but backup retention and research
archive retention are intentionally different policies.

### DB backup offsite policy

DB backups are operational recovery artifacts for live PostgreSQL. They are not
the long-term research dataset.

Use S3 backups for:

- disaster recovery after VPS/DB loss;
- rollback point before schema migrations;
- rollback point before destructive cleanup;
- milestone evidence after major production sweeps.

Recommended automation:

- upload and verify a DB backup after important events: successful search sweep,
  large detail drain, schema migration, before destructive cleanup;
- during active production periods, run periodic backup/offsite, e.g. daily;
- during quiet steady mode, weekly may be enough;
- keep bounded generations, not all dumps forever:
  - last `3` verified backups;
  - last `4` weekly backups;
  - explicitly marked milestone backups;
- do not delete an old remote backup unless newer backup upload and remote verify
  succeeded, and periodic offsite restore drill remains green.

Implemented tooling item: `cleanup-backup-offsite` deletes S3 backup generations
under the `backups/` prefix only after a dry-run plan and explicit `--apply`.
Deletion order is remote `*.parts/`, remote manifest, then local operational
sidecars. The local `.dump` remains under the separate local retention policy.
VPS dry-run is proven. Destructive apply smoke remains open until a real safe
candidate exists; do not manufacture deletion candidates in the production
bucket merely to exercise the command.

### Research archive policy

Research archive is the cold, long-term data product. It should not use DB dump
retention semantics.

Use S3 research archive for:

- settled raw API payloads as canonical `jsonl.gz`;
- manifests, checksums and source row ranges;
- inventory of archive bundles;
- later analytical Parquet datasets generated from canonical raw/normalized data.

Recommended automation:

- write only completed/settled bundles: completed search run, completed detail
  catch-up, or time partitions older than a safety window;
- verify remote manifest, sizes and checksums/readback before considering live DB
  cleanup;
- treat bundles as immutable by convention;
- do not age-delete normal research archive data. Delete only explicitly bad,
  superseded or test bundles after operator review.

Pilot/test corpus policy:

- current VPS corpus is evidence for throughput/storage/restore behavior;
- it may be preserved as one milestone backup/archive evidence bundle;
- it must not be mixed into canonical production archive without explicit labeling.

## 7. Immediate next steps

For DB backup contour:

1. Run `verify-backup-offsite` after every `backup-offsite`.
2. Run `backup-offsite-restore-drill` periodically, especially after changing backup
   format, S3 settings, Postgres version, or retention policy.
3. Keep local backup retention enabled through `HHRU_BACKUP_RETENTION_DAYS`.
4. Run `cleanup-backup-offsite` as dry-run, review retained/candidate generations,
   then apply explicitly when the policy is correct.

For research archive contour:

1. Local Archive v1 smoke passed on 2026-05-28; repeat it after archive schema or
   serialization changes.
2. Keep JSONL.GZ as the canonical raw Archive v1 format.
3. Keep append-only archive inventory and repeat S3 verification/readback after
   archive schema or serialization changes.
4. Define production cadence for settled archive bundles and verified
   archive-before-delete receipts.
5. Add Parquet export only after the v1 dataset schemas are named and stable.
