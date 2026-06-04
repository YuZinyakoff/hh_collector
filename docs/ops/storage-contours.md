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
- Fail-closed daily backup and weekly offsite restore drill drivers plus
  systemd timers are implemented. They share a heavy-ops lock with the research
  archive driver and require supervised VPS smoke before timer enable.
- Daily backup local retention defaults to `2` days for the current VPS because
  one dump is approximately `13 GB`. S3 cleanup remains manual/dry-run-first
  until a safe real deletion candidate proves apply semantics.

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
- Successful remote verification writes one local
  `<chunk>.manifest.json.offsite.verified.json` receipt per verified chunk. This
  is proof for the chunk-level S3 check, not yet permission to delete live DB
  rows.
- Non-destructive incremental export exists for append-only archive datasets.
  It derives a per-dataset watermark only from completed local checkpoints with
  the same `archive_kind` and exports only the contiguous source-id prefix older
  than the settled cutoff. Orphan chunk manifests from an interrupted run do not
  advance the cursor. Point-in-time dimensions remain explicit snapshot exports.
- Local chunk buffering is capped by the requested row count and a `32 MiB`
  serialized-byte ceiling so large raw payload exports remain memory-bounded.
- Every incremental export now records per-dataset cursor transitions in a
  checkpoint. `audit-research-archive-coverage` verifies the chain from cursor
  `0` and requires matching chunk-level and checkpoint-level S3 verification
  receipts. The audit is a fail-closed report only; it is not yet wired to
  destructive housekeeping.
- Full S3 sync publishes checkpoints with inventory and full offsite verification
  checks their remote sizes and records matching local receipts. Partial syncs
  intentionally do not publish these completeness artifacts.
- Isolated VPS incremental smoke passed on 2026-06-01 across three bounded
  exports: raw/request-log cursors advanced `0 -> 71 -> 81 -> 91`,
  snapshot/seen-event cursors advanced `0 -> 1230 -> 1240 -> 1250`, and local
  verification passed for `13/13` manifests with `120` rows.
- Isolated VPS checkpoint coverage smoke passed on 2026-06-01 under a separate
  S3 prefix. Before offsite sync/verify the audit returned `status=incomplete`;
  after uploading `9` manifests and `2` checkpoints and verifying `21` remote
  objects it returned `status=complete`, `issue_count=0`. This proves the
  fail-closed non-destructive gate, not live DB deletion wiring.
- `preview-research-archive-housekeeping` is the first read-only bridge from
  verified coverage to retention planning. It reports age-based raw payload and
  vacancy snapshot candidates only inside verified source-id cursors. It also
  reports old finished runs separately and excludes runs owning
  `vacancy_seen_event` rows above the verified seen-event cursor from the action
  list. It does not authorize deletion.
- Initial isolated VPS preview returned `status=ready` with raw cap `81`,
  snapshot cap `1240`, raw candidates `20` and snapshot candidates `0`, but took
  `446861 ms`. After SQL/index optimization and migration
  `0005_snapshot_payload_ref_idx`, repeated VPS timing was `159 ms` (`2.897s`
  wall time including Docker startup) with `20` raw and `20` snapshot
  candidates.
- Isolated VPS run-tree preview returned one old run candidate but excluded it
  fail-closed because it owned a `vacancy_seen_event` row above verified cursor
  `1240`: `coverage_blocked_candidate_count=1`, `action_count=0`. Preview
  duration was `131 ms`.
- `silver/detail_fetch_attempt` is added as an append-only checkpoint dataset
  and bounded preview target. Fresh isolated five-dataset S3 smoke passed with
  `11/11` manifests, `2` checkpoints, `25` remote objects and complete coverage.
- `apply-research-archive-housekeeping --apply` is a separate production-only
  destructive path. It requires the canonical `/hhru-platform/research-archive`
  offsite root, reruns verified coverage planning in the transaction, replans
  exact bounded ids, locks selected run-tree roots and aborts on any delete-count
  mismatch. Isolated validation bundles cannot authorize apply.
- VPS guard-smoke passed on 2026-06-02: invoking the destructive entrypoint
  without `--apply` exited fail-closed before coverage audit or deletion.

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

- initially run a daily append-only export for time partitions older than a `24h`
  safety window;
- export point-in-time `vacancy` and `vacancy_current_state` snapshots separately
  after a completed production sweep;
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
4. Use daily `--incremental --settled-delay-hours 24 --archive-kind production`
   exports for append-only datasets. Bootstrap a large backlog through repeated
   `--limit-per-dataset` checkpoint batches in a fresh local archive directory
   if an earlier attempt left orphan chunks.
5. Require complete verified archive coverage before any archive-before-delete
   housekeeping.
6. The manual production routine is proven. Use the non-overlapping host-side
   `daily-research-archive` driver and supplied systemd timer; the supervised
   driver smoke passed end-to-end on 2026-06-04. It automates only export,
   verification, offsite sync, coverage audit and read-only preview.
   Destructive apply remains manual. The timer was enabled on 2026-06-04; the
   first unattended run and several subsequent successful runs remain
   operational gates.
7. Add Parquet export only after the v1 dataset schemas are named and stable.
