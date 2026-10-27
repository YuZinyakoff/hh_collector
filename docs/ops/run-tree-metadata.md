# Run-tree metadata preservation

This is a separate control-plane archive, not an expansion of the append-only
Archive v1 watermark chain. Finished-run retention remains disabled in production.
No new timers, automatic deletion or production restore command are introduced.

## What is preserved

One immutable local bundle contains full rows of one explicitly selected
`crawl_run` and every associated `crawl_partition`, including original ids,
timestamps, config, notes, request parameters, split-parent links, errors and
counters. Extraction is a PostgreSQL REPEATABLE READ / READ ONLY snapshot,
streamed in batches of 1000 partitions. Unfinished/missing runs are rejected.
Files: `metadata.jsonl.gz`, `manifest.json`, then `offsite.verified.json`.
The manifest binds compressed and logical SHA-256 digests and record count.
Directories/files are created with private permissions; treat them as sensitive
backups and do not paste their records into chat or documentation.

Observations remain in Archive v1 `silver/vacancy_seen_event`, not this bundle.
Raw/snapshot data, nullified links in other live tables and their restoration
are also outside this metadata-only contract. Restoring the tree does not replay
reconciliation, restore live current state, or recreate a whole historical DB.

## Non-destructive production validation after code deployment

First obtain/review an exact finished run id (not the currently active sweep).
The user validated production export and S3 full readback on October 22 for
`c7e7d8c6-6813-454c-845e-ca44539da1e8`: 18304 partitions, 2.3M, local bundle
`.state/archive/run-tree-metadata/c7e7d8c6-6813-454c-845e-ca44539da1e8-9ud_8za0`.
The actual production-bundle offline restore drill remains pending. The commands
below are the procedure, not a request to repeat this completed export.

```bash
cd /opt/hh_collector
docker compose --profile ops run --rm --no-deps app \
  export-run-tree-metadata --run-id REVIEWED_FINISHED_RUN_UUID
```

Copy the reported `bundle_dir` into the variable below, retaining its basename:

```bash
bundle_dir='.state/archive/run-tree-metadata/EXACT_REPORTED_BUNDLE_DIRECTORY'
docker compose --profile ops run --rm --no-deps app \
  verify-run-tree-metadata --bundle-dir "$bundle_dir"
docker compose --profile ops run --rm --no-deps app \
  sync-run-tree-metadata --bundle-dir "$bundle_dir"
```

The sync command performs explicit S3 writes, then downloads BOTH compressed
data and manifest in full, verifies SHA-256 and validates the downloaded tree.
Only successful full readback writes an atomic verification receipt. Remote
keys are beneath `/hhru-platform/research-archive/run-tree-metadata/<run-id>/<bundle-name>/`.
Existing research archive S3 settings/credential fallback are reused; no new
secret is needed. Upload only after approval. No remote pruning/lifecycle of
these metadata objects is configured by this feature: ensure the bucket policy
does not expire them. A local receipt records a past successful verification,
not a perpetual guarantee of remote availability. Repeat sync/readback before
an eventual supervised cleanup and retain both manifest and receipt locally.

## Restore check

`SqlAlchemyRunTreeMetadataRepository.restore_into_empty_tables` is an internal
reconstruction routine for an isolated target only. It rejects nonempty
run/partition tables, requires exact column compatibility, inserts original ids,
and restores parent links after all rows exist (UUID order is not tree order).
Caller owns one transaction; failures must roll it back. Never call it against
production. There is deliberately no production restoration CLI.

The real-PostgreSQL integration test exports a tree, creates a separate temporary
schema with empty run/partition tables, restores the bundle and compares the
complete logical digest. It also rejects a second restore into nonempty tables.
All schemas/data roll back. Run against a disposable PostgreSQL 16 test DB:

```bash
python -m pytest tests/integration/test_run_tree_retention_safety.py -q
```

Before deleting a production root, additionally perform an offline drill using
the ACTUAL downloaded production bundle in a disposable database. Synthetic tests
do not establish compatibility or recoverability of every production artifact.
The bundle verifier checks ids, checksums, record counts, ownership and an acyclic
complete parent graph; it does not insert into PostgreSQL.

## Destructive path remains separate

Run-tree apply now requires `--run-tree-metadata-dir`: only that bundle's exact
run id can be selected. It rechecks the matching destination/manifest receipt,
age and seen-event coverage while locking the root under idle admission, then
compares live metadata with the exported logical digest before ANY deletion.
Changed config/notes/partitions require a fresh export and readback. Missing,
stale or mismatched proof fails closed; cascade budgets and timeouts still apply.
One bundle authorizes at most one run even if the root limit is larger.
Raw/snapshot targets remain independent; disable them for any isolated future
run-tree experiment. No apply command or budget override is authorized here.
Daily retention must remain `HHRU_RESEARCH_ARCHIVE_DAILY_FINISHED_CRAWL_RUN_RETENTION_DAYS=0`.

This feature preserves technical metadata; it does not solve OS disk reclamation.
Do not couple its deployment to enabling retention or running VACUUM FULL.

Local validation: 283 passed / 25 skipped without services; 7 targeted tests
passed on isolated PostgreSQL 16.2, including exact metadata reconstruction.
The bundled server uses ORM-created tables (pgcrypto is unavailable for a full
Alembic run). Ruff and mypy passed. No production S3 upload or restore was run.
