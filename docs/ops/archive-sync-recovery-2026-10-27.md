# Archive sync recovery (2026-10-27)

## Evidence

Production checkout was `f712702`. The October 22 weekly sweep succeeded on
October 23; the shown daily DB backups through October 27 succeeded. Archive
hit the 300-export-batch limit on October 24, then succeeded on October 25-26.
October 27 exported zero new rows and passed local verification, but S3 sync
failed with EndpointConnectionError after about 95 minutes, before housekeeping.
This is not evidence of archive corruption or failed credentials.

The user approved one repeat of the normal pipeline, started at 19:02:40 UTC
(`20261027T190240Z-282005`). At 20:22 it was still in offsite-sync. HTTP probes
worked from host/container; an authenticated HEAD for the failing checkpoint
returned 1686 bytes in 0.11 seconds. Socket inspection showed old CLOSE-WAIT
connections AND an ESTAB socket with recent traffic: a total network stall or
specific SDK deadlock was not established. No kill/restart was performed.
At 21:08:30 UTC the same unit/container remained active. The user subsequently
approved a controlled stop of this retry, commit/push and deployment of the fix.
Execution of that stop/deployment has not yet been confirmed; do not assume the
container stopped merely because the systemd wrapper was stopped.

There are 4966 checkpoint files. Previous full sync unconditionally uploaded
every checkpoint and inventory, even on a no-new-data run, without progress
events for these transfers. Disk after daily prune was 122G used / 33G free
(80%), up from 117G / 38G on October 22. Finished-run retention remains 0.

## Local fix

- Full sync skips unchanged inventory/checkpoints only when content SHA-256,
  size, endpoint/bucket, root, remote object path and receipt version/kind match.
  Receipts are private, atomically replaced `*.upload-cache.json` sidecars,
  written ONLY after a successful upload and a second local content check.
- Already matching checkpoint verification receipts also permit skipping a
  redundant PUT. This avoids a 4966-object reupload just to initialize the cache.
  Changed/missing/malformed proofs or a different destination require upload.
- A partially failed sync preserves receipts for completed objects; retry skips
  those successes, retries the failed object and continues. Partial/limited
  sync still does NOT upload inventory or checkpoints.
- Progress events report phase (`manifests`, `inventory`, `checkpoints`),
  completed/total, uploaded/skipped at phase boundaries and every 100 files or
  after 10 seconds between files. Each control PUT logs its file/size before
  upload and duration after success. This is not an independent heartbeat
  while inside a blocking SDK call.
- CLI adds `checkpoint_scanned_count` and `checkpoint_skipped_count`;
  `checkpoint_uploaded_count` and checkpoint summaries count actual PUTs, not
  all discovered checkpoints. `inventory_uploaded=no` now also means a matching
  unchanged inventory was skipped, not only missing/partial inventory.
- Research archive/metadata clients use 10s connect / 60s read socket timeouts,
  standard SDK retries with 3 TOTAL attempts per API request, and serial managed
  transfers (`use_threads=False`, concurrency 1). Streamed download retries are
  separately limited to 3 by TransferConfig. DB backup clients retain their
  previous SDK/transfer profile. No new environment variables or dependencies.

These are per-I/O/per-request limits, NOT a hard wall-clock deadline for a
multipart transfer or the entire pipeline. DNS, multipart counts, retry backoff
and the number of objects affect total time. The underlying provider/transport
failure has not been reproduced or conclusively diagnosed locally.

## Safety and deployment

Upload cache is NOT offsite verification and never authorizes database deletion,
local chunk prune or coverage advancement. Full remote verification (including
existing HEAD checks/readback samples) and checkpoint-chain audit remain
mandatory. If a remote object disappears after caching, verification must fail;
reconcile/reupload the affected object explicitly rather than bypassing checks.
Keep verification receipts/checkpoints; upload cache is recreatable but useful
to avoid unnecessary PUTs. Do not delete either kind indiscriminately.

No migration, new systemd unit, retention change or production restart is part
of this fix. Deploy only after approval. Prefer waiting for the existing retry
to finish before changing code used by later pipeline subprocesses; the current
Python sync process does not acquire the fix through git pull. Do not start a
second concurrent sync. Confirm current unit state and failed-step logs before
deciding whether a new supervised run is necessary. Keep finished-run retention 0.
Progress events are redirected to the run directory's `offsite-sync.log` by the
daily wrapper; journalctl shows phase transitions, not live inner-step progress.

Validation: Python 3.12, SQLAlchemy 2.0.48; 301 passed, 25 skipped without
services. Ruff and mypy passed. Tests cover unchanged/changed files, destination
binding, verification-receipt reuse/mismatch, malformed cache, atomic-write
failure, partial failure/retry, source mutation during upload, partial sync,
progress counts and S3 timeout/retry/serial configuration. A coverage test
explicitly confirms upload-cache receipts cannot replace verification receipts.
No live S3 upload or production action was performed by these local tests.
