# Storage State Snapshot

Reusable read-only snapshot command for VPS storage, backup, archive and corpus
state.

Run from `/opt/hh_collector`:

```bash
make storage-state-snapshot \
  ARGS="--boundary-utc 2026-06-01T00:00:00+00:00" \
  | tee /tmp/hhru-storage-state-$(date -u +%Y%m%dT%H%M%SZ).log
```

The default boundary is `2026-06-01T00:00:00+00:00`, used as a practical
cutoff for "post-May-test" corpus counts until the project has a first-class
`corpus_id` / `collection_epoch`.

The script is intentionally read-only:

- reads systemd timer/service state;
- reads compose `scheduler` / `detail-worker` runtime state;
- reads Docker container and volume sizes;
- reads local backup manifests and offsite receipts;
- reads latest archive and backup cleanup logs;
- queries PostgreSQL via `docker compose exec -T postgres psql`;
- prints timestamp ranges for core corpus tables so a `0` post-boundary count is
  distinguishable from an empty database;
- prints latest `crawl_run` rows so collection inactivity is visible directly;
- does not run backup, archive, cleanup or collection jobs.

Use this snapshot before updating project readiness docs or making retention /
production-corpus decisions.

Latest recorded state:
[current-state-2026-06-23.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-state-2026-06-23.md).
