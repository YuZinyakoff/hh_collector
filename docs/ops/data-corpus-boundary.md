# Data Corpus Boundary

This document fixes the boundary between operational pilot evidence and future
production analytical data.

## Current Decision

The existing VPS database and canonical research archive are treated as
`pilot/test corpus` evidence. They proved collection, archive, backup, offsite
restore, housekeeping-preview and unattended storage mechanics. They are not the
canonical analytical production corpus.

Measured state on `2026-06-23`:

- live DB has `865868` vacancies;
- `848056` vacancies have successful detail snapshots;
- `17812` vacancies ended in terminal detail `404`;
- research archive has `6885371` verified rows and `1557/1557` verified
  manifests;
- with boundary `2026-06-01T00:00:00+00:00`, post-boundary production counts are
  all `0`: no new vacancies, no search payloads, no detail attempts and no
  snapshots.

This is timestamp-boundary accounting, not a statement that storage is empty.
The existing pilot/test corpus remains available in PostgreSQL, local archive
and S3 archive, and it is the dataset used by archive readability/notebook
smokes.

The detailed dated snapshot is fixed in
[current-state-2026-06-23.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-state-2026-06-23.md).

Do not delete this corpus now:

- it is the current evidence base for operational hardening;
- backup/archive/restore checks are already proven against it;
- disk pressure is acceptable;
- destructive cleanup is intentionally still dry-run-first.

## How To Separate It Later

Before sustained production collection or serious analysis, create an explicit
production boundary:

1. Add a `corpus_id` / `collection_epoch` concept, at least on `crawl_run` and
   archive metadata.
2. Start a new production crawl with a name such as `production-202606`.
3. Make analysis jobs filter by that corpus boundary by default.
4. Keep the current corpus available as `pilot-202605-202606` for operational
   regression checks and throughput/storage comparisons.

If a fully clean analytical dataset is required, take a final backup/archive of
the pilot corpus, then start from a fresh database or explicitly truncate only
after a reviewed destructive plan. That is a later production-launch decision,
not part of the current storage timer rollout.

## Analysis Rule

Any near-term notebooks or DataFrame smoke tests may use the current archive only
to prove readability and tooling. Their outputs must be labelled as pilot/test
evidence and must not be presented as production labour-market analysis.

As of `2026-06-23`, "without May test collection" means the post-boundary
production epoch has no rows yet unless a query intentionally opts into the
pilot/test corpus.
