# Search Quality Audit, 2026-11-07

## Question and Known Evidence

Do search-filter violations affect only one scope, many scopes, or also the
stored observations themselves? Do not equate API-card authenticity, search
coverage, observation integrity, and reconciliation correctness.

The user confirmed that area `144` is still listed as Autopoligon in the HH
dictionary. Separate no-redirect GET probes from the VPS returned global
results for `area=144`, while `area=1` returned Moscow and descendant
Zelenograd. A Moscow day-window probe returned three in-window items.
This is evidence against a failure of every filter, not a corpus-wide audit.

Saved responses for area 144 in runs starting October 22 and 29 already
contain unrelated regions and out-of-window dates. The supplied October 22
and 29 examples are split nodes, not evidence that all covered leaves are
wrong. The November 5 failed run has 443 pending and one unresolved leaf.
At least one neighbouring covered leaf in that run contained out-of-window
items. Neither the start date nor the full extent of the behaviour is known.

See `search-window-mismatch-2026-11-07.md`. The local strict-window collection
guard is experimental and must NOT be deployed alongside the audit until
the actual API time precision is established. The audit itself does not
modify collection, retries, reconciliation, timers or retention settings.

## Audit Stages

1. Inventory recent runs and ALL partition metadata within configured limits;
   sample run/area/coverage strata, including covered leaves, split nodes and
   unresolved branches. Report every stratum not selected, and requests whose
   raw was pruned. Include ordinary regions, not just area 144.
2. Inspect archive raw for older periods. Preserve source IDs and timestamps.
   The current area tree is only a reference, not an immutable historical tree.
3. Separate filter evidence from persistence evidence. Compare sampled stored
   seen-event IDs, source run/partition IDs, canonical item hashes, and available
   full short-snapshot payloads against raw. Do not compare historical fields
   to the mutable current vacancy row.
4. From actual reports, choose further periods/regions and investigate the
   earliest reproducible failure. Where raw has been pruned locally, retrieve
   verified archive chunks on an analysis machine under a separate download
   budget. Do not conclude those periods are clean because DB joins are empty.
5. Before any repairs, separately assess detail provenance and reconciliation
   effects (`consecutive_missing_runs`, inactivity). This tool does not certify
   detail bodies or prove that every inactivity flag is correct. Do not reset
   flags or rewrite historical coverage based on a diagnostic sample.

No response-level sample can establish that HH returned ALL matching vacancies.
No finding of a wrong query proves that the returned vacancy card is fabricated.

## Tool and Safety

`scripts/analysis/audit_search_quality.py` has database and local archive modes.
It makes NO HTTP/S3 calls, performs NO database writes, adds NO dependencies,
and needs NO migration. It only creates a new private report directory.
The DB reader starts every query in a short READ ONLY transaction with local
statement/lock timeouts. It never selects Authorization/request headers.
Errors are reported by class only, avoiding connection-string disclosure.

Defaults: 12 most recent weekly runs of all statuses, at most 100,000 partition
metadata rows per run, 4,000 sampled partitions, at most two requests (first
and last) per partition, 100 nonempty pages for provenance checks, two MB
payload size limit and 30-minute time budget. Each SQL statement has a 30s
timeout and 2s lock timeout. The time budget is checked between queries; a
query already in flight can add its timeout/connect overhead. A metadata cap
stops the audit rather than silently taking a biased UUID prefix.

Hash-seeded round-robin sampling first spreads over run/area/coverage strata,
then takes further partitions from each stratum. It is NOT a uniform random
sample of vacancies or responses. Do not interpret item percentages as
population estimates. All counts refer to selected evidence only. A different
seed can extend coverage, but reports must not simply be summed as disjoint
samples. Active runs can change between the short DB transactions.

Archive defaults: at most 1,000 local raw manifests, 100,000 rows across selected
chunks, 64 MB per compressed chunk, and the same time/line-size budgets. Chunk
order is hash-seeded, rows within a chunk are sequential. Selected chunks are
SHA256-verified before reading. Paths escaping the archive root are rejected.
Missing files, bad checksums, duplicate raw IDs, caps and skipped data are
reported. A conflicting stored hash for a duplicate raw ID is also counted.
There is no automatic offsite download. Local prune means that the local
archive alone is normally NOT a full-history corpus.

## Run on VPS After Approved Deployment

First check active jobs before pulling: code is bind-mounted. Do not stop
collection or change timer configuration without separate approval. There is
no reason to rebuild the image for this script and source-module addition.
Do not include the experimental strict collection guard in an audit-only deploy.

Run one audit at a time. This command runs in the foreground and can take
several minutes. Do not paste fragments into a live command.

```bash
cd /opt/hh_collector
report_dir=".state/reports/search-quality-db-$(date -u +%Y%m%dT%H%M%SZ)"
docker compose --profile ops run --rm --no-deps --entrypoint python app \
  scripts/analysis/audit_search_quality.py --database --output-dir "$report_dir"
```

Alternatively select specific historical runs with repeated `--run-id UUID`,
or increase `--max-runs` after the first report. The default deliberately
includes failed runs too. Investigate missing strata before increasing load.

Outputs:

- `summary.json`: source, time, runs, population/selection strata and counts.
- `evidence.jsonl`: response IDs, requested area/window, up to three anomalous
  item IDs/areas/timestamps, derivative checks and partial-failure events.
- `area-reference.json` (DB mode): dictionary snapshot used for area checks.

No full raw bodies, job descriptions, headers or tokens are printed. Reports
still contain source IDs and should not be published indiscriminately.

Exit 0 / `evidence_collected` means processing completed, NOT that data passed.
Exit 2 / `incomplete` preserves already collected evidence, including a timeout.
Never overwrite or silently merge previous reports. Share these summaries first:

```bash
jq '{status, source, sampling, counts, derivative_checks}' "$report_dir/summary.json"
jq '[.by_run_area_coverage[] | select((.counts.area_outside_current_tree // 0) > 0 or (.counts.outside_exact_window // 0) > 0)] | .[:30]' "$report_dir/summary.json"
```

## Offline Archive Pass

Prefer an analysis machine with already downloaded verified raw chunks and
their original relative paths. Copy the DB-generated `area-reference.json`
alongside the reports. This pass does not require DB access or credentials.

```bash
PYTHONPATH=src python scripts/analysis/audit_search_quality.py \
  --archive-dir .state/archive/research-production-v2 \
  --area-reference .state/reports/DB_REPORT/area-reference.json \
  --output-dir .state/reports/search-quality-archive-pass-1
```

Optional repeated `--run-id` filters archive evidence; the row budget still
counts rows read, including other runs and non-search rows. Archive rows do
not contain complete current partition metadata, so coverage status is unknown
in that mode. Do not combine DB and archive samples without deduplicating raw
IDs (the tool deduplicates within one archive invocation, not across reports).

## Interpretation

- `area_outside_current_tree`: actual item region is outside requested region
  and its known descendants in the supplied reference. Investigate hierarchy
  changes and API location semantics before declaring historic data invalid.
- `area_uncheckable`: missing area IDs/reference; not a passing check.
- `outside_exact_window`: exact timestamp mismatch, with max deviation and
  a separate over-300s count. The 300s bucket is diagnostic, NOT a tolerance
  granting coverage. Boundary rounding/HH search semantics remain unconfirmed.
- `alternate_area_absent`: diagnostic echo omission only, not proof by itself.
- `raw_missing`, `payload_size_limit`, `empty_pages`, uncheckable dates and
  unselected strata limit the conclusion. Empty pages cannot prove filters work.
- Payload/snapshot/hash/link mismatches are distinct from filter mismatches.
  DB evidence also compares area/date/role/text scope in the recorded request
  with the planned partition (pagination may legitimately differ), and checks
  request run/partition associations. Archive mode lacks the complete planned
  partition metadata and cannot perform that check.
  Legacy short snapshots without a full payload are unavailable, not corrupt.
  Absent seen/snapshot rows can be caused by retention or failed processing;
  lack of a new short snapshot is also normal for an unchanged short hash.

The first report should answer WHERE anomalies appear, whether covered leaves
are affected, and whether sampled persisted observations match their raw source.
It must not claim that every field of every stored vacancy has been certified.

## Verification State

Unit tests exercise hierarchy/descendants, unknown/cyclic references, exact
time bounds and offsets, incident replay, missing/empty/oversize evidence,
normalization, hashes, provenance mismatches, stratification and reader safety.
CLI tests exercise archive checksums, path confinement, missing chunks, row/run
limits, deduplication, private reports, partial failures and censored errors.
No live VPS audit or real PostgreSQL integration run has been performed by
these tests; the local environment has neither PostgreSQL nor Docker binaries.
