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
guard was withdrawn without deployment after confirming documented five-minute
rounding. Do NOT reintroduce literal second-bound validation. The audit itself does not
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

## VPS Results and Contract Correction

The user ran release `5b71197`; report names below are from the VPS clock.
Initial `search-quality-db-20261106T224855Z` sampled 4,000 partitions over
12 runs. Only 719 of 4,241 selected responses still had raw in the DB, all
from the October 29 and November 5 runs. Ten older runs were not assessed
from raw, not certified clean. Runtime was about 29s.

Deep `search-quality-deep-20261106T225929Z` examined first/last requests of
ALL 41,592 partitions of those two runs, NOT every page. Runtime was about
5m22s. Retained raw was present for all 49,172 selected responses. All payload
hashes matched. The derivative sample had 8,649 seen rows and 6,038 short
snapshots without checked ID/hash/link/full-payload mismatches. 102 pages had
no snapshots; this alone is not corruption or missing observations.

Of 424,471 item occurrences, 195,791 were outside the supplied area tree.
Scopes 144 (Autopoligon) and 17683 (Antibesskaya) account for 193,693 (98.9%)
of these, including 110,727 of the 112,107 outside-area occurrences in covered
leaves. The other scopes account for 2,098 occurrences. These are NOT unique
vacancy counts or a uniform corpus sample. Moscow/Petersburg examples show
more other-area items on some tail pages, but the reason is unconfirmed.

267,906 items were checked against exact time bounds: 132,545 were outside,
only 87 by more than 300s. Official [vacancy search documentation](https://api.hh.ru/openapi/redoc#tag/Poisk-vakansij/operation/get-vacancies)
specifies five-minute rounding for BOTH `date_from` and `date_to`. Therefore
the exact-window counter is NOT a count of violated API guarantees. Most
deviations are compatible with that documented granularity, but the 87 larger
ones still need source inspection. Direction, tie rules and upper-bucket
inclusion must be established before implementing effective-bound validation.
The 300s bucket is only a diagnostic envelope, not proof an item is valid.

The production midpoint splitter uses a 2s minimum. Thus it does not account
for the documented granularity and can split without actually narrowing the
effective query. This is an algorithm/contract gap, not evidence that all
historical vacancy cards are false. No splitter repair is deployed yet.

The local strict-second guard was withdrawn; it was never committed/deployed.
The read-only auditor's counters remain unchanged for report compatibility.
An absence of raw/snapshot corruption in the selected checks does not certify
vacancy authenticity, population completeness or inactivity correctness.

Further saved-item inspection found that the three selected non-144/non-17683
examples have an address city matching the requested city despite a different
`area`: raw 8479935 / vacancy 138399935 (requested Petersburg, area Belgorod,
address Petersburg); raw 8496636 / 138455068 (requested Moscow, area
Yekaterinburg, address Moscow); raw 8876887 / 139063386 (requested Moscow,
area Yartsevo, address Moscow). Therefore `area_outside_current_tree` alone
cannot be interpreted as a number of erroneous results. This does NOT prove
the actual server filtering rule or resolve all 2,098 other-area occurrences.

Local audit v2 adds address-city supporting evidence while retaining the
original area/time counters. It also emits separate `area_examples`,
`time_examples`, and `time_over_300s_examples` (up to three each), because
the v1 shared example limit could hide a region anomaly behind time examples.
The address-aware pass was deployed as `86c9664` and ran on VPS as
`search-quality-address-20261106T232213Z`. Original area/time totals remained
unchanged. Excluding 144/17683, 2,098 area-outside occurrences comprise 1,528
with a supporting address city, 379 with address city also outside the requested
tree, 165 with unknown city and 26 missing address city. Another 23 occurrences
have uncheckable posting-area IDs. Only 10 over-300s time deviations occur in
these other scopes. These remain diagnostic signals, not corrupted-card counts.
Earlier reports are unchanged.

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

The first production local-only pass selected 1,000 manifests, all with local
data files missing: zero responses were assessed. The source manifest catalog
covers May--October (~26 GiB compressed). Use the separately budgeted plan /
explicit S3 readback procedure in `historical-search-audit-sample.md` rather
than repeating local-only scans or downloading the full archive to the VPS.

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
- `area_outside_address_supports_requested_area`: posting-area tree differs,
  but the address city name resolves entirely within the requested area tree.
  This is supporting evidence, NOT certification of a correct response. The
  original `area_outside_current_tree` count is not reduced or cleared.
- Other `area_outside_address_*` buckets distinguish missing city, unavailable
  name reference, unknown city/tree, ambiguous names and a city outside the
  tree. City strings are only case/space/yo-normalized dictionary-name matches:
  no geocoding or speculative coordinate/name inference. Multiple same-name
  cities with conflicting memberships remain ambiguous, not accepted. Old
  references lacking names remain uncheckable. Only city names are included
  in evidence, never full street addresses or coordinates.
- `outside_exact_window`: literal timestamp mismatch, with max deviation and
  a separate over-300s count. HH documents five-minute rounding, so this counter
  is NOT an API-violation count. The 300s bucket is diagnostic, NOT a tolerance
  granting coverage. Exact effective-bound semantics remain unconfirmed.
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
No real PostgreSQL integration run was performed by the local automated tests;
the local environment has neither PostgreSQL nor Docker binaries. The user
subsequently ran both DB audits successfully on the VPS as recorded above.
