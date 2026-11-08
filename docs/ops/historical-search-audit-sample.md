# Historical Search Audit Sample

## Scope

The VPS manifest catalog reported 14,806 search chunks / 3,436,037 search
response rows across May--October 2026, approximately 26 GiB compressed.
The first local archive audit found 1,000 selected manifests with their local
chunks absent, and checked zero responses. This is not a passing quality
result or proof the chunks still exist in S3. The next step is bounded S3
readback, not downloading the whole historical corpus onto the collector VPS.

`scripts/analysis/prepare_search_audit_sample.py` separates offline planning
from explicitly requested remote reads. No DB access or live HH requests are
used in either mode. Neither mode uploads or deletes anything in S3 or
modifies the source archive. A separate private directory holds the sample.

Defaults: up to 12 chunks per observed month, hash-seeded ordering within a
month, round-robin months, at most 32 MiB per data object, and 256 MiB total
for selected data objects PLUS remote manifests. For the six known months
this means at most 72 chunks. Oversize/budget exclusions and months with no
selected chunks remain visible. This is a chunk sample, not a uniform sample
of vacancies, and cannot certify the entire history. Dates refer to raw
receipt time, not vacancy publication time or collection run start time.

## Plan First

After an approved audit-only deployment, run on the VPS:

```bash
cd /opt/hh_collector
sample_dir=".state/reports/search-history-sample-$(date -u +%Y%m%dT%H%M%SZ)"
docker compose --profile ops run --rm --no-deps --entrypoint python app \
  scripts/analysis/prepare_search_audit_sample.py plan \
  --archive-dir .state/archive/research-production-v2 \
  --output-dir "$sample_dir"
```

This reads ONLY local search manifests, not chunks, and makes zero network
requests. It creates `plan.json` with exact object keys, sizes, SHA256 values,
selected-row counts, months and exclusions. No settings/credentials are loaded.
Review `months`, `planned_rows` and `planned_download_bytes` before download.
An empty/invalid plan is rejected by download. Do not change the byte budget
without approval or interpret unselected periods as assessed.

## Explicit Download

Only after reviewing and approving the plan:

```bash
docker compose --profile ops run --rm --no-deps --entrypoint python app \
  scripts/analysis/prepare_search_audit_sample.py download \
  --plan-file "$sample_dir/plan.json"
```

Uses configured research-archive S3 credentials, with the existing backup S3
fallback. This mode issues only HEAD and GET for planned keys (no LIST/PUT/
DELETE). HTTPS is required. Client connect/read timeouts are 10/60s, standard
retries allow 3 total API attempts. The 15-minute budget is checked between
operations and streaming reads; in-flight operations may add timeout/retry
overhead. SDK retries/protocol overhead mean planned object bytes are not an
absolute wire-traffic/billing guarantee.

Both HEAD and GET size must exactly match the plan. Streaming checks prevent
writing an oversized response even if the object changes between HEAD and GET.
Each object is SHA256-verified before accepting its filename. The downloaded
remote manifest must match the locally planned manifest's SHA256 and its data
key/size/hash must agree with the plan. Relative paths are confined to the
search-data prefix and the new sample root. Invalid manifests, duplicate data
keys and object-key collisions are rejected instead of silently skipped.

Verified objects go under `$sample_dir/archive/v1/...`, with original relative
paths. Incomplete transfers remove only their own temporary files. A failure
keeps already verified files, reports `incomplete`, and never writes into the
live archive. `download-report.json` records completion, verified counts/bytes
and censored errors (safe missing-key/access codes may be included). This is
not a retention/coverage verification receipt and cannot authorize deletion.
Neither the script nor the collector will automatically prune this sample.

Existing sample/output directories are not overwritten. A fresh attempt
requires a new plan directory; retries may download objects again. Protect
reports as they contain corpus object paths and IDs, but no credentials.

## Audit Downloaded Evidence

Proceed only when `download-report.json` says `download_verified`:

```bash
archive_report=".state/reports/search-history-audit-$(date -u +%Y%m%dT%H%M%SZ)"
docker compose --profile ops run --rm --no-deps --entrypoint python app \
  scripts/analysis/audit_search_quality.py \
  --archive-dir "$sample_dir/archive" \
  --area-reference .state/reports/search-quality-address-20261106T232213Z/area-reference.json \
  --output-dir "$archive_report" --max-manifests 1000 --max-rows 200000
```

If the plan contains more than 200,000 rows, the audit row cap truncates the
sample; review this before raising it. The auditor verifies gzip chunk hashes
again, uses the supplied current area reference (not a historical dictionary),
and retains diagnostic rather than pass/fail interpretation of time/region
signals. Partition coverage status is unknown for archive rows. Do not sum
DB and archive reports as disjoint data without deduplicating source raw IDs.

Tests use fake S3 HEAD/GET clients and synthetic local manifests: six-month
stratification, deterministic plans, size/budget/path limits, checksum and
HEAD/GET races, partial-file safety, secret censorship and interoperability
with the archive auditor. They do not establish that production S3 objects
exist. The user subsequently verified all selected objects and ran the archive
audit on VPS as recorded below; those production executions are distinct
from the synthetic automated tests.

## Recorded VPS Evidence

Tool commit `716985a` selected 12 chunks in each month May--October: 72 chunks,
19,004 raw response rows, 158,438,514 planned object bytes. All were downloaded
and size/SHA256 verified. Report `search-history-audit-20261106T235243Z` read
all 72 chunks / 19,004 rows, with no raw payload hash mismatches. It contains
363,504 item occurrences, not unique vacancies.

221,800 occurrences have posting area outside the current reference tree.
Scope 144 accounts for 186,428 and scope 17683 for 32,034 (98.5% combined).
Their 9,394 and 1,614 sampled responses sum to 11,008, equal to the total
alternate-area omissions in this archive sample. This equality of aggregate
counts is consistent with the same known symptom, not a full per-response
proof of which server filter was used. Other scopes still need attribution.
Across all six months there are 72,936 literal-window deviations, only 16
exceeding 300s; literal deviations alone are not violations of the documented
five-minute rounding contract.

On November 7 the user clarified that May--June were debugging months and
industrial collection starts in July. Therefore July--October is the primary
historical quality cohort; May--June is reference/onset evidence. Filter by
raw receipt/collection month, NOT vacancy publication or first-seen date.
Do not apply the combined six-month percentages to the production corpus.
The separately computed July--October subset
contains 11,263 responses / 218,686 item occurrences, 150,592 posting-area/tree
signals and 8 deviations over 300s. See `data-corpus-boundary.md`.
No corpus has been deleted, repaired, certified complete or re-tagged by this audit.

## Industrial Cohort Attribution

The user recomputed July--October independently from the existing evidence:

| Scope Group | Responses | Item Occurrences | Area/Tree Signals | Address Supports Query | Address Also Outside | Unknown Area IDs | Time Over 300s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 144 + 17683 | 7478 | 148449 | 148314 | 0 | 102329 | 135 | 7 |
| Other | 3785 | 70237 | 2278 | 1592 | 495 | 11 | 1 |

98.5% of area/tree signals in this industrial sample come from the same two
small scopes; their output is not supported by a matching address city.
The remaining 191 other-scope signals have unknown/missing city names. The
495 cases where both declared area and address city lie outside the query
still need interpretation, not automatic rejection as corrupted cards.

This audit does not support declaring the whole corpus corrupted or wiping
it. Verified raw/source consistency and geographic-search validity are
different properties. It also does not certify complete historical search
coverage, authentic job content, correct inactive flags, or unbiased counts
of observations. Investigation should now move to effective server-side
arguments, five-minute splitting, and explainable coverage/recovery, rather
than more repetitions of the same sample or an unreviewed full recollection.
