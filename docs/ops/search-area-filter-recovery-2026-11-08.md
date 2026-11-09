# Search Area Filter Recovery, 2026-11-08

## Verified Evidence

The user ran six standalone, redirect-disabled HTTPS requests from the VPS
with its configured application token and user agent. Each request specified
`describe_arguments=true`, page 0 and `per_page=1`. Each of areas 144, 17683
and 1 was tested with both `no_magic=false` and `no_magic=true`.

| Requested area | Applied area in arguments | Approximate found | no_magic effect |
| --- | --- | --- | --- |
| 144 (Autopoligon) | absent | 865,070 | none on area |
| 17683 (Antibesskaya) | absent | 865,070 | none on area |
| 1 (Moscow) | 1 | 147,617 | none on area |

For 144/17683, `alternate_url` also omitted area and the first returned item
was in Morshansk (1910). Moscow retained area in both response metadata fields.
This confirms silent loss of these two filters on this production API path;
`no_magic` is not a recovery. It does not identify the server's internal cause
or prove every other area is supported. Historical evidence already shows
the area-144 symptom in May, before the July industrial collection boundary.

The subsequent user-run parent probes retained area in both `arguments`
and `alternate_url`: 2019 (Moscow Oblast) returned 68,890 results; 1229
(Kemerovo Oblast) returned 9,182; Moscow 1 returned 147,604. These requests
used `no_magic=true`, `describe_arguments=true` and `per_page=1` without time
bounds. Parent acceptance is confirmed; completeness of those searches,
effective time buckets and individual unsupported-leaf inclusion are not.

The official [search documentation](https://api.hh.ru/openapi/en/redoc#tag/Vacancy-search/operation/get-vacancies)
defines `describe_arguments` as returning the search parameters used. Its
example represents multiple areas as repeated `argument=area` entries.
It separately documents five-minute rounding for date bounds. Do not reject
items solely for violating literal second-precision bounds.

## Guard, Deployed as bcebb83

Scoped list requests now force `describe_arguments=true`. After storing the
request and raw payload and validating response structure, the list processor
requires the applied area set to equal the requested set. Missing/malformed
arguments, dropped areas and broadened/narrowed area sets produce a
`SearchAreaScopeError`, without retrying or writing vacancies, seen events,
current state, snapshots, or successful page progress. Empty pages require
the same confirmation; they cannot prove scope by their item contents.
No hardcoded area blacklist, silent item filtering, automatic parent
substitution, or time-window rejection was added. Unscoped queries are unchanged.

The guard is necessary, not a completeness certificate: a server can report
an applied filter while still returning unexpected data. Posting area need
not equal queried area (descendants/address matching); this guard does not
use item area as its acceptance rule. It only checks newly fetched pages,
not old coverage, cached saturation decisions or already persisted progress.
Blindly resuming an old run would not repair its affected subtrees.

## Whole-Area Fallback, Deployed as bcebb83

For newly split scopes, saturated areas 2019 and 1229 now keep their full
area filter and subdivide by time, rather than replacing the parent query
with geographic child queries. This retains the intended parent scope,
including its other towns, without sending area 144/17683 queries or creating
a child-to-parent cycle. Other areas keep their existing geographic policy.
There is no dynamic upward rewrite of a leaf or silent exclusion of a town.

Each resulting time partition records `split_policy` with name
`whole_area_time_fallback_v1`, actual parent query area, unsupported descendant
ID, reason and evidence date. Recursive time children retain this metadata.
All new pages still require the actual queried area to appear in arguments.
This is an explicit evidence-backed override for two parents, not a claim
that every dictionary area is searchable or every vacancy is included.

New split points lie on a UTC five-minute grid, with both children at least
300 seconds wide. Original outer bounds are preserved, and children share
the same boundary: no literal gap or assumed rounding direction is added.
Unaligned legacy intervals are handled conservatively; if no such interior
boundary exists, saturation remains `unresolved` with
`search_resolution_seconds=300`. The policy does not invent second-level
precision or certify server bucket inclusivity/rounding ties. A saturated
five-minute interval still needs a separate, supported split dimension.

Existing `split_done` trees remain idempotent and are NOT silently rewritten
to this policy. Existing observations, branch coverage labels and raw history
are untouched. Thus plain resume of a legacy geographic tree cannot apply
the complete repair. Recovery needs a separately approved fresh tree or an
explicit, audited subtree replacement mechanism; the latter is not implemented.

Local validation includes both parent overrides, recursive metadata retention,
repeat-split idempotency, preservation of existing geographic trees, gap-free
recursive time subdivision, UTC offsets, narrow-window unresolved outcomes
and rejection before observations/progress. PostgreSQL reload tests were
added but cannot run in the local environment without PostgreSQL. No live
bounded parent search or full collection has been run by the local tests.

## Recorded Recovery and Completed Run

The user confirmed the controller/container stopped and search timer inactive
before pulling bcebb83. The interrupted tree retained 14,789 covered, 145
pending and 590 split partitions. Four subsequent probes through the production
client and new guard used adjacent UTC windows on 2026-11-01:
16:35..16:40 and 16:40..16:45.

| Area | First Window Found | Second Window Found | Applied Area and Dates |
| --- | ---: | ---: | --- |
| 2019 | 16 | 306 | retained |
| 1229 | 12 | 17 | retained |

The returned date arguments were equivalent +0300 values; the three displayed
publication timestamps per response were inside the windows. This is a bounded
smoke test, not full paging or exact boundary-inclusivity proof. No observations
were written by the probes.

At November 8 20:28:43 UTC, interrupted run
256ea99b-d563-4d57-8867-08a0abd46dbd was finalized failed with an operator note
under admission/target-row locks. Guards checked its created state/type, no
other created run and no running partitions. The existing finalizer preserved
observations/tree rows, reported 15,379 done/split partitions and zero failed
partitions, and did not reconcile the partial tree. The search timer was restored.
Normal admission first waited for 2,218 detail items; the catch-up controller
started three workers and drained the queue before a new tree was admitted.

Fresh run d0cd5cea-69fd-47e3-8696-b6235b80d10d started November 8 20:42:11 UTC
and succeeded November 9 06:47:05 UTC (~8h05m). VPS inspection at 10:17 UTC
showed 14,015 covered leaves and 1,077 split nodes, with no pending, running,
failed or unresolved partitions. Fallback evidence:

| Area | Covered Time Leaves | Split Time Nodes | Split Area Parents |
| --- | ---: | ---: | ---: |
| 2019 | 71 | 69 | 1 |
| 1229 | 16 | 14 | 1 |

All listed time nodes have whole_area_time_fallback_v1 metadata. No area
144/17683 partition appears in the supplied run-specific inventory. By 08:17 UTC
first-detail backlog was zero again. Search checks skip as not due; next due
November 15 ~20:42 UTC. At 10:17 UTC disk was 72% / 45 GiB free.

This confirms rollout and a completed planner run, not external population
completeness, detailed-card provenance or every inactivity decision. Those
require independent post-run audit. The existing auditor, even with all
partition metadata selected, checks first/last requests, NOT every response.

## Remaining Work

1. Investigate/checkpoint-catch-up the separate archive failure: run
   20261109T024347Z-4062915 started 02:43:47 UTC and stopped 07:54:02 UTC
   with max_export_batches_exhausted, max_export_batches=300. In this driver
   that branch follows local verify, offsite sync/readback verification and
   optional local prune, but precedes DB housekeeping. Confirm the actual
   step log before rerunning; do not equate this cap with an S3 connection
   failure, change retention or erase a failed status without real recovery.
2. Audit the new sweep independently, including request/page/tree accounting
   and detail/source integrity beyond sampled endpoint pages. Historical
   samples and planner success cannot measure missing vacancies.
3. Preserve failed/interrupted historical runs and their raw/history. No
   retrospective reconciliation, inactivity repair or deletion is authorized.

## First Independent Post-Fix Check

Report search-quality-postfix-20261109T103348Z finished at 10:35:36 UTC.
It selected all 15,092 partition metadata records, inspecting 17,038 first/last
requests, NOT every response/page. No checked scope, run or partition mismatch;
17,037 retained raw payload hashes matched, with one raw absent. The subsequent
full inventory confirmed 57,275 HTTP-200 requests with exactly one raw each;
the single body-less attempt was status 0 / URLError, request 9425115 (area
13358). Thus the missing sampled raw is explained, not a demonstrated lost
HTTP-200 response. Raw existence for all requests is not a hash check of all
those payloads; endpoint-only content checks remain the stated sample.

The sample contains 102,925 item occurrences, 101,993 inside the current
posting-area tree, 16 uncheckable and 916 outside it. Of the latter, 629 have
a supporting address city, 164 also have an outside-tree address city, 116
unknown city and 7 missing city. These are not 916 corrupted or fabricated
cards; the unresolved geographic cases still need source inspection.
35,867 timestamp checks produced 34 literal-bound deviations and two over
300 seconds. Their examples and effective query arguments were subsequently
inspected as recorded below; the mechanism still needs historical evidence.

The 1,000-page derivative sample checked 8,937 seen rows and 518 short snapshots,
with no checked hash, source-ID or snapshot-payload mismatches. 798 selected
pages have no snapshot, not itself a storage defect because unchanged short
payloads do not create snapshots. Presence of some seen rows is not proof all
items were persisted; missing-item accounting and all-page coverage still need
the deeper audit. No report status or zero sampled mismatch certifies corpus
population completeness, authentic content or every inactivity decision.

## Remaining Timestamp Evidence

The two large deviations are genuine content of the sampled raw, not a
demonstrated normalizer/hash/reference mismatch:

| Raw | Request | Area | Vacancy | Requested Window (UTC) | Published Value | Deviation Past Upper Bound |
| --- | --- | --- | --- | --- | --- | ---: |
| 9449305 | 9449465 | 90 | 138303968 | Oct 20 16:50..Oct 30 06:45 | Nov 9 06:53:54+0300 | 853734s |
| 9473799 | 9473959 | 1 | 136984984 | Nov 2 06:30..07:20 | Nov 9 09:34:11+0300 | 602051s |

Each response has one out-of-window item among 20, all 20 within its posting
area tree. Seven/ten-day discrepancies cannot be explained by five-minute rounding.
The user supplied the saved metadata: both responses retain area/date_from/date_to
in arguments and alternate_url, with null fixes/suggests. Both cards have type
open, is_adv_vacancy=false and no adv_response_url; Tomsk premium=false, Moscow
premium=true. Thus neither a dropped date argument nor a common explicit
advertising flag explains these two examples.

Tomsk was received 2026-11-09 03:54:44.534737 UTC, 50.534737 seconds after its
published_at. Moscow was received 06:36:35.026521 UTC, 144.026521 seconds after
its published_at. In both, created_at equals the recent published_at value.
Recent republication plus a lagging search index / hydrated card is a working
hypothesis, not a demonstrated root cause. Inspect pre-publication short
snapshots; do not use mutable current vacancy fields as historical proof.
New live fetches may see a changed index and cannot recreate the saved event.
Do not add an arbitrary freshness exemption, discard these cards or declare
historical scopes complete merely because this pattern seems plausible.

## Archive Partial Progress Confirmation

VPS logs show export-299 wrote 1,714 rows and export-300 wrote 2,418. Local
verification then succeeded (07:16:55 UTC), offsite sync (07:24:00), offsite
verification (07:53:22) and local prune (07:54:02). Only then did the driver
return max_export_batches_exhausted. This confirms verified/offsite-protected
partial progress; it does not say the eligible backlog was fully exported.
The failure branch precedes coverage-audit/DB housekeeping. A checkpointed
continuation still needs a supervised plan, without changing retention or
assuming the cap is a transport failure.

## Production Coordination

At 2026-11-08 17:20 UTC, VPS HEAD was 716985a; disk usage 71% with 46 GiB free.
Controller PID 3417556 and container hh_collector-app-run-aeaf5b03a590 were
active for run 256ea99b-d563-4d57-8867-08a0abd46dbd (started 11:20 UTC).
Two prior weekly runs failed at adjacent one-second area-144 windows.
The controller starts new due searches after failed runs when detail backlog
allows; these are not automatic resumes of the failed tree.

At 19:50 UTC the same PID and container were still active (nine hours). The
user subsequently approved publishing the fix and controlled stopping of the
old search. Stop and recovery execution were later confirmed as recorded above.

### Historical Stop Sequence

The user has already executed and verified this sequence. It is retained as
the runbook record, not a request to stop the new sweep again.

Run these commands on the VPS before any runtime pull. Stopping only the
systemd wrapper can leave its Docker container running. Do not use a broad
container kill or stop unrelated collection/backup services.

```bash
systemctl stop hhru-production-search-controller.timer
systemctl stop --no-block hhru-production-search-controller.service
docker stop -t 60 hh_collector-app-run-aeaf5b03a590
systemctl stop hhru-production-search-controller.service
systemctl show hhru-production-search-controller.service -p ActiveState -p SubState -p MainPID
docker ps --filter name=hh_collector-app-run --format 'table {{.Names}}\t{{.Status}}'
systemctl is-active hhru-production-search-controller.timer
```

Expected: MainPID 0, controller no longer activating/active, the old container
absent, search timer inactive. A failed/signal status following the manual
stop is not a new upstream incident. If another app-run container remains,
identify its command before deciding whether a pull is safe; it may be a
different maintenance job. `docker stop` may report an absent container if
it already exited and was auto-removed; still verify the process state.

Pause after this snapshot, before pull/reset-failed/restart. The interrupted
run may remain `created` in the database and must not be left blocking a new
search or relabelled succeeded. Inspect and finalize it through an explicit
guarded recovery step, preserving all observations and tree rows, before a
new full run. Ordinary resume is not that step. Do not re-enable the search
timer until the bounded smoke and recovery checks pass. Timer re-enablement
remains a required end-of-recovery step, not a permanent scheduling change.

Stop, pull, interrupted-run finalization and fresh-run completion are now
verified. No retention change, data deletion, legacy-tree rewrite or
retrospective reconciliation was part of recovery. Source/scripts remain
bind-mounted: future runtime updates still require an active-job check.
