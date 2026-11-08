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

## Prepared Guard, Not Deployed

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

## Whole-Area Fallback, Not Deployed

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

## Remaining Recovery Work

1. Recheck active VPS services, container and run state; obtain approval for
   controlled stop, commit/push and runtime deployment. The user has approved
   publication and controlled stop; execution is not yet confirmed. No pull into live code.
2. Verify bounded parent queries and their effective arguments before admitting
   a new full collection. The unbounded parent probes are not that smoke test.
3. Choose a fresh tree or separately designed subtree recovery, preserve all
   old observations and do not reconcile the failed/partial legacy sweep.
4. Audit new search coverage and details independently; the historical
   sample does not measure missing vacancies or certify complete sweeps.

## Production Coordination

At 2026-11-08 17:20 UTC, VPS HEAD was 716985a; disk usage 71% with 46 GiB free.
Controller PID 3417556 and container hh_collector-app-run-aeaf5b03a590 were
active for run 256ea99b-d563-4d57-8867-08a0abd46dbd (started 11:20 UTC).
Two prior weekly runs failed at adjacent one-second area-144 windows.
The controller starts new due searches after failed runs when detail backlog
allows; these are not automatic resumes of the failed tree.

At 19:50 UTC the same PID and container were still active (nine hours). The
user subsequently approved publishing the fix and controlled stopping of the
old search. This is approval, not confirmation that the VPS was stopped.

### Approved Stop Sequence

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

No production stop, pull, deployment, run reset, retention change or
reconciliation has been performed as part of this local fix. Source and
scripts are bind-mounted: do not pull into an active pipeline. Confirm current
state and follow the approved controlled stop before runtime rollout.
