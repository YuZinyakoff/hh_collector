# Search Window Mismatch, 2026-11-07

Broad audit tooling and later VPS probe evidence are documented in
`search-quality-audit-2026-11-07.md`. The strict collection guard below is
experimental, not approved for rollout: actual time-filter precision is not
established. Deploy the read-only audit separately from that guard.

## Production Evidence

Run `aae93d03-f35d-4315-ac94-2c14f75b883b` failed on partition
`799297d9-ef92-4632-b0b8-09d8d73d0916`. Request `8986186` recorded
area `144` and UTC window `2026-11-01T16:40:01..16:40:02`. Raw payload
`8986030` reported 2324 results / 100 pages. The supplied first five
items were published around `19:44+0300` (outside the requested window)
and belonged to unrelated regions, including Minsk and Saint Petersburg.
443 pending leaves and one unresolved leaf remained. Reconciliation was
skipped. Existing observations must not be interpreted as a complete sweep.

The stored response violates the requested time bounds. This evidence does
not establish when the behaviour began or whether the cause is upstream,
redirects, configuration, or another transport intermediary.

## Local Findings and Guard

The client builds URLs using `urlencode`, including area and aware ISO date
strings. Each synchronous request returns its own response; no response cache
is implemented. A regression test inspects the actual outgoing request URL,
including encoded UTC offsets, and two consecutive request/response pairs.
It does not exercise a real network or rule out production redirects.

Historically the list processor validated response structure, not publication
timestamps against the query window. That is a pre-existing validation gap,
not proof that historical API responses were wrong.

The new guard runs after request/raw persistence but before vacancy upsert,
seen events, current-state updates, snapshots or page-progress advancement.
For bounded searches, each item must have an aware `published_at` inside
the inclusive requested bounds. Missing dates or invalid/naive bounds fail
safely via the existing normalization-failure path. No items are silently
filtered, no extra automatic retries are added, and the page is not treated
as saturated or covered after rejection. Unbounded searches are unchanged.

This is intentionally strict: if upstream rounds time filters, it will also
reject those responses rather than claim coverage. Empty results provide no
item-level evidence that filters were honoured. Region validation is not
implemented: parent-area queries legitimately include descendant areas.
Other search filters are also outside this guard's scope.

## Recovery Constraints

- Do not resume the failed run before identifying the response mismatch cause.
- Do not mark the unresolved branch covered or reconcile this partial sweep.
- The guard does not repair observations already stored or certify old runs.
- Do not delete historical data based on this incident alone.
- Before rollout, check for active collection processes; code is bind-mounted.
- No migration or retention setting change is needed.
- Commit, deployment and production recovery require separate approval.

Further evidence should compare request/response associations and bounded
historical samples, then perform a separately approved bounded live probe
with redacted configuration and redirect inspection. A production history
audit and live probe have not been performed by the local regression tests.
