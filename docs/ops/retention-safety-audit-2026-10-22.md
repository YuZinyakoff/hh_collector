# Retention safety audit (2026-10-22)

## Production evidence

The VPS has a 154G root filesystem, 117G used and 38G available after cleanup.
Daily verified PostgreSQL backup creates a 19,347,916,501-byte local dump
(about 18 GiB), then deletes it after successful S3 verification. Filesystem
warnings on October 18-22 resolved just after this prune. The warning is real,
not a reason to raise its threshold. Disk expansion is not the preferred solution.

PostgreSQL database: 96 GB; WAL: 560 MB. Main relations including indexes:
`vacancy_snapshot` 42 GB, `vacancy_seen_event` 19 GB, `raw_api_payload` 19 GB.
Images/cache are not the source of growth. Weekly sweeps on October 8 and 15
succeeded; the October 22 sweep was active during this audit.

Production daily retention currently keeps raw payloads and snapshots for
14 days; detail-attempt and finished-run retention are disabled. October 18-21
housekeeping deleted zero rows. October 22 deleted 295,045 rows in five passes.
Do not infer ineffective cleanup or a particular amount of bloat from dead-tuple
estimates alone. DELETE/vacuum generally makes space reusable within PostgreSQL,
not immediately available on the host. No VACUUM FULL, reindex, manual deletion,
or production retention change was performed during this audit.

Read-only preview with finished-run retention 30 days found 15 finished roots
older than the finished-at cutoff, all within verified seen-event coverage.
The first root alone has 18,304 partitions and 2,309,443 observations.
The generic delete limit counts roots, NOT cascade rows.

## Findings and local safeguards

- Confirmed bug: snapshot candidate selection used `NOT EXISTS newer_snapshot`,
  selecting the latest rather than superseded snapshot of each vacancy/type.
  Both count and selection now require `EXISTS newer_snapshot`; timestamp ties
  are broken by id. The existing real-PostgreSQL housekeeping test exposed this
  bug. New non-service SQL regression tests prevent skipping its coverage when
  PostgreSQL is unavailable. This may have removed latest old snapshots locally;
  coverage-gated deletion means their archive copies should remain. It does not
  prove that every affected snapshot has been individually restored/read back.
- Run-tree apply now shares the collection admission advisory lock until the
  transaction ends, rejects any `created` run, and uses transaction-local 5s
  lock / 5min per-statement timeouts. This serializes normal `trigger-run-now`
  starts, not arbitrary SQL or manual commands bypassing admission.
- Apply has independent `--run-tree-delete-limit` (default 1) and
  `--max-run-tree-cascade-rows` (default 100000). The latter counts partitions
  plus observations, not SET NULL updates, WAL bytes, or disk space. All counts
  and checks precede any target deletion. A larger batch requires explicit
  review/override. Defaults deliberately reject the production 2.3-million-row
  root. Preview still uses its generic limit; its selected roots may exceed the
  independent apply root limit. Set preview's generic limit to 1 for a first audit.
- Existing verified checkpoint/receipt-chain audit, age/source-id bounds,
  root lock recheck, exact delete-count checks and transaction rollback remain.
  Receipt-chain completeness is not a fresh full download of every S3 object;
  offsite verification checks sizes and only configured sample readbacks.
- With finished-run retention set to 0, the daily raw/snapshot path does not
  acquire the new admission lock and keeps working during collection.

## Data contract

Deleting a finished root cascades to its partitions and observations. It does
NOT delete vacancies, current states, snapshots, detail attempts, request logs,
or raw payloads. The separate age-based targets can delete eligible raw/snapshot
rows in the same apply command, so disable those targets for an isolated run-tree
experiment. Related nullable run/partition links are SET NULL, including
`vacancy_current_state.last_seen_run_id`; other current-state fields remain.
Reconciliation uses observations for the current run and supports a NULL previous
run link. Global first-detail drain does not require the deleted historical run.
Run-specific CLI reports for deleted ids will no longer work, historical SQL
observation counts will shrink, and original run/partition joins disappear.
The scheduler reads the last successful run from PostgreSQL: retain recent
successful sweeps and verify this query before and after a cleanup. A 30-day
policy is a proposal, not an approved or deployed production setting.

Archive `silver/vacancy_seen_event` preserves observation ids, vacancy ids,
run/partition ids, timestamps, positions and payload references/hashes. It does
NOT preserve full `crawl_run` configuration/status/notes or partition parameters,
split tree, errors and counters. Therefore "no data loss" is NOT established
for deleting technical trees. A rotating DB backup is only temporary protection,
not a permanent archive of that metadata. Daily archive is not a full DB restore.

## Required before any production run-tree apply

1. Deploy the snapshot fix and safeguards first, without enabling run-tree retention.
2. Decide whether all technical metadata must remain recoverable. If yes, add a
   durable export of exact selected run/partition records, with matching S3
   verification/readback and a documented reconstruction test, before deletion.
   This export/reconstruction is not implemented by this change.
3. Confirm approved retention, fresh verified DB backup and relevant archive
   readback. Read-only preview must remain coverage-complete with no issues.
4. Wait for the current sweep/reconciliation to finish. Do not disable or restart
   it merely to make retention run. Check workers, last successful sweep and
   free space. Do not run manual producers/resume commands concurrently.
5. Review one exact root, its SET NULL references and cascade counts; use a
   supervised operation with explicit budgets/timeouts, not the daily generic
   50000-root allowance. An explicit id allowlist/durable execution plan is not
   implemented: apply replans oldest eligible roots, so preview is not approval
   of an immutable root selection.
6. Compare vacancy/current-state invariants, latest sweep, detail backlog, errors,
   WAL and free space afterward. Do not promise OS-visible disk reclamation from
   DELETE. A separate space-reclamation plan needs measurements and downtime/
   free-space analysis; no table rewrite is authorized here.

## Validation

Python 3.12 / SQLAlchemy 2.0.48: full suite without services, 268 passed and
24 skipped. Targeted isolated PostgreSQL suite: 6 passed (including the existing
housekeeping repository test). Ruff (`src tests scripts`) and mypy passed.

Real PostgreSQL 16 tests cover FK cascades/SET NULL, preservation of payloads and
current-state fields, reconciliation policy with NULL last-seen links, full
rollback of deleted roots and nullified links, active-run/coverage exclusions,
admission lock contention/release, latest-snapshot retention and timestamp ties.
The local bundled PostgreSQL lacks pgcrypto: these runs use the ORM-created
schema, not a successful full Alembic migration. Unit schema-contract tests also
check migration/model FK contracts. Small fixtures do not measure production-scale
WAL, blocking time or throughput. No production S3 contents were fetched here.

## Separate host security follow-up

Timeweb Zabbix agent was already installed. Its installer opened 10050/TCP to
all IPv4/IPv6 sources; agent `Server` lists three provider IPv4 addresses.
Restricting UFW to those sources is a separate pending host change; preserve
OpenSSH access. Do not rerun an HTTP-downloaded root shell installer.
