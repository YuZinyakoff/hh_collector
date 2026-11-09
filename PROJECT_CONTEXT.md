# Project

Audit date: 2026-09-20. This file is the migration handoff for the current WSL
checkout at `/home/yurizinyakov/projects/hh_collector`.

## Purpose

`hhru-platform` is a stateful hh.ru vacancy collector for long-term research data
accumulation. It performs broad search sweeps, preserves raw API payloads and observation
history, fetches vacancy details, and maintains a cold research archive. The current focus is
reliable collection, storage, recovery, and observability; research-specific enrichment is out
of scope unless explicitly requested.

## Current State

November 9 VPS verification (10:17 UTC): bcebb83 is deployed. Fresh sweep
d0cd5cea-69fd-47e3-8696-b6235b80d10d succeeded, started November 8 20:42:11
UTC and finished November 9 06:47:05 UTC (~8h05m). Its tree has 14,015 covered
leaves and 1,077 split nodes, no pending/running/failed/unresolved rows.
Area 2019 has 71 covered / 69 split time nodes; 1229 has 16 covered / 14 split
time nodes. Both have a split area parent, with whole_area_time_fallback_v1
on their time nodes and no area-144/17683 partitions in the supplied inventory.
This proves successful repaired planner execution, NOT population completeness.
The independent post-collection source/detail/coverage audit remains pending.

Controlled stop and pull were verified. Four bounded parent probes passed
the area guard; date arguments were UTC-equivalent and displayed items fell
inside adjacent five-minute windows. Interrupted run
256ea99b-d563-4d57-8867-08a0abd46dbd was finalized failed on November 8
20:28:43 UTC under advisory/row locks, preserving observations/tree rows and
without reconciliation. Search scheduling was restored, three detail workers
drained 2,218 items, then the fresh run started. By November 9 08:17 UTC its
first-detail backlog was zero again. Search now skips as not due, with next
due November 15 ~20:42 UTC. At 10:17 UTC disk was 72% / 45 GiB free.
Separate archive incident is now identified: run 20261109T024347Z-4062915
failed at 07:54:02 UTC with max_export_batches_exhausted, max_export_batches=300.
The driver protects partial progress with verify/sync/readback before this
failure branch and skips DB housekeeping until export is complete. Confirm
step logs before planning a checkpointed catch-up; do not change retention,
raise limits or assume a new network fault. No new search is needed now.

Post-fix report search-quality-postfix-20261109T103348Z completed at 10:35:36:
all 15,092 partitions selected, but first/last requests only (NOT all pages).
17,038 requests inspected: 17,037 raw payload hashes matched, one raw absent.
Full request inventory subsequently confirmed 57,275 HTTP-200 requests with
exactly 57,275 raw rows and a single status-0 URLError attempt without raw
(request 9425115). This is an expected body-less transport attempt, not a
demonstrated lost successful response. Hash checks still cover endpoint pages,
not all 57,275 payloads. No checked scope/run/partition
mismatch. Of 102,925 item occurrences, 916 have area/tree signals: 629
address-supported, 164 address also outside, 116 unknown city and 7 missing;
16 further posting areas are uncheckable. Of 35,867 time checks, 34 are
outside literal bounds and 2 exceed 300s; inspect their actual examples.
Provenance sample: 1,000 nonempty pages, 8,937 seen rows and 518 snapshots,
no checked hash/run/partition/payload mismatches. 798 pages have no snapshot,
which alone does not imply loss (unchanged short payloads are deduplicated).
Two large time deviations are actual sampled raw items: raw 9449305 / request
9449465, Tomsk area 90, vacancy 138303968 published November 9 despite an
October 20..30 window (853,734s past upper bound); raw 9473799 / request
9473959, Moscow area 1, vacancy 136984984 published November 9 despite a
November 2 window (602,051s). Each response has only one out-of-window item
among 20; all 20 posting areas match their requested tree. These cannot be
explained by five-minute rounding. Saved metadata subsequently confirms both
area/date bounds in arguments and alternate_url; fixes/suggests null. Both
cards are open / is_adv_vacancy=false with no adv URL, but only Moscow is
premium. Publication preceded receipt by 50.534737s (Tomsk) / 144.026521s
(Moscow), and created_at equals published_at. Recent republication plus
lagging index/card hydration is a hypothesis requiring older snapshots, not
proven. Do not infer republishing, advertisement injection or client corruption as established,
drop these cards, or reintroduce the withdrawn literal-time guard.
Archive stage logs confirmed export-299=1,714 / export-300=2,418 rows, then
successful local verify, offsite sync, offsite verify and local prune before
the cap error. No DB-housekeeping step ran on that failure path. Checkpointed
catch-up is a separate pending operation; no limit/retention change approved.
Full-page accounting, whole-response content and detail provenance audits
remain pending; this report is not a corpus completeness certificate.

Read-only auditor extension is approved for publication; VPS deployment and
execution are not yet confirmed:
--all-responses for one explicit finished run, bounded keyset batches and
request/partition/seen-count inventories checked before/after. It checks all
successful raw bodies/hashes/declared area arguments, page sequence/progress,
and per-response unique-item versus seen-row counts. Full identity/hash/detail
provenance remains capped/sampled, not a population certificate. Evidence
streams to JSONL and only aggregates remain in memory. Local tests include
middle-page anomalies and incomplete/cap/source-change cases. Verification:
461 tests passed, 27 skipped; Ruff clean, mypy clean for 200 source files.
Real PostgreSQL integration was not available locally. Publication was approved
on November 9; pull/run still require checking active VPS jobs first.

### Preparation History

The following records the November 8 preparation and prior audit evidence;
rollout and run-completion status is superseded by the verification above.

November 8 live probe: with describe_arguments=true, areas 144 and 17683
are absent from response arguments and alternate_url, with ~865k global
results, for both no_magic=false/true. Moscow 1 is explicitly applied
(~147.6k results). no_magic is not a fix. The prepared list-page guard
now forces described arguments for scoped requests and rejects unconfirmed
or altered area sets after raw persistence, before observations/progress.
It does not fix historical coverage or cached split decisions. A subsequent
VPS probe confirms parents 2019 (Moscow Oblast, 68,890 results) and 1229
(Kemerovo Oblast, 9,182) retain the area argument. The prepared split
policy now uses whole-parent time windows for these two areas, with explicit
split_policy metadata, instead of descending into their towns. Time splits
use a five-minute UTC grid, at least 300 seconds per child; unsplittable
saturation stays unresolved. Old split_done trees are not rewritten: ordinary
resume does not repair them. Bounded parent smoke and deployment/recovery were
subsequently confirmed above; post-collection audit remains pending.
VPS had an active six-hour run at 17:20 UTC
on 716985a: 256ea99b-d563-4d57-8867-08a0abd46dbd, controller PID 3417556,
container hh_collector-app-run-aeaf5b03a590. At 19:50 UTC the same controller
and container were still active (nine hours). The user then approved publishing
the fix and a controlled stop for rollout. That stop, pull and interrupted-run
finalization were subsequently confirmed. Do not blindly resume the old tree.
See
`docs/ops/search-area-filter-recovery-2026-11-08.md`.

User clarification on November 7: sustained industrial collection starts in
July; May--June are debugging/reference months. Primary historical quality
conclusions must separate July--October, while keeping May--June to investigate
onset. Use observation/collection time, not vacancy publication/first-seen time.
This is a reporting cohort boundary, not a new schema or deletion permission;
see `docs/ops/data-corpus-boundary.md`.

November 7 search-quality incident: a failed November 5 sweep left 443 pending
and one unresolved leaf. Separate VPS probes and retained raw show area 144
(Autopoligon) returning unrelated regions/global results; Moscow probes did
honour area and a day window. Saved anomalies exist in October 22 and 29 runs
as well. Exact extent, onset and effective filter semantics are NOT yet established. Do not
declare all cards invalid, certify old coverage, resume blindly, or reconcile
the partial run. See `docs/ops/search-quality-audit-2026-11-07.md` for the
read-only stratified DB / offline archive auditor and its safety limits.
The read-only audit was deployed as `5b71197`. VPS deep report
`search-quality-deep-20261106T225929Z` covers first/last requests for all 41,592
partitions in the last two runs: 49,172 raw hashes matched; 8,649 seen rows
and 6,038 short snapshots had no checked source mismatches. Of 195,791
area-outside-current-tree item occurrences, 193,693 belong to scopes 144 and
17683 (Autopoligon and Antibesskaya). Covered leaves are affected too. This
does not certify coverage or all data; ten older runs in the initial sample
had no retained raw and still need archive evidence. See the audit doc.
Official HH search docs explicitly specify five-minute rounding for date
filters. Exact-window deviations MUST NOT be treated as API violations;
132,545 were recorded, only 87 exceeded 300s. The uncommitted strict-window
guard was withdrawn without deployment. The deployed splitter's 2s minimum
does not account for this granularity; the November 8 local grid correction
above is deployed and passed the new sweep. Exact server-bound semantics remain a
live-audit question. Regional expansion outside the two overrides remains unresolved.
Three saved Moscow/Petersburg anomaly items have matching `address.city`
despite different `area` IDs. The original area counter is diagnostic, not
an error verdict. Audit v2 adds address-city supporting buckets and separate
region/time examples without clearing original counters. Do not interpret
these examples as explaining all other-region occurrences or certifying
historical coverage.
Audit v2 deployed as `86c9664`. Its full two-run pass retained identical raw /
provenance counts. Excluding 144/17683, 230,444 item occurrences include 2,098
posting-area mismatches: 1,528 supported by address city, 379 with address city
also outside the requested tree, 191 missing/unknown addresses; 23 further
items have uncheckable area IDs. Only 10 exact-window deviations in these
other scopes exceeded 300s. These are diagnostic counts, not corrupted cards.
Local archive pass `search-quality-archive-20261106T233014Z` assessed zero raw:
all 1,000 selected chunks were locally absent. VPS metadata catalog has 14,806
search manifests / 3,436,037 rows across May--October (~26 GiB compressed).
Monthly sample tool deployed as `716985a`. The user downloaded 72 chunks
(12 per month, May--October), 158,438,514 bytes / 19,004 search rows, all
size/SHA256 verified. Archive audit `search-history-audit-20261106T235243Z`
read all 19,004 rows, payload hash mismatches 0; 363,504 item occurrences.
Of 221,800 posting-area/tree mismatches, 186,428 are from scope 144 and
32,034 from 17683 (98.5% together). Their 11,008 sampled responses numerically
equal the total alternate-area omissions; joint attribution is not yet checked. Two May 14
responses already show region 144 absent from alternate_url and unrelated
areas/addresses. This is historical debugging evidence, not a new November
drift explanation or a production-only statistic. July--October attribution was
subsequently computed separately as recorded below. See
`docs/ops/historical-search-audit-sample.md`.
The July--October subset has 11,263 responses / 218,686 item occurrences,
150,592 posting-area/tree signals and 8 time deviations over 300s. Scopes
144/17683 contribute 7,478 responses / 148,449 occurrences, 148,314 signals
(98.5% of production-sample signals), zero address-city support, 135 unknown
area IDs and 7 over-300s time deviations. Other scopes: 3,785 responses /
70,237 occurrences, 2,278 area signals (1,592 address-supported, 495 with
address also outside, 191 missing/unknown addresses), 11 unknown area IDs
and 1 over-300s time deviation. This is not a uniform corpus sample or a count
of unique/corrupt vacancies. No evidence justifies wiping/rebuilding the
whole corpus. Remaining questions are effective query/filter semantics,
coverage/provenance interpretation, repeated observations and inactivity.
Controlled search stop and fix publication are now approved as described above.
Stop/deployment/fresh-run recovery is now confirmed. No inactivity repair,
data deletion, legacy-tree rewrite or retention change is authorized.

October 27 archive sync investigation and local fix:
`docs/ops/archive-sync-recovery-2026-10-27.md`. Morning sync failed on S3
connection; a supervised retry started at 19:02 UTC and was still active at
20:22 (final result not yet reported). Local code now caches unchanged control
uploads, logs progress and bounds research S3 socket/retry/transfer settings.
Do not restart/pull into the active pipeline without a state check and approval.
Disk was 122G used / 33G free; backups succeeded and finished-run retention is 0.

October 22 disk/retention audit: see
`docs/ops/retention-safety-audit-2026-10-22.md`. Nightly ~18 GiB dumps explain
the filesystem warnings; PostgreSQL is 96 GB. Run-tree retention remains disabled
on production. Local audit fixes inverted latest-snapshot retention selection
and adds idle-admission/cascade-budget guards. Full run/partition metadata is
NOT archived by Archive v1: do not enable destructive run-tree retention until
the metadata-preservation decision/export and supervised procedure are complete.
Disk expansion is a fallback, not the preferred solution. No VPS cleanup was run.

Follow-up code adds separate full run/partition metadata bundles, S3 full readback
and exact-bundle/digest gating before run-tree deletion. See
`docs/ops/run-tree-metadata.md`. The user exported and S3-readback-verified run
`c7e7d8c6-6813-454c-845e-ca44539da1e8` on October 22: 18304 partitions, 2.3M
bundle at `.state/archive/run-tree-metadata/c7e7d8c6-6813-454c-845e-ca44539da1e8-9ud_8za0`
on the VPS. The real-bundle offline restore drill is still pending; production
run-tree retention must remain 0.
The user applied `docs/ops/zabbix-timeweb-firewall.md`: 10050/TCP is restricted
to the three configured Timeweb IPv4 addresses, general IPv4/IPv6 agent rules
removed, OpenSSH unchanged. Fresh Timeweb polling and a second SSH login remain
to be confirmed.

Post-reinstall audit on 2026-10-07 found a run orphaned by reconciliation OOM
on September 24. The bounded-memory fix was committed/pushed as `09cf003`
and deployed with approval. Recovery of run
`38280dea-5262-4e92-ac9c-876bc15373e2` succeeded on October 7 at 19:29 UTC:
full terminal coverage, no failed/unresolved partitions, Python memory about 216 MiB.
See `docs/ops/reconciliation-recovery-2026-10-07.md` for the original incident procedure.
On October 8, detail backlog fell from 204,245 to 3,045, backup/archive succeeded,
no systemd units were failed, and disk had 44G free. A new sweep
`a6b88bdc-4ce6-4af8-bbf2-a70511d841d5` started automatically at 10:18 UTC.
The remaining stale-scheduler alert exposed delayed metrics publication until
sweep completion. The local fix publishes admitted-attempt timing before collection,
without doubling terminal counters or changing the 8-day alert threshold.
Deploy this fix only with approval; do not restart the active sweep to clear the alert.
The restored local `.venv` points to Python 3.14 while its packages were installed
for 3.12. Tests for the fix use an isolated Python 3.12 environment under `/tmp`.

Implemented and exercised:

- planner-v2 search partitioning and exhaustive list collection;
- raw request/payload capture, vacancy normalization, snapshots, seen events, current state,
  detail attempts, and crawl run/partition lifecycle;
- retry/resume handling for transient search failures;
- first-detail backlog selection and three-worker catch-up controller;
- PostgreSQL custom-format backups, local verification, S3 upload/verification, weekly
  integrity drill, and guarded remote retention;
- Research Archive v1 (`jsonl.gz` + manifests + inventory + checkpoints), S3 verification,
  coverage-gated housekeeping, and verified local chunk eviction;
- Prometheus/Grafana/Alertmanager/Telegram observability;
- systemd timers for production search, detail catch-up, backup, research archive, restore
  drill, and offsite cleanup.

Last production state verified in the Codex session, on 2026-08-27 UTC:

- VPS: `msk-1-vm-3mxl`, checkout: `/opt/hh_collector`;
- latest successful weekly sweep `ed764338-5256-42da-8f26-b0a6e06c6a28` ran from
  2026-08-25 to 2026-08-27, observed `3,556,625` events, and created `205,067` vacancies;
- coverage was `17,474/17,474` terminal partitions, with zero pending, unresolved, or failed;
- corpus contained `3,504,147` vacancies (`980,245` created in the preceding 30 days);
- first-detail backlog was about `188k` and three detail workers were draining it;
- daily PostgreSQL backup had uploaded and verified successfully in S3;
- disk was `109G/154G` used (`46G` free, `71%`);
- the stale failed-partition alert bug was deployed, the successful run coverage was
  republished, and `HHRUPlatformFailedPartitionsPresent` resolved.

The 2026-08-27 research archive run safely exported, locally verified, uploaded, and remotely
verified 75 batches, then reported `max_export_batches_exhausted`. The production setting
`HHRU_RESEARCH_ARCHIVE_DAILY_MAX_EXPORT_BATCHES` was subsequently raised from `75` to `300`.
Later checks confirmed archive coverage audit/housekeeping success on October 7
and overall archive success on October 8.

The laptop is not the production data authority. Production PostgreSQL runs on the VPS, while
Timeweb S3 is the canonical cold store for the research archive and verified DB backups.

## Repository / Git State

State before adding this handoff file:

- branch: `master`;
- remote: `origin = https://github.com/YuZinyakoff/hh_collector`;
- HEAD: `afc88e89dac0936236a3fb8b0d67ac6769534d94` (`Fix stale failed-partition
  monitoring snapshots`);
- local `master`, local `origin/master`, and GitHub `refs/heads/master` all matched that SHA;
- no other local branches or tags;
- no staged, unstaged, untracked, or local-only commits;
- `git fsck --full` completed without errors.

Creating `PROJECT_CONTEXT.md` makes the worktree dirty by design. Review, commit, and push this
file before the wipe so the handoff exists both in the physical copy and on GitHub.

No second `hh_collector` checkout was found under this WSL home or the usual Windows locations
checked (`PycharmProjects`, Desktop, Documents, OneDrive, Yandex.Disk). This WSL checkout is the
primary local copy. The search was targeted, not an exhaustive byte-level scan of every disk.

## Architecture

Code follows domain/application/infrastructure/interface separation:

- domain entities: `src/hhru_platform/domain/entities/`;
- use cases: `src/hhru_platform/application/commands/`;
- SQLAlchemy models and repositories: `src/hhru_platform/infrastructure/db/`;
- API, archive, backup, and metrics adapters: `src/hhru_platform/infrastructure/`;
- CLI and workers: `src/hhru_platform/interfaces/`.

Critical operational tables are `crawl_run`, `crawl_partition`, `api_request_log`,
`raw_api_payload`, `vacancy`, `vacancy_seen_event`, `vacancy_current_state`,
`vacancy_snapshot`, and `detail_fetch_attempt`. Alembic currently has one head:
`0006_seen_event_payload_ref_idx`.

Docker Compose provides PostgreSQL 16, Redis 7, the application/worker/metrics containers,
Prometheus, Grafana, Alertmanager, webhook, node-exporter, and cAdvisor. Production scheduling
is host systemd plus short-lived Compose jobs, not the old continuously running scheduler
container.

## Important Decisions

- Keep the collector stateful and preserve observation history and raw payloads.
- S3 research archive is the durable long-term research dataset. It is not a low-latency
  production serving layer and does not need constant readback.
- PostgreSQL backups and the research archive are separate storage contours: backups restore
  the live DB; Archive v1 is an independently readable research data product.
- Never delete DB history merely because an upload exists. Housekeeping requires contiguous
  checkpoint coverage plus matching S3 verification receipts.
- Local archive chunks may be evicted only after matching manifest, upload receipt, and remote
  verification receipt. This reduced VPS local archive usage without losing research data.
- A partial archive run still performs local verification, S3 sync, S3 verification, and safe
  local prune before reporting batch exhaustion; incomplete coverage does not authorize
  housekeeping.
- Coverage metrics expose only the latest snapshot per `run_type`; historical run state remains
  in PostgreSQL. This prevents old recovered failures from firing forever.
- Weekly production search should not be stopped merely because detail catch-up or archive work
  is running. Admission checks, active-run guards, failed units, backlog, and free disk govern
  starts.
- Keep canonical Archive v1 in `jsonl.gz`; Parquet is a future derived analytical format.

## Local Data and Non-Git State

The checkout occupies about `3.7G`; approximately `2.9G` is ignored `.state` data. Ignored does
not mean disposable.

### A. Must preserve

- `.git/`: full local history/config. GitHub currently has HEAD, but preserve this with the
  checkout as requested.
- `.env`: local configuration and secrets. Preserve only in an encrypted/private backup; never
  commit it.
- `.state/reports/` (`~2.0G`): expensive and historically unique experiments. This includes
  `~1.8G`, 662 files of hh API captcha/recovery/auth/throughput probes plus large search baseline
  logs. Some filenames and records describe application-token experiments, so treat the copy
  as sensitive even though no secret value was intentionally inspected.
- `notebooks/.state/` (`~4.3M`, three raw probe outputs): unique experimental observations.
- `.state/backups/hhru-platform_hhru_platform_20260428T215846Z.dump` (`905,826,544` bytes):
  potentially useful local PostgreSQL snapshot. It is owned by `nobody:nogroup`, mode `0600`,
  and is unreadable by the current user. A normal user-level copy/archive can omit it.
- Any Docker PostgreSQL volume discovered before wipe, until it is explicitly confirmed
  disposable or exported. Docker is currently unavailable in this WSL session, so volume state
  could not be audited.

Safest migration choice: preserve the complete repository including dotfiles and ignored state
inside an archive created with sufficient privileges, or export the entire WSL distribution.
Verify the resulting archive, size, and checksum before wiping Windows.

### B. Prefer to preserve

- `.state/analysis/` (`~4.5M`): S3 archive samples and analysis-smoke CSV/PNG/summary outputs;
  derivable from S3, but useful and cheap to keep.
- `.state/archive/` (`~408K`): local retention smoke chunks and upload receipts; reproducible,
  but useful as evidence.
- `.state/metrics/metrics.json` (`~33K`): local operational metric snapshot; not authoritative,
  but useful for debugging.
- Local editor settings outside the repository only if personally useful. No project-specific
  VS Code configuration is required by this repo.

### C. Do not transfer

- `.venv/` (`~684M`), `.mypy_cache/`, `.pytest_cache/`, `.ruff_cache/`;
- `__pycache__/`, `*.pyc`, build/dist output;
- Docker images and stopped containers, provided any important named volumes are handled first;
- empty `.codex` and `.codex_write_test` markers.

There are currently no non-ignored untracked files except this handoff after it is created.

## Environment and Dependencies

Current development environment:

- WSL2, Ubuntu 24.04.1 LTS, Linux kernel `6.6.87.2-microsoft-standard-WSL2`;
- Python `3.12.3`; project requires Python `>=3.12`;
- package/build metadata: `pyproject.toml`, Hatchling, pip-compatible editable install;
- no Node runtime is used;
- Docker Desktop with WSL integration and Docker Compose are required for the normal stack;
- application image additionally installs `bash` and `postgresql-client`.

There is no dependency lockfile. `pyproject.toml` has bounded direct dependency ranges, but an
install after migration can resolve newer transitive versions. The old `.venv` is not a safe
portable replacement for a lockfile. The audited environment used Alembic 1.18.4, boto3 1.43.23,
Pydantic 2.12.5, SQLAlchemy 2.0.48, psycopg 3.3.3, pytest 8.4.2, mypy 1.19.1, Ruff 0.12.12,
pandas 2.3.3, and matplotlib 3.10.9.

Default localhost ports:

- PostgreSQL `5432`, Redis `6379`, metrics `8001`;
- Prometheus `9090`, Alertmanager `9093`, alert webhook `8010`;
- Grafana `3000`, node-exporter `9100`, cAdvisor `8080`.

Docker is currently not available inside this WSL distro; Docker Desktop WSL integration must be
re-enabled after reinstall. The local dump could not be checked with `pg_restore` because the
host PostgreSQL client is not installed.

## Secrets / Required Credentials

Do not put values in Git. Restore them from the password manager, provider console, Telegram/HH
application settings, or the still-running VPS configuration under `/etc/hhru-platform/`.
`.env.example` is the canonical list of supported settings.

Credential variables:

- `HHRU_DB_PASSWORD`: PostgreSQL password; restore from the chosen local/production DB config.
- `HHRU_HH_API_APPLICATION_TOKEN`: hh.ru application token; restore from the hh.ru application.
- `HHRU_GRAFANA_ADMIN_PASSWORD`: Grafana admin password; choose/restore securely.
- `HHRU_ALERT_TELEGRAM_BOT_TOKEN`: Telegram bot token; restore via BotFather/password manager.
- `HHRU_ALERT_TELEGRAM_CHAT_ID`: destination chat identifier; restore from bot configuration.
- `HHRU_BACKUP_OFFSITE_USERNAME`, `HHRU_BACKUP_OFFSITE_PASSWORD`,
  `HHRU_BACKUP_OFFSITE_BEARER_TOKEN`: legacy/non-S3 backup backend credentials if used.
- `HHRU_BACKUP_OFFSITE_S3_ACCESS_KEY_ID`, `HHRU_BACKUP_OFFSITE_S3_SECRET_ACCESS_KEY`: DB backup
  S3 credentials; restore from Timeweb/provider IAM.
- `HHRU_HOUSEKEEPING_ARCHIVE_OFFSITE_USERNAME`,
  `HHRU_HOUSEKEEPING_ARCHIVE_OFFSITE_PASSWORD`,
  `HHRU_HOUSEKEEPING_ARCHIVE_OFFSITE_BEARER_TOKEN`: legacy retention archive credentials if
  that contour is still used.
- `HHRU_RESEARCH_ARCHIVE_OFFSITE_S3_ACCESS_KEY_ID`,
  `HHRU_RESEARCH_ARCHIVE_OFFSITE_S3_SECRET_ACCESS_KEY`: research archive S3 credentials;
  restore from Timeweb/provider IAM.

Also restore GitHub authentication for the HTTPS remote and an SSH private key/config for
`msk-1-vm-3mxl`. The VPS systemd jobs use external files not stored in Git:

- `/etc/hhru-platform/production-search-controller.env`;
- `/etc/hhru-platform/detail-catchup-controller.env`;
- `/etc/hhru-platform/backup-daily.env`;
- `/etc/hhru-platform/research-archive-daily.env`;
- `/etc/hhru-platform/backup-restore-drill.env`;
- `/etc/hhru-platform/backup-offsite-cleanup.env`;
- `/etc/hhru-platform/ops-failure-notify.env`.

These remain on the VPS and should not be copied into this repository.

## How to Restore on a New Machine

1. On clean Windows, enable WSL2 and install Ubuntu 24.04. Install Docker Desktop and enable
   integration for that distro.
2. Restore/extract the project into the WSL ext4 filesystem, for example
   `/home/<user>/projects/hh_collector`, not under `/mnt/c`. Ensure hidden files `.git`, `.env`,
   and `.state` were included.
3. Install host tooling:

   ```bash
   sudo apt update
   sudo apt install -y git make python3.12 python3.12-venv python3-pip postgresql-client
   ```

4. Rebuild the Python environment:

   ```bash
   python3.12 -m venv .venv
   ./.venv/bin/python -m pip install --upgrade pip
   ./.venv/bin/python -m pip install -e '.[dev,analysis]'
   ```

5. Restore `.env` from the encrypted backup, or start from `.env.example` and re-enter secrets.
   Review it against the newer `.env.example`; the audited `.env` predates several current
   options and must not be assumed complete.
6. Confirm Docker works, then start the base stack:

   ```bash
   docker version
   docker compose version
   make up
   ```

7. Choose the database path:

   - For a fresh local DB, run `make migrate-compose`.
   - To restore the retained local dump into the new local PostgreSQL, first make it readable,
     verify it with `pg_restore --list`, then run:

     ```bash
     make restore BACKUP_FILE=/backups/hhru-platform_hhru_platform_20260428T215846Z.dump
     make migrate-compose
     ```

   The restore target is destructive to the new local DB, which is expected only on a fresh
   environment. Do not run it against production.
8. Start observability only when needed:

   ```bash
   make up-observability
   ```

9. Run the checks in the next sections. Do not start a production sweep from the laptop merely
   as a bootstrap check.

## How to Run

Base infrastructure and health:

```bash
make up
make migrate-compose
make compose-health
```

Safe local collection smoke, after configuration and only when intended:

```bash
make run-once-v2 ARGS="--sync-dictionaries no --detail-limit 0 --detail-refresh-ttl-days 30 --triggered-by local-smoke"
```

Production is operated on the VPS via the systemd timers documented in
`docs/ops/unattended-operations.md`; do not recreate production scheduling on the laptop.

## How to Test

Audited on 2026-09-20:

```bash
./.venv/bin/python -m pytest
./.venv/bin/python -m mypy src
./.venv/bin/python -m ruff check src tests scripts
```

Results: `249 passed, 19 skipped`; mypy clean; Ruff clean for `src tests scripts`.
The 19 integration tests require services/configuration and were skipped in the current
Docker-less session.

`make lint` currently fails because Ruff also scans `notebooks/`, where
`hh_api_probe_cooldown_driver.py` and `hh_api_probe_night_driver.py` have 17 pre-existing import
ordering/unused import/line-length findings.

## Known Issues

- Production health after 2026-08-27 has not been observed in this handoff. In particular,
  confirm that archive batch limit `300` allowed a complete export/coverage/housekeeping cycle.
- Docker Desktop integration is currently unavailable in WSL; local named volumes could not be
  inventoried.
- The retained April dump is unreadable by the current user and has not been checksum- or
  `pg_restore`-verified during this audit.
- No dependency lockfile exists; exact transitive dependency reproduction is not guaranteed.
- `make lint` has the 17 tracked notebook findings described above.
- Several status documents and the README contain historical wording; this file and dated ops
  evidence should be preferred when they conflict.
- Observability images use mutable `latest` tags, so a clean reinstall may pull newer versions.

## Unfinished Work

- Recheck the VPS after the `MAX_EXPORT_BATCHES=300` change and record a newer dated state.
- Continue unattended weekly-cycle/storage-growth validation.
- Confirm detail catch-up returns to zero after each weekly sweep.
- Fix notebook Ruff findings or explicitly exclude experimental drivers from the main lint scope.
- Add a dependency lock/constraints strategy if byte-for-byte environment reproducibility is
  required.
- Verify or consciously discard the old local PostgreSQL dump and any local Docker DB volume.

## Next Steps

1. Complete and verify the physical backup items below before wiping Windows.
2. Commit and push this handoff file.
3. After reinstall, restore the repository and ignored state, recreate the environment, and run
   tests.
4. Restore VPS/GitHub/S3/Telegram credentials from their authoritative sources.
5. Run a fresh read-only VPS health audit before making any operational changes.

## Migration Checklist

### Physical backup must include

- the complete repository including `.git/` and all dotfiles;
- encrypted/private copy of `.env`;
- complete `.state/`, especially `.state/reports/`, `.state/analysis/`, and the April dump;
- `notebooks/.state/`;
- optionally, the entire Ubuntu WSL export as the safest fallback;
- any Docker named volume or a fresh logical dump if local PostgreSQL contains needed data.

### Already recoverable from Git/GitHub

- all tracked source, migrations, tests, docs, Compose/systemd definitions, notebooks, and
  scripts through commit `afc88e8`;
- all Git history currently reachable from the sole local branch `master`.

### Do not transfer

- `.venv`, Python/tool caches, `__pycache__`, build output;
- Docker images and containers after checking named volumes;
- empty Codex marker files.

### Commit and push

- Commit required: **yes**, after reviewing `PROJECT_CONTEXT.md`.
- Push required: **yes**, so the migration handoff is present on GitHub as well as the physical
  backup.
- No other local code changes or local-only commits existed at audit start.

### Required separate data checks

- Resolve the unreadable `nobody:nogroup` dump: either archive/export WSL with sufficient
  privileges or deliberately change ownership/copy it with elevated privileges.
- Start/re-enable Docker Desktop before wipe and inspect `docker volume ls`; export/dump any
  local PostgreSQL volume that is not disposable.
- Keep the physical archive encrypted because it contains `.env` and authenticated API probe
  artifacts.
- Verify the physical copy by listing/extracting it and comparing its total size/checksum before
  deleting the original.

### Immediately before wiping Windows

- run `git status --short --branch` and confirm only intended state;
- run `git ls-remote origin refs/heads/master` and confirm the pushed handoff commit;
- verify the backup contains `.git`, `.env`, `.state`, `notebooks/.state`, and the unreadable dump;
- confirm access to the GitHub account, password manager, VPS SSH key, Timeweb S3 credentials,
  hh.ru application credentials, and Telegram bot settings;
- preferably create and validate a full `wsl --export` backup from Windows PowerShell;
- do not wipe until the repository/data backup has a second copy outside the laptop.

## Notes for the Next Codex Session

- Read this file first, then `docs/ops/project-status-roadmap.md`,
  `docs/ops/unattended-operations.md`, `docs/ops/research-archive-v1.md`, and
  `docs/ops/observability.md`.
- Treat the VPS and S3 as separate from laptop local state. Do not infer current production
  health from this old local checkout or from the 2026-08-27 snapshot.
- Preserve raw/history semantics and archive safety gates when changing retention.
- Do not write secrets into Git or docs.
- Before changing production, collect a fresh read-only health snapshot: failed units, timers,
  search/detail services, recent crawl runs, backlog, latest backup/archive receipts, active
  alerts, Compose health, and disk usage.
