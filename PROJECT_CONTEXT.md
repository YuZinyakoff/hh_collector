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

Post-reinstall read-only audit on 2026-10-07 confirmed local/GitHub/VPS HEAD
`afc88e8`. Production search is blocked by an orphaned run after a confirmed
reconciliation OOM on September 24. Daily archive/S3 verification still works.
See `docs/ops/reconciliation-recovery-2026-10-07.md` for evidence and the
pending recovery procedure. The bounded-memory fix is implemented locally;
production deployment/recovery still requires separate approval.
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
No later production check is present in this repository or chat handoff. After migration,
verify that later archive runs completed coverage audit and housekeeping successfully.

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
