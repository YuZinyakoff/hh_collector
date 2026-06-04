# Unattended Operations

Runbook для перехода от проверенных manual storage-команд к расписанию, которое
можно наблюдать через systemd, логи и Telegram alerts.

## 1. Текущая Граница

На 2026-06-04:

- `hhru-research-archive.timer` включён на VPS;
- supervised `daily-research-archive` smoke прошёл полностью;
- daily backup и weekly offsite restore drill drivers реализованы, но их
  supervised VPS smoke и timers ещё не выполнены;
- общий systemd failure notifier реализован, но synthetic Telegram smoke ещё не
  выполнен;
- автоматический destructive research housekeeping и S3 backup cleanup
  отсутствуют намеренно.

Production collection calendar не входит в этот storage schedule. Текущий
hourly `scheduler-loop` нельзя включать как production policy без отдельного
решения по search/detail cadence.

## 2. Безопасное Расписание

| Unit | Schedule | Purpose |
| --- | --- | --- |
| `hhru-daily-backup.timer` | daily `00:30 UTC` + up to `15m` jitter | create, local verify, S3 sync and exact remote verify |
| `hhru-research-archive.timer` | daily `02:30 UTC` + up to `15m` jitter | settled export, local/S3 verify, coverage audit and read-only preview |
| `hhru-weekly-backup-restore-drill.timer` | Sunday `06:00 UTC` + up to `30m` jitter | restore newest offsite-verified backup into a temporary DB |

All three drivers use `.state/locks/heavy-ops.lock` and wait up to six hours.
This serializes heavy PostgreSQL, disk and S3 work even after a delayed
`Persistent=true` timer start.

Daily backup local dump retention defaults to two days through
`HHRU_BACKUP_DAILY_LOCAL_RETENTION_DAYS=2`. This is intentionally lower than the
old VPS value because one current dump is approximately `13 GB`. Verification
receipts and manifests remain available for the offsite restore drill.

## 3. Safety Boundary

Automated drivers are fail-closed:

- each driver has its own lock in addition to the shared heavy-ops lock;
- each step writes a separate log under `.state/logs/`;
- a failed local verification prevents offsite operations;
- daily backup remote-verifies the exact dump created by the same run;
- weekly drill selects only a backup with an adjacent
  `.dump.offsite.verified.json` receipt;
- the temporary restore-drill database is dropped after success and cleanup is
  attempted after failure;
- no driver invokes `cleanup-backup-offsite --apply`;
- no driver invokes `apply-research-archive-housekeeping`.

S3 backup retention remains a gate before multi-month unattended operation.
Daily backups will grow remote storage until a real safe deletion candidate has
passed dry-run review and the bounded retention apply policy is operationally
proven.

## 4. Failure Delivery

Storage services use:

```text
OnFailure=hhru-ops-failure-notify@%n.service
```

The notifier POSTs a synthetic critical Alertmanager payload to the existing
host-bound `alert-webhook`, which forwards it to Telegram when Telegram
credentials are configured.

Synthetic smoke before enabling new timers:

```bash
cd /opt/hh_collector

SYNTH_UNIT=hhru-ops-failure-notify@hhru-synthetic-ops-test.service.service
systemctl start "$SYNTH_UNIT"
systemctl show "$SYNTH_UNIT" \
  --property=Result \
  --property=ExecMainStatus \
  --no-pager
./scripts/ops/notify_systemd_failure.sh hhru-synthetic-direct-test.service
```

The systemd unit must report `Result=success` and `ExecMainStatus=0`. The direct
command must return `operation=notify_systemd_failure status=succeeded`.
Telegram must receive messages naming `hhru-synthetic-ops-test.service` and
`hhru-synthetic-direct-test.service`.

## 5. Supervised Rollout Order

Do not enable the backup timers before these steps pass in order:

1. Deploy the new scripts and systemd units.
2. Reinstall the archive service so its shared lock and failure hook become
   active.
3. Run the synthetic failure notification smoke.
4. Run one supervised `make daily-backup`.
5. Run one supervised `make weekly-backup-restore-drill`.
6. Review disk usage, logs and absence of a lingering restore-drill database.
7. Enable the daily backup and weekly restore-drill timers.

Install or refresh units:

```bash
cd /opt/hh_collector

install -m 0644 deploy/systemd/hhru-ops-failure-notify@.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-research-archive.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-daily-backup.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-daily-backup.timer /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-weekly-backup-restore-drill.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-weekly-backup-restore-drill.timer /etc/systemd/system/

systemctl daemon-reload
```

Run supervised smoke jobs sequentially in tmux:

```bash
tmux new-session -d -s daily-backup-smoke \
  "cd /opt/hh_collector && set -o pipefail && time make daily-backup 2>&1 | tee /tmp/hhru-daily-backup-smoke.log"
```

After daily backup succeeds:

```bash
tmux new-session -d -s backup-restore-drill-smoke \
  "cd /opt/hh_collector && set -o pipefail && time make weekly-backup-restore-drill 2>&1 | tee /tmp/hhru-backup-restore-drill-smoke.log"
```

Enable new timers only after both jobs pass:

```bash
systemctl enable --now hhru-daily-backup.timer
systemctl enable --now hhru-weekly-backup-restore-drill.timer

systemctl list-timers \
  hhru-daily-backup.timer \
  hhru-research-archive.timer \
  hhru-weekly-backup-restore-drill.timer \
  --all --no-pager
```

## 6. Operator Checks

Check active or failed jobs:

```bash
systemctl --failed --no-pager
systemctl list-timers 'hhru-*' --all --no-pager
docker ps --filter name=hh_collector-app-run \
  --format 'table {{.Names}}\t{{.Status}}'
```

Inspect the latest unit executions:

```bash
journalctl -u hhru-daily-backup.service --since yesterday --no-pager
journalctl -u hhru-research-archive.service --since yesterday --no-pager
journalctl -u hhru-weekly-backup-restore-drill.service --since '8 days ago' --no-pager
```

Inspect driver logs and storage:

```bash
du -sh .state/backups .state/archive/research-production-v2
df -h /opt/hh_collector
find .state/logs/backup-daily -mindepth 1 -maxdepth 1 -type d | sort | tail -3
find .state/logs/research-archive-daily -mindepth 1 -maxdepth 1 -type d | sort | tail -3
find .state/logs/backup-restore-drill -mindepth 1 -maxdepth 1 -type d | sort | tail -3
```

For the first `3-7` days, review every run. After a clean soak, move to
alert-driven monitoring plus a weekly checklist. Multi-month unattended
operation still requires a proven bounded S3 backup retention apply policy and a
separate production collection calendar.
