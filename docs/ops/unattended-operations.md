# Unattended Operations

Runbook для перехода от проверенных manual storage-команд к расписанию, которое
можно наблюдать через systemd, логи и Telegram alerts.

## 1. Текущая Граница

На 2026-06-23:

- `hhru-research-archive.timer` включён на VPS; первый unattended запуск
  2026-06-05 завершился успешно: local verify `1557/1557` manifests, offsite
  verify `1557/1557` manifests и `29` checkpoints, housekeeping preview
  `0` actions;
- checked daily research archive timer run 2026-06-08 завершился успешно:
  local verify `1557/1557` manifests, offsite verify `1557/1557` manifests и
  `32` checkpoints, housekeeping preview `0` actions;
- daily research archive timer continued cleanly through 2026-06-15: latest
  checked run had local verify `1557/1557`, offsite verify `1557/1557`,
  `39` verified checkpoints, coverage `complete`, housekeeping preview
  `0` actions;
- daily research archive timer continued cleanly through 2026-06-23: latest
  checked run had local verify `1557/1557`, offsite verify `1557/1557`,
  `47` verified checkpoints, coverage `complete`, housekeeping preview
  `0` actions;
- supervised `daily-research-archive` smoke прошёл полностью;
- daily backup systemd driver прошёл supervised VPS smoke;
- первый unattended daily backup timer run 2026-06-06 завершился успешно:
  create, local verify, offsite sync и offsite verify all succeeded;
- checked daily backup timer run 2026-06-08 завершился успешно: backup uploaded
  to S3 and verified as `199` offsite objects;
- daily backup timer continued cleanly through 2026-06-15: latest five checked
  runs 2026-06-11..2026-06-15 all completed create, local verify, offsite sync
  and offsite verify; each verified `199` offsite objects;
- weekly offsite restore drill systemd driver прошёл supervised VPS smoke;
- `hhru-daily-backup.timer` и `hhru-weekly-backup-restore-drill.timer` включены
  2026-06-05;
- первый unattended weekly restore drill timer run 2026-06-07 завершился
  успешно: latest offsite-verified backup восстановлен во временную DB,
  cleanup step passed, lingering restore DB отсутствует;
- second unattended weekly restore drill timer run 2026-06-14 also succeeded:
  `198/198` parts downloaded, restore schema verified `5/5`, cleanup step
  passed, lingering restore DB отсутствует;
- initial `3-7` day storage/archive unattended soak is complete; production
  search/detail cadence was not part of this soak and remains a separate gate;
- bounded S3 backup retention apply passed manually on 2026-06-15:
  `7` verified generations, `1393` remote objects and `28` local sidecars were
  deleted after dry-run; follow-up dry-run returned `delete_candidate_count=0`;
- weekly S3 backup cleanup driver/timer is code-ready: default mode is dry-run,
  destructive apply requires explicit `HHRU_BACKUP_OFFSITE_CLEANUP_APPLY=true`
  and the systemd unit requires a fresh successful weekly restore drill marker;
- first guarded weekly S3 backup cleanup timer run succeeded on 2026-06-21 after
  the weekly restore drill marker: `5` verified generations, `995` remote
  objects and `20` local sidecars were deleted; old unverified `20260517`
  remained fail-safe skipped;
- общий systemd failure notifier прошёл non-blocking local acceptance smoke;
  direct и systemd-template вызовы завершились за `22-44 ms`;
- автоматический destructive research housekeeping отсутствует намеренно.

Production collection calendar не входит в этот storage schedule. Текущий
hourly `scheduler-loop` нельзя включать как production policy без отдельного
решения по search/detail cadence.

## 2. Безопасное Расписание

| Unit | Schedule | Purpose |
| --- | --- | --- |
| `hhru-daily-backup.timer` | daily `00:30 UTC` + up to `15m` jitter | create, local verify, S3 sync and exact remote verify |
| `hhru-research-archive.timer` | daily `02:30 UTC` + up to `15m` jitter | settled export, local/S3 verify, coverage audit and read-only preview |
| `hhru-weekly-backup-restore-drill.timer` | Sunday `06:00 UTC` + up to `30m` jitter | integrity-check newest offsite-verified backup; full restore only when explicitly enabled |
| `hhru-weekly-backup-offsite-cleanup.timer` | Sunday `08:30 UTC` + up to `30m` jitter | bounded cleanup of verified S3 backup generations after a successful weekly backup drill |

All four drivers use `.state/locks/heavy-ops.lock` and wait up to six hours.
This serializes heavy PostgreSQL, disk and S3 work even after a delayed
`Persistent=true` timer start.

Daily backup local dump retention defaults to one day through
`HHRU_BACKUP_DAILY_LOCAL_RETENTION_DAYS=1`. This keeps the local dump as a short
technical restore artifact rather than long-term research storage. Verification
receipts and manifests remain available for the offsite integrity/restore drill.

## 3. Safety Boundary

Automated drivers are fail-closed:

- each driver has its own lock in addition to the shared heavy-ops lock;
- each step writes a separate log under `.state/logs/`;
- a failed local verification prevents offsite operations;
- daily backup remote-verifies the exact dump created by the same run;
- weekly drill selects only a backup with an adjacent
  `.dump.offsite.verified.json` receipt;
- the default weekly backup drill is disk-light and does not create a temporary
  restore DB;
- full weekly restore mode is opt-in through `HHRU_BACKUP_RESTORE_DRILL_MODE=full`
  and has a disk-space preflight before downloading/restoring data;
- weekly S3 backup cleanup is dry-run by default and invokes
  `cleanup-backup-offsite --apply` only when
  `HHRU_BACKUP_OFFSITE_CLEANUP_APPLY=true`;
- the systemd cleanup unit requires a fresh `success.env` marker from
  `weekly-backup-restore-drill`, so cleanup does not run after a missing or
  stale backup-drill proof;
- the research archive driver invokes destructive
  `apply-research-archive-housekeeping --apply` only when
  `HHRU_RESEARCH_ARCHIVE_DAILY_HOUSEKEEPING_APPLY=true`;
- research archive housekeeping retention can be narrowed only through explicit
  daily-driver overrides, and still reruns complete verified S3 coverage before
  deleting exact ids.

S3 backup retention apply is operationally proven as a manual dry-run-first
procedure. The automation path is now fail-closed: schedule it after successful
weekly backup drill, keep latest `2`, keep weekly `0`, and enable destructive
apply only through `/etc/hhru-platform/backup-offsite-cleanup.env`.

## 4. Failure Delivery

Storage services use:

```text
OnFailure=hhru-ops-failure-notify@%n.service
```

The notifier POSTs a synthetic critical Alertmanager payload to the existing
host-bound `alert-webhook`. The webhook validates and queues the message, returns
immediately, and attempts Telegram delivery asynchronously. Telegram outage must
not block systemd failure handling or create Alertmanager retry floods.

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
command must return `operation=notify_systemd_failure status=succeeded`. These
results prove local queue acceptance only.

External Telegram delivery is a separate optional gate. If the VPS cannot reach
Telegram directly, configure a narrowly scoped HTTP(S) proxy through
`HHRU_ALERT_TELEGRAM_PROXY_URL` or provide transparent WireGuard/policy routing.
Until external delivery is restored, use Grafana, direct server access and
`alert-webhook` logs as the monitoring path.

VPS smoke on 2026-06-04 proved the local contract: the direct notifier returned
in `22 ms`, the systemd template returned in `44 ms`, and systemd reported
`Result=success`, `ExecMainStatus=0` while Telegram remained unreachable.

## 5. Supervised Rollout Order

Do not enable the backup timers before these steps pass in order:

1. Deploy the new scripts and systemd units.
2. Reinstall the archive service so its shared lock and failure hook become
   active.
3. Run the synthetic local failure-notification acceptance smoke.
4. Run one supervised daily backup systemd unit. Completed successfully on
   2026-06-04 with `Result=success` and `ExecMainStatus=0`.
5. Run one supervised weekly backup restore drill systemd unit. Completed
   successfully on 2026-06-04 with `Result=success`, `ExecMainStatus=0`.
6. Review disk usage, logs and absence of a lingering restore-drill database.
7. Enable the daily backup and weekly restore-drill timers. Completed on
   2026-06-05; no failed units were present after enablement.
8. Observe the first unattended timer runs. Daily backup completed successfully
   on 2026-06-06; weekly restore drill completed successfully on 2026-06-07.

Install or refresh units:

```bash
cd /opt/hh_collector

install -m 0644 deploy/systemd/hhru-ops-failure-notify@.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-research-archive.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-daily-backup.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-daily-backup.timer /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-weekly-backup-restore-drill.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-weekly-backup-restore-drill.timer /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-weekly-backup-offsite-cleanup.service /etc/systemd/system/
install -m 0644 deploy/systemd/hhru-weekly-backup-offsite-cleanup.timer /etc/systemd/system/

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
install -d -m 0755 /etc/hhru-platform
cat >/etc/hhru-platform/backup-offsite-cleanup.env <<'EOF'
HHRU_BACKUP_OFFSITE_CLEANUP_APPLY=true
HHRU_BACKUP_OFFSITE_CLEANUP_KEEP_LATEST=2
HHRU_BACKUP_OFFSITE_CLEANUP_KEEP_WEEKLY=0
EOF

systemctl enable --now hhru-daily-backup.timer
systemctl enable --now hhru-weekly-backup-restore-drill.timer
systemctl enable --now hhru-weekly-backup-offsite-cleanup.timer

systemctl list-timers \
  hhru-daily-backup.timer \
  hhru-research-archive.timer \
  hhru-weekly-backup-restore-drill.timer \
  hhru-weekly-backup-offsite-cleanup.timer \
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
journalctl -u hhru-weekly-backup-offsite-cleanup.service --since '8 days ago' --no-pager
```

Inspect driver logs and storage:

```bash
du -sh .state/backups .state/archive/research-production-v2
df -h /opt/hh_collector
find .state/logs/backup-daily -mindepth 1 -maxdepth 1 -type d | sort | tail -3
find .state/logs/research-archive-daily -mindepth 1 -maxdepth 1 -type d | sort | tail -3
find .state/logs/backup-restore-drill -mindepth 1 -maxdepth 1 -type d | sort | tail -3
find .state/logs/backup-offsite-cleanup -mindepth 1 -maxdepth 1 -type d | sort | tail -3
```

Initial `3-7` day storage/archive soak completed successfully by 2026-06-15.
Bounded S3 backup retention apply was proven manually on 2026-06-15, and the
guarded weekly timer path was proven automatically on 2026-06-21 after a
successful restore drill. Continue with alert-driven monitoring plus a weekly
checklist; before full unattended month mode, decide the separate production
collection calendar and monitor the next weekly cleanup cycle.
