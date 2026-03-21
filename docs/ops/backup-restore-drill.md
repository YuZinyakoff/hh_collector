# Backup / Restore Drill

Практичный operator runbook для PostgreSQL backup и безопасного restore drill перед VPS pilot.

## 1. Что считается нормой

- backup создаётся через `run-backup` / `make backup`;
- backup сразу проверяется как restorable archive;
- restore drill всегда идёт в отдельную target DB;
- live destructive `restore` остаётся low-level аварийным инструментом, а не default path.

## 2. Создать backup

Локально через Compose:

```bash
make backup
```

Или напрямую через CLI:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-backup --triggered-by cli-backup
```

Ожидаемо:

- `status=succeeded`
- есть `backup_file`
- есть `backup_size_bytes`
- есть `backup_sha256`
- есть `archive_entry_count > 0`

## 3. Проверить backup file

Повторная operator-проверка конкретного dump:

```bash
make verify-backup BACKUP_FILE=.state/backups/<file>.dump
```

Или напрямую:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main verify-backup-file --backup-file .state/backups/<file>.dump
```

Ожидаемо:

- `verified backup file`
- тот же `backup_sha256`
- `archive_entry_count > 0`

Если verify падает, этот archive нельзя считать drill-ready.

## 4. Выполнить restore drill

Рекомендуемый безопасный путь:

```bash
make restore-drill BACKUP_FILE=.state/backups/<file>.dump
```

При необходимости в отдельную custom DB:

```bash
make restore-drill BACKUP_FILE=.state/backups/<file>.dump TARGET_DB=hhru_platform_restore_drill_candidate
```

CLI-эквивалент:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-restore-drill --backup-file .state/backups/<file>.dump --target-db hhru_platform_restore_drill --triggered-by cli-restore-drill
```

Restore drill:

- сначала проверяет archive через `pg_restore --list`;
- затем пересоздаёт target DB, если включён `HHRU_BACKUP_RESTORE_DRILL_DROP_EXISTING=true`;
- делает restore только в target DB;
- проверяет наличие core tables:
  - `crawl_run`
  - `crawl_partition`
  - `raw_api_payload`
  - `vacancy_snapshot`
  - `vacancy_current_state`

Ожидаемо:

- `status=succeeded`
- `target_db=<restore_db>`
- `schema_verified=yes`
- `verified_tables=5/5`

## 5. Проверить, что restore drill жив

После успешного drill достаточно:

```bash
docker compose exec postgres psql -U ${HHRU_DB_USER:-hhru} -d ${HHRU_BACKUP_RESTORE_DRILL_TARGET_DB:-hhru_platform_restore_drill} -c '\dt'
```

Или посмотреть summary `run-restore-drill`, где уже есть `schema_verified=yes`.

Если нужен дополнительный sanity check:

```bash
docker compose exec postgres psql -U ${HHRU_DB_USER:-hhru} -d ${HHRU_BACKUP_RESTORE_DRILL_TARGET_DB:-hhru_platform_restore_drill} -c 'select count(*) from crawl_run;'
```

Нулевой count допустим. Важно, что schema поднялась и таблицы читаются.

## 6. Когда использовать low-level restore

Legacy destructive path остаётся только как аварийный инструмент:

```bash
make restore BACKUP_FILE=/backups/<file>.dump
```

Его использовать только после того, как:

- backup уже проверен;
- restore drill в отдельную DB уже прошёл;
- понятна причина live recovery.

## 7. Metrics и dashboard signals

Оператору важны:

- `hhru_backup_run_total{status}`
- `hhru_backup_last_success_timestamp_seconds`
- `hhru_restore_drill_run_total{status}`
- `hhru_restore_drill_last_success_timestamp_seconds`
- recording rules `hhru:backup_last_success_age_seconds` и `hhru:restore_drill_last_success_age_seconds`

Главный экран:

- `Scheduler / Recovery Health`
  - `Backup Last Success Age`
  - `Backup Runs In Range`
  - `Restore Drill Last Success Age`
  - `Restore Drill Runs In Range`

Если `Backup Last Success Age` уходит в warning/red, backup contour уже не считается свежим для pilot baseline.
