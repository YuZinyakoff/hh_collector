# Backup / Restore Drill

Практичный operator runbook для PostgreSQL backup и безопасного restore drill перед VPS pilot.

## 1. Что считается нормой

- backup создаётся через `run-backup` / `make backup`;
- backup сразу проверяется как restorable archive;
- restore drill всегда идёт в отдельную target DB;
- offsite copy проверяется не только фактом upload, но и remote manifest/parts;
- хотя бы периодически делается restore drill именно из offsite copy;
- live destructive `restore` остаётся low-level аварийным инструментом, а не default path.

Важно: PostgreSQL backup contour не является research archive. Для разделения
storage-контуров см. [storage-contours.md](/home/yurizinyakov/projects/hh_collector/docs/ops/storage-contours.md).

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

Restore-drill DB занимает почти столько же места, сколько live DB на момент backup-а.
После успешного drill её можно удалить, если она больше не нужна для расследования:

```bash
docker compose exec postgres dropdb -U ${HHRU_DB_USER:-hhru} --if-exists ${HHRU_BACKUP_RESTORE_DRILL_TARGET_DB:-hhru_platform_restore_drill}
```

VPS observation 2026-05-21: live DB `8930 MB`, restore-drill DB `7458 MB`.

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

## 6. Синхронизировать backup offsite

Retention archive offsite sync не загружает `.state/backups/*.dump`. Для PostgreSQL
dump-ов используется отдельная команда:

```bash
make backup-offsite
```

По умолчанию команда берёт только свежий `.dump` из `HHRU_BACKUP_DIR`.
Для нескольких последних файлов:

```bash
make backup-offsite ARGS="--limit 3 --triggered-by manual-backup-offsite"
```

CLI-эквивалент:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main sync-backup-offsite --limit 1 --triggered-by cli-backup-offsite
```

Offsite settings:

- `HHRU_BACKUP_OFFSITE_BACKEND` (`webdav` или `s3`; `webdav` по умолчанию)
- `HHRU_BACKUP_OFFSITE_URL`
- `HHRU_BACKUP_OFFSITE_ROOT`
- `HHRU_BACKUP_OFFSITE_USERNAME`
- `HHRU_BACKUP_OFFSITE_PASSWORD`
- `HHRU_BACKUP_OFFSITE_BEARER_TOKEN`
- `HHRU_BACKUP_OFFSITE_TIMEOUT_SECONDS` (`1800` по умолчанию, потому DB dump может быть гигабайтным)
- `HHRU_BACKUP_OFFSITE_CHUNK_SIZE_BYTES` (`4194304` по умолчанию)

Для S3-compatible storage:

- `HHRU_BACKUP_OFFSITE_BACKEND=s3`
- `HHRU_BACKUP_OFFSITE_S3_ENDPOINT_URL=https://s3.twcstorage.ru`
- `HHRU_BACKUP_OFFSITE_S3_BUCKET=<bucket-name>`
- `HHRU_BACKUP_OFFSITE_S3_REGION=ru-1`
- `HHRU_BACKUP_OFFSITE_S3_ACCESS_KEY_ID=<access-key>`
- `HHRU_BACKUP_OFFSITE_S3_SECRET_ACCESS_KEY=<secret-key>`
- `HHRU_BACKUP_OFFSITE_ROOT=/hhru-platform/backups`
- `HHRU_BACKUP_OFFSITE_CHUNK_SIZE_BYTES=67108864` для S3-пилота, чтобы не плодить
  сотни мелких objects на один multi-GB dump.

Если `HHRU_BACKUP_OFFSITE_URL` и credentials не заданы, команда использует уже настроенный
`HHRU_HOUSEKEEPING_ARCHIVE_OFFSITE_*` WebDAV contour, но кладёт backup-и под
`HHRU_BACKUP_OFFSITE_ROOT` (`/hhru-platform/backups` по умолчанию).

Dump загружается не одним большим request, а fixed-size частями:

- части лежат в remote directory `<dump>.parts/000001.part`, `<dump>.parts/000002.part`, ...
- `.manifest.json` содержит `backup_sha256`, `chunk_size_bytes`, список частей, размер и sha256 каждой части;
- `.offsite.parts.json` фиксирует уже загруженные части, чтобы повторный запуск мог продолжить upload после обрыва;
- для recovery надо скачать manifest и все parts, затем склеить parts по порядку и проверить итоговый `backup_sha256`;
- повторный запуск пропускает backup только если локальный `.offsite.json` receipt совпадает с dump, manifest, remote path, `chunk_size_bytes` и количеством частей.

Ожидаемо:

- `status=succeeded`
- `scanned_backup_count=1`
- `uploaded_backup_count=1` при первом upload или `skipped_backup_count=1` при повторном запуске
- в summary есть `part_count > 0`
- рядом с dump появляется `.manifest.json`
- рядом с dump появляется `.offsite.json` receipt

Проверено на VPS 2026-05-23:

- backend: Timeweb cold S3, `https://s3.twcstorage.ru`;
- dump: `2269000643` bytes;
- chunk size: `67108864`;
- parts: `34`;
- upload duration: about `82s`;
- повторный запуск: `uploaded_backup_count=0`, `skipped_backup_count=1`.

Upload+receipt path доказывает transport и idempotency. Для production-grade
backup contour также нужны:

- offsite restore drill из S3 copy;
- explicit local/offsite backup retention policy.

## 7. Проверить offsite copy

После `make backup-offsite` нужно проверить, что remote manifest и все remote parts
реально существуют и имеют размеры из manifest:

```bash
make verify-backup-offsite
```

Для конкретного dump-а:

```bash
make verify-backup-offsite BACKUP_FILE=.state/backups/<file>.dump
```

Ожидаемо:

- `verified backup offsite`
- `status=succeeded`
- `verified_object_count = part_count + 1`
- `backup_sha256` совпадает с local `verify-backup-file`

Текущий verify проверяет remote object sizes. Полная readback-проверка делается через
offsite restore drill: скачать remote manifest и parts, склеить dump, посчитать
`backup_sha256`, затем восстановить в отдельную DB.

## 8. Выполнить offsite restore drill

Offsite restore drill проверяет, что S3 copy действительно пригодна для восстановления,
а не только присутствует по размерам:

```bash
make backup-offsite-restore-drill
```

Для конкретного dump-а:

```bash
make backup-offsite-restore-drill BACKUP_FILE=.state/backups/<file>.dump
```

Ожидаемо:

- `completed backup offsite restore drill`
- `status=succeeded`
- `downloaded_part_count = part_count`
- `schema_verified=yes`
- `verified_tables=5/5`

Команда не читает локальный `.dump` как источник данных. Она использует соседний
`.manifest.json` как inventory, скачивает remote manifest и parts из S3, собирает
temporary dump, проверяет итоговый `backup_sha256` и запускает обычный restore drill
в отдельную target DB.

Порядок перед risky/long-running работами:

```bash
make backup
BACKUP_FILE="$(ls -1t .state/backups/*.dump | head -n 1)"
make verify-backup BACKUP_FILE="$BACKUP_FILE"
make restore-drill BACKUP_FILE="$BACKUP_FILE"
make backup-offsite
make verify-backup-offsite BACKUP_FILE="$BACKUP_FILE"
make backup-offsite-restore-drill BACKUP_FILE="$BACKUP_FILE"
```

## 9. Когда использовать low-level restore

Legacy destructive path остаётся только как аварийный инструмент:

```bash
make restore BACKUP_FILE=/backups/<file>.dump
```

Его использовать только после того, как:

- backup уже проверен;
- restore drill в отдельную DB уже прошёл;
- offsite copy свежего dump-а загружена или сознательно признана недоступной;
- понятна причина live recovery.

## 10. Metrics и dashboard signals

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
