# Manual Happy Path

Короткий runbook для ручной проверки текущего MVP happy path.

## Preconditions

- Docker Postgres запущен.
- Миграции применены в ту же БД, куда будут ходить CLI-команды.
- `PYTHONPATH=src` выставлен для запуска CLI через `python -m`.

Если локальный volume был создан до выравнивания schema/alembic/ORM, удобнее начать с чистой БД:

```bash
docker compose down -v
docker compose up -d postgres
PYTHON=./.venv/bin/python make migrate
```

Альтернатива без удаления volume: использовать отдельную временную БД через `HHRU_DB_NAME=<temp_db>`.

## Orchestration-Lite Shortcut

Если нужен один быстрый сквозной smoke flow без ручного запуска каждого slice, можно использовать `run-once`:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-once --sync-dictionaries yes --pages-per-partition 1 --detail-limit 1 --triggered-by cli-happy-path
```

Ожидаемо:
- `status=succeeded`
- есть `run_id`
- `partitions_processed>=1`
- `list_pages_processed>=1`
- `vacancies_found>=0`
- `detail_fetch_attempted=1` или меньше, если найдено меньше вакансий
- `reconciliation_status=completed`

Эта команда использует те же самые existing slices, что и пошаговый manual flow ниже. Если нужно точечно диагностировать отдельный этап, используй ручные команды из следующего раздела.

Если `HHRU_HH_API_USER_AGENT` оставлен placeholder-значением вроде `hhru-platform/0.1` или `change-me@example.com`, live `run-once` теперь не маскирует это как happy path:

- `process-list-page` помечает partition как `failed`;
- `run-once` завершится с `status=failed`, `failed_step=process_list_page` и кодом выхода `1`;
- detail fetch и reconciliation будут пропущены и отражены в `skipped_steps`.

## Tree-Aware Shortcut

Если нужен уже не legacy smoke flow, а цельный planner-v2/list-engine-v2 проход по tree semantics, используй `run-once-v2`:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-once-v2 --sync-dictionaries yes --detail-limit 25 --detail-refresh-ttl-days 30 --triggered-by cli-happy-path-v2
```

Ожидаемо:
- `status=succeeded`, если `coverage_ratio=1.0000`, `pending_terminal_partitions=0`, `unresolved_partitions=0`, `failed_partitions=0`
- `status=completed_with_detail_errors`, если list coverage завершён полностью, но часть selective detail fetch завершилась ошибкой
- `status=completed_with_unresolved`, если tree дошёл до terminal unresolved scopes и полный coverage не достигнут
- `status=failed`, если list stage дал failed partitions или не удалось выполнить один из критичных orchestration шагов
- `list_stage_status=completed` только после полного tree coverage
- `detail_stage_status` становится `completed` или `completed_with_failures` только после успешного list coverage stage
- `reconciliation_status` становится `succeeded` или `completed_with_detail_errors` только после успешного list coverage stage

`run-once-v2` отличается от legacy `run-once` тем, что не ограничивается одной smoke partition и не считает run успешным просто по факту нескольких обработанных страниц. Итоговый статус здесь честно привязан к tree coverage outcome.

## Resume Unresolved Run

Если `run-once-v2` завершился с `status=completed_with_unresolved`, операторский путь продолжения теперь такой:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main resume-run-v2 --run-id <run_id> --detail-limit 25 --detail-refresh-ttl-days 30 --triggered-by cli-resume
```

Что делает команда:

- читает текущий coverage summary этого же `crawl_run`;
- считает `unresolved_before_resume` и `pending_before_resume`;
- переводит `unresolved` terminal branches обратно в `pending`;
- снова запускает existing `run-list-engine-v2`;
- если tree теперь покрыт полностью, продолжает existing selective detail + `reconcile-run` в том же `crawl_run`.

Ожидаемо:

- `status=succeeded`, если после resume tree полностью покрыт и detail stage завершён без ошибок;
- `status=completed_with_detail_errors`, если coverage закрыт, но после resume часть detail fetch по-прежнему упала;
- `status=completed_with_unresolved`, если unresolved scopes остались и после resume;
- summary печатает `initial_run_status`, `unresolved_before_resume`, `resumed_unresolved_partitions`, `covered_terminal_partitions` и финальный `coverage_ratio`.

Интерпретация:

- `completed_with_unresolved` больше не означает, что run нужно бросить или пересоздать;
- новый `crawl_run` не создаётся;
- resume path не меняет базовую split/detail policy, он только повторно использует существующий tree/list/detail orchestration поверх того же run.

## Repair Detail Backlog

Если `run-once-v2` завершился с `status=completed_with_detail_errors`, list coverage уже считается завершённым. Чинить нужно не tree, а derived detail repair backlog:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main retry-failed-details --run-id <run_id> --triggered-by cli-detail-repair
```

Что считается backlog:

- latest failed `detail_fetch_attempt` per vacancy внутри этого `crawl_run`.

Что делает команда:

- находит backlog этого run;
- повторяет `fetch-vacancy-detail` только по backlog items;
- не трогает list coverage tree;
- после retry пересчитывает backlog ещё раз и обновляет status run.

Ожидаемо:

- `status=succeeded`, если backlog опустел;
- `status=completed_with_detail_errors`, если часть backlog всё ещё падает;
- summary печатает `backlog_size`, `retried_count`, `repaired_count`, `still_failing_count`, `remaining_backlog_count`.

Интерпретация:

- `repaired` означает, что latest detail attempt для backlog vacancy стал `succeeded`;
- `still_failing` означает, что latest attempt после retry остаётся `failed`;
- если backlog очищен, run может быть promoted из `completed_with_detail_errors` в `succeeded`.

## Unattended Shortcut

Для одного guarded запуска поверх scheduler admission control используй `trigger-run-now`:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main trigger-run-now --sync-dictionaries yes --detail-limit 25 --detail-refresh-ttl-days 30 --triggered-by trigger-run-now
```

Ожидаемо:
- `status=skipped_overlap`, если advisory lock уже удерживается другим scheduler/run
- `status=skipped_active_run`, если в БД уже есть active `crawl_run` со статусом `created`
- иначе будет напечатан полный nested summary `run-once-v2`

Для unattended execution foundation используй `scheduler-loop`:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main scheduler-loop --interval-seconds 300 --sync-dictionaries yes --detail-limit 25 --detail-refresh-ttl-days 30 --triggered-by scheduler-loop
```

Ожидаемо:
- loop на каждом tick пытается стартовать новый guarded `run-once-v2`
- overlapping runs не допускаются через PostgreSQL advisory lock плюс active-run check
- summary печатает `ticks_executed`, `runs_started`, `skipped_overlap_ticks`, `skipped_active_run_ticks` и разбивку по terminal run statuses

Если цель уже не просто "запустить scheduler", а держать его в контуре подтверждённой HH policy, ориентиром для operator settings теперь служит:

- [hh-api-scheduler-policy-v1-draft.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-scheduler-policy-v1-draft.md)

## Housekeeping Dry-Run

Для безопасной проверки retention plan сначала запусти dry-run:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-housekeeping --triggered-by cli-housekeeping-dry-run
```

Ожидаемо:
- `status=succeeded`
- `mode=dry_run`
- `total_candidates>=0`
- `total_deleted=0`
- есть per-target строки вида `target=raw_api_payload ... candidate_count=... action_count=... limited=yes|no`

Dry-run использует retention policy из runtime settings и ничего не удаляет. Это основной operator preview перед реальным cleanup.

## Housekeeping Execute

Если dry-run summary выглядит корректно, выполнить real cleanup можно так:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-housekeeping --execute --triggered-by cli-housekeeping
```

Если для `raw_api_payload` и `vacancy_snapshot` нужен безопасный hot-to-cold path, используй archive-before-delete:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-housekeeping --execute --archive-before-delete --triggered-by cli-housekeeping
```

Ожидаемо:
- `status=succeeded`
- `mode=execute`
- `total_deleted>=0`
- `total_archived>=0`
- per-target summary показывает фактически удалённые rows/files через `deleted_count`
- для `raw_api_payload` и `vacancy_snapshot` summary дополнительно печатает `archived_count`, `archive_file`, `manifest_file`, `archive_sha256`

Если нужна только выгрузка retention-кандидатов без удаления, используй отдельный export path:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main export-retention-archive --triggered-by cli-retention-archive
```

Ожидаемо:
- `status=succeeded`
- `total_exported>=0`
- per-target summary печатает `archive_file`, `manifest_file`, `archive_sha256`, `archive_size_bytes`

Что делает housekeeping:

- удаляет только старые `raw_api_payload`, которые старше TTL, не относятся к active run и не защищены snapshot references;
- удаляет старые `vacancy_snapshot`, но сохраняет latest snapshot на каждую vacancy;
- удаляет старые `detail_fetch_attempt`, но сохраняет latest attempt на `(vacancy_id, crawl_run_id)`;
- удаляет старые terminal `crawl_run` и их `crawl_partition` tree state только по `finished_at`;
- удаляет старые локальные artifacts из `.state/reports/detail-payload-study`;
- не трогает `vacancy_current_state`, справочники и актуальные `vacancy` rows;
- не трогает `crawl_run` со статусом `created` и связанные с ними live artifacts.
- при `--archive-before-delete` сначала пишет сжатые локальные archive chunks в `HHRU_HOUSEKEEPING_ARCHIVE_DIR`, и только потом удаляет `raw_api_payload`/`vacancy_snapshot`.

## Backup / Restore Drill

Перед длинным unattended run или любым опасным ops-действием baseline теперь такой:

```bash
make backup
make verify-backup BACKUP_FILE=.state/backups/<file>.dump
make restore-drill BACKUP_FILE=.state/backups/<file>.dump
```

Подробный runbook:

- [backup-restore-drill.md](/home/yurizinyakov/projects/hh_collector/docs/ops/backup-restore-drill.md)

Ожидаемо:

- backup summary печатает `backup_sha256` и `archive_entry_count`
- restore drill идёт только в separate target DB
- restore drill завершает проверку с `schema_verified=yes`

Legacy `make restore` остаётся аварийным destructive path и больше не считается default operator flow.

## Night Soak-Test

Для ночного локального прогона используй:

```bash
HHRU_SCHEDULER_INTERVAL_SECONDS=900 \
HHRU_SCHEDULER_SYNC_DICTIONARIES=no \
HHRU_SCHEDULER_DETAIL_LIMIT=25 \
HHRU_SCHEDULER_TRIGGERED_BY=soak-test \
make soak-test
```

Подробный checklist:

- [soak-test-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/soak-test-readiness.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
- [current-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-readiness.md)

Утром первым экраном должен быть `Scheduler / Recovery Health`.

Отдельно от system soak-test есть research-only ночной HH API driver:

- [hh-api-probe-night-driver.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-probe-night-driver.md)

Его нужно использовать, когда цель не проверить scheduler/platform, а проверить HH collection policy на живом API по time-distributed slots.

## Happy Path

1. Синхронизировать `areas`:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main sync-dictionaries --name areas
```

Ожидаемо:
- `status=succeeded`
- `source_status_code=200`
- есть `sync_run_id`, `request_log_id`, `raw_payload_id`

2. Создать `crawl_run`:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main create-run --run-type weekly_sweep --triggered-by cli
```

Ожидаемо:
- `created crawl_run`
- есть `id=<run_id>`
- `status=created`

3. Создать partition для run:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main plan-run --run-id <run_id>
```

Ожидаемо:
- `planned crawl partitions`
- `partitions_created=1`
- есть `partition=<partition_id> key=global-default status=pending`

Если нужен именно planner v2 foundation вместо legacy smoke partition, используй отдельную команду:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main plan-run-v2 --run-id <run_id>
```

Ожидаемо:
- создаются disjoint root partitions вида `key=area:<hh_area_id>`
- у root partitions `depth=0`, `parent=-`, `status=pending`
- это уже tree-based foundation для exhaustive list coverage, а не single global partition

4. Обработать одну list page:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main process-list-page --partition-id <partition_id>
```

Ожидаемо:
- `status=done`
- `vacancies_processed>0`
- `seen_events_created>0`
- есть строки вида `vacancy=<vacancy_id> hh_vacancy_id=<hh_vacancy_id>`

Для planner v2 / exhaustive tree path вместо single-page smoke шага используй:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main process-partition-v2 --partition-id <partition_id>
```

Ожидаемо для несатурированного leaf:
- `partition_final_status=done`
- `coverage_status=covered`
- `pages_processed>=1`
- `saturated=no`

Если root scope слишком широкий, используй полный engine по run:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-list-engine-v2 --run-id <run_id>
```

Ожидаемо:
- engine проходит по `pending` terminal partitions текущего tree;
- saturated parent получает `partition_final_status=split_done` и `coverage_status=split`;
- children создаются автоматически и затем обрабатываются как новые terminal leaves;
- `remaining_pending_terminal_partitions=0` означает, что на текущем tree нет необработанных leaf scopes.

5. Запросить detail по одной сохранённой вакансии:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main fetch-vacancy-detail --vacancy-id <vacancy_id>
```

Ожидаемо:
- `detail_fetch_status=succeeded`
- есть `snapshot_id`
- есть `request_log_id`, `raw_payload_id`, `detail_fetch_attempt_id`

6. Выполнить reconciliation для завершения run:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main reconcile-run --run-id <run_id>
```

Ожидаемо:
- `reconciled crawl run`
- `vacancies_observed_in_run>=0`
- есть `missing_updated`, `marked_inactive`
- `status=completed`

7. Проверить accumulated metrics:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main show-metrics
```

Ожидаемо:
- есть `hhru_operation_total`
- есть `hhru_records_written_total`
- после live flow появляются метрики по `sync_dictionary`, `process_list_page`, `process_partition_v2`, `run_list_engine_v2`, `fetch_vacancy_detail`, `reconcile_run`
- `run-once-v2` дополнительно пишет operation metric `run_collection_once_v2`
- scheduler admission path дополнительно пишет `hhru_scheduler_tick_total` и timestamps последних tick/run

## Notes

- `process-list-page` и `fetch-vacancy-detail` используют официальный live hh API, поэтому конкретные `hh_vacancy_id`, счётчики и тексты вакансий будут меняться.
- Для воспроизводимой локальной проверки legacy flow достаточно пройти шаги выше по порядку и убедиться, что каждый следующий шаг получает ID из вывода предыдущего.
- Для planner v2 path типичный операторский сценарий такой: `sync-dictionaries --name areas` -> `create-run` -> `plan-run-v2` -> `run-list-engine-v2` -> `fetch-vacancy-detail` при необходимости -> `reconcile-run`.
- `run-once-v2` упаковывает этот planner-v2 path в один операторский entrypoint: `create-run` -> `plan-run-v2` -> tree-aware `run-list-engine-v2` loop -> selective detail -> `reconcile-run`.
- `resume-run-v2` продолжает уже существующий problematic tree-aware run, а не создаёт новый.
- `retry-failed-details` работает только с derived detail repair backlog и не переоткрывает list coverage stage.
- `trigger-run-now` и `scheduler-loop` не дублируют planner/list/detail/reconcile logic: они только координируют уже существующий `run-once-v2`.
