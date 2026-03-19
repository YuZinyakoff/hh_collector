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

4. Обработать одну list page:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main process-list-page --partition-id <partition_id>
```

Ожидаемо:
- `status=done`
- `vacancies_processed>0`
- `seen_events_created>0`
- есть строки вида `vacancy=<vacancy_id> hh_vacancy_id=<hh_vacancy_id>`

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
- после live flow появляются метрики по `sync_dictionary`, `process_list_page`, `fetch_vacancy_detail`, `reconcile_run`

## Notes

- `process-list-page` и `fetch-vacancy-detail` используют официальный live hh API, поэтому конкретные `hh_vacancy_id`, счётчики и тексты вакансий будут меняться.
- Для воспроизводимой локальной smoke-проверки достаточно пройти шаги выше по порядку и убедиться, что каждый следующий шаг получает ID из вывода предыдущего.
