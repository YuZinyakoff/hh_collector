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

## Notes

- `process-list-page` и `fetch-vacancy-detail` используют официальный live hh API, поэтому конкретные `hh_vacancy_id`, счётчики и тексты вакансий будут меняться.
- Для воспроизводимой локальной smoke-проверки достаточно пройти шаги выше по порядку и убедиться, что каждый следующий шаг получает ID из вывода предыдущего.
