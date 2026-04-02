# Testing Plan

План тестирования и operator checklist между сессиями.

Текущий фокус: надёжность сбора данных, а не аналитический слой.

Статус на 2026-04-03:

- длинный локальный `search-only` baseline уже практически доказан near-complete run'ом;
- planner completeness blocker и локальный memory blocker больше не выглядят текущими stop-факторами;
- следующий operational step теперь не "первый ночной тест", а VPS pilot плюс transport/outage resilience.

## 1. Цель

Подтвердить, что collector:

- стабильно делает длинные unattended запуски;
- не теряет vacancy-level данные при retention raw payload;
- не накапливает скрытый operational debt в coverage, detail repair, backup и housekeeping contour;
- даёт оператору понятный recovery path.

## 2. Лестница тестирования

### Stage 0. Локальные gates на кодовую базу

Должно быть зелёным перед любым длинным запуском:

```bash
./.venv/bin/python -m ruff check .
./.venv/bin/python -m mypy src
./.venv/bin/python -m pytest
```

### Stage 1. Ручной smoke

Цель:

- проверить, что live HH API доступен;
- миграции применяются;
- scheduler/list/detail/reconcile path стартует без очевидных regressions.

Базовые команды:

```bash
make up-observability
make migrate-compose
make compose-health
```

При необходимости один ручной guarded run:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main trigger-run-now --sync-dictionaries yes --detail-limit 5 --detail-refresh-ttl-days 30 --triggered-by manual-smoke
```

### Stage 2. Ночной soak-test

Цель:

- проверить, что система переживает ночь без ручного вмешательства;
- убедиться, что scheduler не stale;
- увидеть честные terminal outcomes run'ов;
- понять, нет ли failed partitions, stuck unresolved branches или неконтролируемого detail backlog.

Это ближайший обязательный шаг.

### Stage 3. Несколько дней unattended

Цель:

- проверить стабильность уже не на одной ночи, а на серии run'ов;
- подтвердить, что planner/resume/detail-repair path работает не только на одном удачном случае;
- проверить объёмы snapshots/raw/backups и operator routine.

Рекомендуемый минимум:

- 2-3 дня;
- ежедневная утренняя проверка dashboard + scheduler logs;
- хотя бы один свежий backup и один успешный restore drill в этом окне.

### Stage 4. VPS pilot

Цель:

- перенести уже проверенный local soak baseline на постоянную машину;
- включить alert delivery, регулярные backup/housekeeping и off-host backup copy.

## 3. Ночной тест 2026-03-21 -> 2026-03-22

Это конкретный план на сегодняшнюю ночь.

### 3.1. Вечерний preflight

```bash
make up-observability
make migrate-compose
make backup
make compose-health
```

Дополнительно полезно:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-housekeeping --triggered-by soak-preflight
docker compose ps
```

Что должно быть верно до старта:

- `postgres`, `redis`, `metrics`, `prometheus`, `grafana` healthy/running;
- backup создан успешно;
- `health-check` не ругается на `HHRU_HH_API_USER_AGENT`;
- retention для `vacancy_snapshot` явно выставлен в `0`.

### 3.2. Запуск ночного soak-test

Рекомендуемые параметры:

- `interval=900s`
- `sync-dictionaries=no`
- `detail-limit=25`
- `detail-refresh-ttl-days=30`
- `run-type=weekly_sweep`
- `triggered-by=soak-test-2026-03-21`

Команда:

```bash
HHRU_HOUSEKEEPING_VACANCY_SNAPSHOT_RETENTION_DAYS=0 \
HHRU_SCHEDULER_INTERVAL_SECONDS=900 \
HHRU_SCHEDULER_SYNC_DICTIONARIES=no \
HHRU_SCHEDULER_DETAIL_LIMIT=25 \
HHRU_SCHEDULER_DETAIL_REFRESH_TTL_DAYS=30 \
HHRU_SCHEDULER_RUN_TYPE=weekly_sweep \
HHRU_SCHEDULER_TRIGGERED_BY=soak-test-2026-03-21 \
make soak-test
```

Примечание:

- `make soak-test` запускает контейнеры detached;
- ночью ничего руками дёргать не нужно, если нет явной аварии;
- если захочется посмотреть логи до сна, достаточно:

```bash
docker compose logs --tail=100 scheduler
```

## 4. Что смотреть утром

### 4.1. Grafana: первый экран

Открыть `Scheduler / Recovery Health`.

Главные панели:

- `Scheduler Tick Age`
- `Last Triggered Run Age`
- `Open Failed Partitions`
- `Open Unresolved Partitions`
- `Open Detail Repair Backlog`
- `Resume Unresolved Again In 12h`
- `Housekeeping Last Run Age`
- `Backup Last Success Age`

Как читать:

- `Scheduler Tick Age` должен быть маленьким. Большое значение означает stale scheduler или умерший loop.
- `Last Triggered Run Age` не должен внезапно быть огромным при живом scheduler. Иначе ticks идут, но новые run'ы не стартуют.
- `Open Failed Partitions` в хорошем результате равно `0`.
- `Open Unresolved Partitions` ideally `0`; допустимо `>0`, только если уже понятно, почему нужен targeted `resume-run-v2`.
- `Open Detail Repair Backlog` ideally `0`; небольшой понятный backlog допустим, но это уже follow-up action.
- `Resume Unresolved Again In 12h` > `0` означает, что blind resume уже не помогает и нужно разбирать policy/scope, а не жать retry бесконечно.
- `Backup Last Success Age` должен быть свежим после вечернего preflight backup.

### 4.2. Grafana: второй экран

Открыть `Collector Overview`.

Смотреть:

- `Run Terminal Statuses In Range`
- `Failures In Range`
- coverage block

Интерпретация:

- хорошо, если есть terminal runs со статусом `succeeded`;
- допустимы отдельные `completed_with_detail_errors` или `completed_with_unresolved`, если их причина понятна и repair path работает;
- плохо, если виден `failed` run или явный рост failed coverage debt.

### 4.3. Grafana: третий экран

Открыть `HH API / Ingest Health`.

Смотреть:

- upstream errors;
- latency p95;
- любые spikes, которые коррелируют с проблемными run'ами.

Интерпретация:

- если planner/list/detail path деградировал одновременно с ростом HH API errors или latency, причина может быть внешней, а не в нашей логике.

### 4.4. CLI и логи

Утренний минимум:

```bash
docker compose ps
docker compose logs --tail=200 scheduler
make compose-show-metrics
```

Если нужно больше контекста по конкретному run:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main show-run-coverage --run-id <run_id>
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main show-run-tree --run-id <run_id>
```

## 5. Как интерпретировать утренний результат

### Хороший результат

Можно считать ночь успешной, если:

- scheduler не stale;
- были свежие triggered runs;
- `Open Failed Partitions = 0`;
- `Open Unresolved Partitions = 0` или причина известна и repair path понятен;
- `Open Detail Repair Backlog = 0` или backlog небольшой и понятный;
- backup свежий;
- нет красных alert'ов по scheduler/coverage/repair/backup.

### Допустимый, но не идеальный результат

Ночь можно считать полезной, но не "чистой", если:

- есть `completed_with_unresolved`, но это ограниченный кейс и `resume-run-v2` выглядит разумным;
- есть небольшой detail backlog, но он repairable;
- scheduler жив, failed partitions нет, данные продолжают собираться.

### Плохой результат

Останавливаемся и разбираем причину, если:

- `Open Failed Partitions > 0`;
- `Scheduler Tick Age` stale;
- `Last Triggered Run Age` большой без понятной причины;
- `Run Terminal Statuses In Range` показывает `failed`;
- repeated `completed_with_unresolved` уже виден в recovery panels;
- backup stale или backup contour сломан.

## 6. Что делать при типовых проблемах

### Failed partitions

Не запускать blind full rerun.

Сначала:

- открыть affected run в Grafana;
- посмотреть tree/coverage;
- проверить scheduler logs;
- разбирать planner/list failure root cause.

### Unresolved partitions

Использовать targeted resume:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main resume-run-v2 --run-id <run_id> --detail-limit 25 --detail-refresh-ttl-days 30 --triggered-by morning-resume
```

Если unresolved быстро возвращается, проблема уже не в операторском отсутствии resume, а в split-policy или upstream shape.

### Detail repair backlog

Использовать:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main retry-failed-details --run-id <run_id> --triggered-by morning-detail-repair
```

### Stale scheduler

Проверить:

```bash
docker compose ps
docker compose logs --tail=200 scheduler
```

До ручного trigger важно понять, почему scheduler-loop умер или перестал тикать.

### Stale backup

Повторить backup и verify:

```bash
make backup
make verify-backup BACKUP_FILE=.state/backups/<file>.dump
```

## 7. Минимальный утренний handoff между сессиями

Если утром нужна помощь в новой сессии, полезно сразу принести:

- значение основных stat-панелей из `Scheduler / Recovery Health`;
- terminal statuses run'ов за ночь;
- один `run_id`, который выглядит самым показательным;
- tail логов `scheduler`;
- есть ли `failed_partitions`, `unresolved_partitions`, `detail_repair_backlog`;
- имя свежего backup file, если backup делался ночью/утром.

Этого достаточно, чтобы быстро продолжить разбор без повторного восстановления контекста с нуля.

## Смежные документы

- [current-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-readiness.md)
- [soak-test-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/soak-test-readiness.md)
- [observability.md](/home/yurizinyakov/projects/hh_collector/docs/ops/observability.md)
- [backup-restore-drill.md](/home/yurizinyakov/projects/hh_collector/docs/ops/backup-restore-drill.md)
