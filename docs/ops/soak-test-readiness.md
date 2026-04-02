# Soak-Test Readiness

Короткий runbook для ночного локального прогона перед VPS pilot.

Статус на 2026-04-03:

- local soak-grade baseline contour уже подтверждён near-complete long run;
- документ остаётся полезным как operator checklist, но следующий практический шаг уже смещается в VPS pilot.

Смежные документы:

- [current-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-readiness.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)

## 1. Цель

Проверить, что система выдерживает длинный unattended запуск без:

- stale scheduler;
- накопления failed partitions;
- бесконтрольного роста unresolved branches;
- нечинящегося detail repair backlog;
- протухшего backup/housekeeping contour.

## 2. Рекомендуемый preflight вечером

1. Поднять infra и observability:

```bash
make up-observability
make migrate-compose
```

2. Сделать свежий backup:

```bash
make backup
```

3. При желании посмотреть housekeeping preview:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-housekeeping --triggered-by soak-preflight
```

4. Убедиться, что `health-check` выглядит ожидаемо:

```bash
make compose-health
```

## 3. Рекомендуемые параметры на ночь

Практичный baseline для локального soak-test:

- `HHRU_SCHEDULER_INTERVAL_SECONDS=900`
- `HHRU_SCHEDULER_SYNC_DICTIONARIES=no`
- `HHRU_SCHEDULER_DETAIL_LIMIT=25`
- `HHRU_SCHEDULER_DETAIL_REFRESH_TTL_DAYS=30`
- `HHRU_SCHEDULER_RUN_TYPE=weekly_sweep`
- `HHRU_SCHEDULER_TRIGGERED_BY=soak-test`

Запуск:

```bash
HHRU_SCHEDULER_INTERVAL_SECONDS=900 \
HHRU_SCHEDULER_SYNC_DICTIONARIES=no \
HHRU_SCHEDULER_DETAIL_LIMIT=25 \
HHRU_SCHEDULER_DETAIL_REFRESH_TTL_DAYS=30 \
HHRU_SCHEDULER_TRIGGERED_BY=soak-test \
make soak-test
```

`make soak-test` поднимает `postgres`, `redis`, `metrics`, `prometheus`, `grafana` и `scheduler`.

## 4. Что смотреть утром

Сначала открыть `Scheduler / Recovery Health`.

Главные панели:

- `Scheduler Tick Age`
- `Last Triggered Run Age`
- `Open Failed Partitions`
- `Open Unresolved Partitions`
- `Open Detail Repair Backlog`
- `Resume Unresolved Again In 12h`
- `Housekeeping Last Run Age`
- `Backup Last Success Age`

Потом проверить:

- `Collector Overview`
  - `Run Terminal Statuses In Range`
  - `Failures In Range`
  - coverage block
- `HH API / Ingest Health`
  - upstream errors
  - latency p95

CLI checks:

```bash
make compose-show-metrics
docker compose ps
docker compose logs --tail=100 scheduler
```

## 5. Как интерпретировать утренний результат

Хороший soak outcome:

- scheduler не stale;
- были свежие triggered runs;
- `Open Failed Partitions = 0`;
- `Open Unresolved Partitions = 0` или известно, почему нужен targeted `resume-run-v2`;
- `Open Detail Repair Backlog = 0` или backlog небольшой и понятен;
- backup свежий;
- нет красных alert'ов по scheduler/coverage/repair/backup.

## 6. Что делать при проблемах

- `Open Failed Partitions > 0`
  Разбирать конкретный run и planner/list failure. Не запускать blind full rerun.
- `Open Unresolved Partitions > 0`
  Делать `resume-run-v2 --run-id <run_id>` только для конкретного run.
- `Open Detail Repair Backlog > 0`
  Делать `retry-failed-details --run-id <run_id>`.
- `Scheduler Tick Age` stale
  Проверять `scheduler` container и structured logs до любых ручных trigger.
- `Backup Last Success Age` stale
  Сначала чинить backup contour и только потом считать soak baseline pilot-ready.

## 7. Когда считать систему готовой к VPS pilot

- хотя бы один ночной soak-test прошёл без critical alerts;
- есть свежий backup;
- restore drill был успешно выполнен в отдельную DB;
- scheduler утром жив и без open failed coverage debt;
- operator path для unresolved/detail backlog понятен и реально работает.
