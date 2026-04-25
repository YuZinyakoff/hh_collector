# Deployment Runbook

Практичный baseline для развёртывания текущего MVP collector на одной VPS через Docker Compose.

Для первого supervised VPS baseline используй более конкретный чеклист:

- [vps-pilot-checklist.md](/home/yurizinyakov/projects/hh_collector/docs/ops/vps-pilot-checklist.md)

## 1. Подготовка VPS

- Установить Docker Engine и Compose plugin.
- Клонировать репозиторий на VPS.
- Держать внешние bind-порты на `127.0.0.1`, если доступ планируется только через SSH tunnel или reverse proxy.

## 2. Подготовка окружения

Скопировать шаблон:

```bash
cp .env.example .env
```

`.env` теперь может содержать одновременно app runtime settings и compose-only переменные для bind/UI. Python CLI игнорирует лишние compose-only ключи и использует только свои объявленные `HHRU_*` поля.

Минимально проверить и изменить:

- `HHRU_ENV=production`
- `HHRU_DB_PASSWORD`
- `HHRU_HH_API_USER_AGENT`
- `HHRU_GRAFANA_ADMIN_PASSWORD`
- `HHRU_DB_BIND_HOST`, `HHRU_REDIS_BIND_HOST`, `HHRU_METRICS_BIND_HOST`, `HHRU_PROMETHEUS_BIND_HOST`, `HHRU_GRAFANA_BIND_HOST`

Требование к `HHRU_HH_API_USER_AGENT`:

- для live vacancy search нужен реальный `User-Agent` с рабочим contact value;
- placeholder-значения вроде `hhru-platform/0.1`, `change-me@example.com`, `your_email@example.com` теперь блокируются до похода в live `/vacancies`;
- быстрый preflight можно посмотреть через `health-check`, поле `hh_api_user_agent_live_search_valid`.

## 3. Запуск сервисов

Поднять базовый long-running набор:

```bash
make up
```

Если нужен Prometheus и Grafana:

```bash
make up-observability
```

Это поднимает:

- `postgres`
- `redis`
- `metrics`
- `prometheus` только при `up-observability`
- `grafana` только при `up-observability`

Операционные CLI-команды запускаются on-demand через `docker compose run --rm`.

## 4. Миграции

Применить Alembic migrations внутри app image:

```bash
make migrate-compose
```

## 5. Health и metrics

Проверить runtime config из app container:

```bash
make compose-health
```

Проверить metrics endpoint:

```bash
curl http://127.0.0.1:8001/healthz
curl http://127.0.0.1:8001/metrics
```

Если поднят Prometheus:

```bash
curl http://127.0.0.1:9090/-/ready
```

Если поднят observability profile, открыть:

- Grafana: `http://127.0.0.1:3000`
- Prometheus: `http://127.0.0.1:9090`

Dashboards provisioned автоматически из репозитория:

- `Scheduler / Recovery Health`
- `Collector Overview`
- `HH API / Ingest Health`

## 6. Операторские CLI-команды

Самый короткий run-once сценарий:

```bash
docker compose --profile ops run --rm app run-once --sync-dictionaries yes --pages-per-partition 1 --detail-limit 5 --triggered-by vps-manual
```

`run-once` — это orchestration-lite shortcut поверх существующих MVP slices. Он не заменяет scheduler и не создаёт очередь.

Новая семантика `run-once` для critical step:

- если `process-list-page` завершился с ошибкой, `run-once` завершится с `status=failed` и кодом выхода `1`;
- итоговый summary покажет `failed_step`, `completed_steps`, `skipped_steps`;
- шаги после критичного сбоя, включая detail fetch и reconciliation, не запускаются.

Примеры текущего manual flow в контейнере:

```bash
docker compose --profile ops run --rm app sync-dictionaries --name areas
docker compose --profile ops run --rm app create-run --run-type weekly_sweep --triggered-by vps-manual
docker compose --profile ops run --rm app plan-run --run-id <run_id>
docker compose --profile ops run --rm app process-list-page --partition-id <partition_id>
docker compose --profile ops run --rm app fetch-vacancy-detail --vacancy-id <vacancy_id>
docker compose --profile ops run --rm app reconcile-run --run-id <run_id>
```

## 7. Backup

Создать PostgreSQL backup:

```bash
make backup
```

Результат сохраняется в `.state/backups/`.

По умолчанию backup:

- делается через `pg_dump --format=custom`
- содержит `--create` и `--clean`
- чистит старые dump-файлы по `HHRU_BACKUP_RETENTION_DAYS`
- сразу проверяется как restorable archive и публикует lifecycle metrics

Повторно проверить конкретный файл:

```bash
make verify-backup BACKUP_FILE=.state/backups/<file>.dump
```

## 8. Restore

Рекомендуемый путь сначала сделать safe restore drill в отдельную DB:

```bash
make restore-drill BACKUP_FILE=.state/backups/<file>.dump
```

Legacy destructive restore остаётся только как аварийный инструмент и требует явного подтверждения.

Восстановить из файла, который уже виден внутри backup container как `/backups/...`:

```bash
make restore BACKUP_FILE=/backups/<file>.dump
```

## 9. Полезные проверки после deploy

- `docker compose ps`
- `docker compose logs --tail=100 metrics`
- `docker compose logs --tail=100 grafana`
- `docker compose logs --tail=100 postgres`
- `make compose-show-metrics`

## 10. Обновление приложения

После обновления кода:

```bash
make up
make migrate-compose
```

Если включён Prometheus profile:

```bash
make up-observability
```
