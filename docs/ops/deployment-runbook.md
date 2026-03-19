# Deployment Runbook

Практичный baseline для развёртывания текущего MVP collector на одной VPS через Docker Compose.

## 1. Подготовка VPS

- Установить Docker Engine и Compose plugin.
- Клонировать репозиторий на VPS.
- Держать внешние bind-порты на `127.0.0.1`, если доступ планируется только через SSH tunnel или reverse proxy.

## 2. Подготовка окружения

Скопировать шаблон:

```bash
cp .env.example .env
```

Минимально проверить и изменить:

- `HHRU_ENV=production`
- `HHRU_DB_PASSWORD`
- `HHRU_HH_API_USER_AGENT`
- `HHRU_DB_BIND_HOST`, `HHRU_REDIS_BIND_HOST`, `HHRU_METRICS_BIND_HOST`, `HHRU_PROMETHEUS_BIND_HOST`

## 3. Запуск сервисов

Поднять базовый long-running набор:

```bash
make up
```

Если нужен Prometheus:

```bash
make up-observability
```

Это поднимает:

- `postgres`
- `redis`
- `metrics`
- `prometheus` только при `up-observability`

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

## 6. Операторские CLI-команды

Самый короткий run-once сценарий:

```bash
docker compose --profile ops run --rm app run-once --sync-dictionaries yes --pages-per-partition 1 --detail-limit 5 --triggered-by vps-manual
```

`run-once` — это orchestration-lite shortcut поверх существующих MVP slices. Он не заменяет scheduler и не создаёт очередь.

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

## 8. Restore

Restore перезаписывает базу из dump и поэтому требует явного подтверждения.

Восстановить из файла, который уже виден внутри backup container как `/backups/...`:

```bash
make restore BACKUP_FILE=/backups/<file>.dump
```

## 9. Полезные проверки после deploy

- `docker compose ps`
- `docker compose logs --tail=100 metrics`
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
