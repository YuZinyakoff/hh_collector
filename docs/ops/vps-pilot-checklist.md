# VPS Pilot Checklist

Дата: 2026-04-25

Цель этого документа: провести первый VPS pilot без смешивания трёх разных задач:

- получить successful `search-only` baseline на стабильном хосте;
- подтвердить backup/restore/offsite archive contour;
- не смешивать search baseline с масштабным `first-detail` drain.

## 1. Что уже готово

- Planner v2 и `area -> time_window` fallback выдержали near-complete live run.
- Memory regression снят: crawler больше не растёт до WSL memory wall.
- `resume-run-v2` умеет переочередить failed terminal search partitions.
- Short snapshot churn снижен до `first_seen/hash_changed`.
- Есть local archive export для `raw_api_payload` и `vacancy_snapshot`.
- Housekeeping умеет `--archive-before-delete`.
- WebDAV offsite sync проверен на Yandex Disk:
  - smoke bundle uploaded;
  - повторный sync skipped по receipt;
  - real DB archive bundles uploaded.
- Есть persistent `first-detail` backlog + `detail_worker` loop для bounded drain.
- Локально проверен batch `1000`: `962` detail snapshots, `38` HTTP 404, средняя скорость около `2.9 req/s`.
- HTTP 404 detail responses закрываются как `terminal_404` и не остаются retryable backlog.
- Добавлены first-detail backlog metrics и alert rules.

## 2. Что ещё не готово

- `first-detail` backlog ещё не прогнан на масштабе полного baseline.
- Нет dashboard panels для first-detail backlog metrics.
- Нет cooldown/backoff для repeated non-terminal detail failures.
- Нет production alert delivery; dashboards/metrics foundation есть, но уведомления ещё не оформлены.
- Нет многодневного unattended production signal.

Практический вывод: VPS pilot должен быть `search-only` baseline pilot, а не финальный месячный production launch.

## 3. Рекомендуемый VPS spec

Эконом-пилот:

- `4 vCPU`
- `8-12 GB RAM`
- `160-200 GB NVMe`

Разумный single-node старт:

- `8 vCPU`
- `16 GB RAM`
- `320-500 GB NVMe`

Для первого pilot лучше не брать диск меньше `160 GB`. Один near-complete local `search-only` run уже дал несколько GB Postgres growth, а `detail` stage позже увеличит storage pressure.

## 4. Подготовка VPS

1. Установить Docker Engine и Compose plugin.
2. Клонировать репозиторий.
3. Скопировать env profile:

```bash
cp deploy/env.vps.example .env
```

4. В `.env` заменить:

- `HHRU_DB_PASSWORD`
- `HHRU_HH_API_USER_AGENT`
- `HHRU_HH_API_APPLICATION_TOKEN`, если используем application token
- `HHRU_GRAFANA_ADMIN_PASSWORD`
- `HHRU_HOUSEKEEPING_ARCHIVE_OFFSITE_USERNAME`
- `HHRU_HOUSEKEEPING_ARCHIVE_OFFSITE_PASSWORD`

5. На pilot держать bind hosts на `127.0.0.1` и ходить через SSH tunnel.

## 5. Первый deploy

```bash
make up
make migrate-compose
make compose-health
docker compose ps
```

Ожидаемо:

- `postgres`, `redis`, `metrics` healthy;
- `health-check` показывает `env=production`;
- `hh_api_user_agent_live_search_valid=yes`;
- `housekeeping_archive_offsite_configured=yes`, если уже настроен Yandex Disk WebDAV.

## 6. Preflight

```bash
docker compose --profile ops run --rm app sync-dictionaries --name areas
make backup
```

Проверить свежий backup:

```bash
make verify-backup BACKUP_FILE=.state/backups/<file>.dump
make restore-drill BACKUP_FILE=.state/backups/<file>.dump
```

Проверить archive/offsite contour без удаления:

```bash
docker compose --profile ops run --rm app export-retention-archive --triggered-by vps-preflight
docker compose --profile ops run --rm app sync-retention-archive-offsite --triggered-by vps-preflight
```

Если retention candidates ещё нет, `total_exported=0` это нормально. Важно, чтобы command path не падал.

## 7. Search-Only Baseline

Цель: получить successful terminal `search-only` baseline на VPS.

```bash
tmux new -s hh-search-baseline
```

Внутри `tmux`:

```bash
docker compose --profile ops run --rm app run-once-v2 \
  --sync-dictionaries no \
  --detail-limit 0 \
  --detail-refresh-ttl-days 30 \
  --triggered-by vps-search-baseline
```

Detach:

```text
Ctrl+b, d
```

Проверять:

```bash
tmux ls
docker compose ps
docker compose logs --tail=100 metrics
```

## 8. Если Baseline Упал

Если причина похожа на transient transport/DNS outage:

```bash
docker compose --profile ops run --rm app resume-run-v2 --run-id <run_id> --triggered-by vps-resume
```

Если run упал из-за memory/disk/host issue:

- не запускать сразу заново;
- снять `docker compose ps`;
- снять `docker compose logs --tail=200 postgres metrics`;
- проверить disk usage;
- проверить размер Postgres volume и `.state`.

## 9. После Successful Baseline

Сразу сделать:

```bash
make backup
make verify-backup BACKUP_FILE=.state/backups/<file>.dump
make restore-drill BACKUP_FILE=.state/backups/<file>.dump
```

Затем:

```bash
docker compose --profile ops run --rm app export-retention-archive --triggered-by post-baseline
docker compose --profile ops run --rm app sync-retention-archive-offsite --triggered-by post-baseline
```

Снять факты:

- unique vacancies;
- seen events;
- HH request count;
- DB size;
- backup size;
- archive size;
- run duration;
- terminal status.

## 10. После VPS Pilot

Следующий крупный slice после successful search baseline:

1. Запустить MVP first-detail drain на bounded batch.
2. Замерить detail throughput, failure mix и storage growth.
3. Добавить dashboard panels и cooldown/backoff для repeated non-terminal detail failures.
4. Провести supervised `search + detail drain` week.
5. Только потом месячное unattended окно.

Пока detail backlog не прогнан на масштабе baseline, утверждение "полная research completeness" преждевременно. Корректная формулировка после pilot: full search coverage is operationally validated.

См. также: [first-detail-backlog.md](/home/yurizinyakov/projects/hh_collector/docs/ops/first-detail-backlog.md).
