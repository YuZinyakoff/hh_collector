# VPS Pilot Checklist

Дата: 2026-04-29

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
- Добавлен exponential cooldown для repeated non-terminal detail failures.
- Добавлены Grafana panels для first-detail open/ready/cooldown backlog и drain outcomes.
- Controlled local `detail-worker --once --batch-size 25` прошёл успешно: `24` detail snapshots, `1` terminal_404, `0` retryable failures, `~1.88 req/s`, DB delta `270336 bytes`.
- Добавлен Alertmanager + `alert-webhook` receiver; Telegram delivery включается через env.
- Добавлен in-run search transport budget для `run-once-v2` и `resume-run-v2`: transient transport failed partitions переочередятся до лимитов `3` consecutive / `5` total.

## 2. Что ещё не готово

- `first-detail` backlog ещё не прогнан на масштабе полного baseline.
- Production alert delivery foundation есть, но на VPS ещё нужно настроить Telegram credentials и сделать synthetic alert test.
- Новый in-run transport budget ещё не проверен на полном VPS `search-only` baseline.
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

### Timeweb Cloud decision

Источник истины на момент выбора: актуальная панель Timeweb Cloud. Ссылки для сверки:

- [VDS/VPS tariffs](https://timeweb.cloud/services/vds-vps)
- [Cloud servers calculator](https://timeweb.cloud/prices/)
- [Cloud server configuration](https://timeweb.cloud/docs/cloud-servers/manage-servers/server-configuration)
- [Network drives](https://timeweb.cloud/docs/network-drives)
- [Managed PostgreSQL](https://timeweb.cloud/services/postgresql)
- [S3 storage](https://timeweb.cloud/services/s3-storage)

Решение для первого pilot:

- взять один VPS/cloud server с локальным NVMe/SSD и держать `postgres`, `redis`, `metrics`, `prometheus`, `grafana`, `alertmanager` в Docker Compose на этом же хосте;
- целиться минимум в `4 vCPU / 8 GB RAM / 160 GB NVMe`;
- если разница в цене приемлемая, сразу брать production-shaped `8 vCPU / 16 GB RAM / 320-500 GB NVMe`;
- не использовать сетевой HDD как основной диск PostgreSQL;
- сетевой диск или S3 использовать только под backups, retention archives и cold/offsite copies;
- Managed PostgreSQL отложить до момента, когда single-node Compose перестанет быть достаточным по storage/ops/availability.

Почему так:

- PostgreSQL для crawler state и raw payloads чувствителен к latency и random IO, поэтому live DB должна лежать на локальном NVMe/SSD или, минимум, на высокопроизводительном сетевом NVMe, но не на HDD cold storage.
- Сетевой HDD выгоден для объёма, но это backup/archive tier, не primary database tier.
- Managed PostgreSQL снижает операционную нагрузку, но добавляет стоимость, сетевую связность, отдельный backup/restore contour и отклоняется от текущего Docker Compose runbook.
- S3/Yandex Disk/WebDAV подходят для offsite-копий и архивов; это не файловая система для live PostgreSQL.

Go/no-go после pilot:

- если `DB size + next backup + local archive` занимает больше `60-70%` диска, до `detail` drain нужно увеличивать диск или мигрировать на larger node;
- если `search-only` baseline стабилен, но `detail` batch показывает быстрый рост storage, переходить на `320-500 GB` до недельного `search + detail`;
- если CPU/RAM становятся узким местом, сначала масштабировать VPS вверх; managed DB рассматривать вторым шагом.

Локальные storage facts на 2026-04-29:

- near-complete `search-only` corpus: `767451` unique vacancies, `880556` seen events, `57101` HH requests, `coverage_ratio=0.9863`;
- текущая локальная PostgreSQL DB после baseline и small detail samples: `4595626467 bytes` (`~4.4 GB`);
- row counts: `880556` short snapshots, `1122` detail snapshots, `58276` raw payload rows;
- fresh `pg_dump` custom backup: `905826544 bytes` (`~0.91 GB`), duration `~176s`;
- measured `detail-worker` batch `100`: DB delta `2277376 bytes`, примерно `22.8 KB` на successful detail item.

Практическая оценка:

- full `search-only` baseline с текущей схемой ожидается в порядке `5 GB` DB, не десятки GB;
- first-detail drain для `~767k` vacancies по текущему small-sample порядку может добавить `~15-30 GB`, но это пока bounded measurement, не доказанная production-константа;
- `160 GB` локального NVMe достаточно для VPS pilot и early supervised drain, если держать локально только короткую цепочку backups и регулярно выгружать archives/offsite;
- `320-500 GB` остаётся safer production-shaped вариантом, но не обязательным для первого pilot.

Компромисс при cost pressure:

- основной PostgreSQL volume оставить на локальном NVMe/SSD `160 GB`;
- сетевой HDD подключить только под `.state/backups`, `.state/archive` и временные export bundles;
- после успешного offsite sync удалять локальные archive bundles по retention policy;
- не переносить `/var/lib/postgresql/data` на сетевой HDD без отдельного load test.

Storage terminology:

- `api_request_log`: metadata about one request to HH API.
- `raw_api_payload`: raw full JSON response body from HH API for search/detail endpoints.
- `vacancy_seen_event`: observation fact that a vacancy appeared in a search result page/partition.
- `vacancy_snapshot` with `snapshot_type=short`: normalized per-vacancy document extracted from a search page item. Current behavior: created only on `first_seen` or `short_hash_changed`.
- `vacancy_snapshot` with `snapshot_type=detail`: normalized per-vacancy document extracted from a successful `GET /vacancies/{id}` detail response. Current behavior: created on every successful detail fetch.
- `vacancy_current_state`: current aggregate state and latest known short/detail hashes/statuses.
- `detail_fetch_attempt`: operational attempt log for detail requests, including retries, terminal 404 and retryable failures.

Future storage optimization candidate:

- detail snapshots can likely adopt `first_seen/hash_changed` semantics too, using `last_detail_hash`, so repeated successful detail refetches with identical normalized content do not create new `vacancy_snapshot` rows;
- raw detail payload retention can still preserve the full original JSON for a bounded TTL/archive window even if normalized detail snapshot churn is reduced;
- this should be implemented only after VPS measurements confirm that detail snapshot churn is a real storage pressure point.

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

Если поднят observability profile:

```bash
make up-observability
curl http://127.0.0.1:9100/metrics >/dev/null
curl http://127.0.0.1:8080/metrics >/dev/null
```

В Prometheus targets должны быть `up`:

- `hhru_platform`
- `node_exporter`
- `cadvisor`

Перед long run открыть Grafana dashboard `Host / Container Resources` и убедиться, что видны host CPU/RAM/filesystem/IO и container CPU/RAM.

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

По умолчанию `run-once-v2` не валит весь baseline на первом transient transport leaf: failed search partition будет переочереден, пока не достигнуты лимиты `3` consecutive / `5` total transport failures. В summary смотреть:

- `search_transport_failures_total`
- `search_captcha_failures_total`

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

`resume-run-v2` использует тот же in-run transport budget. Если summary содержит `search transport budget exhausted`, не запускать blind loop сразу: сначала проверить сеть/DNS/VPS host и recent HH API health.

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
3. Оформить alert delivery.
4. Провести supervised `search + detail drain` week.
5. Только потом месячное unattended окно.

Пока detail backlog не прогнан на масштабе baseline, утверждение "полная research completeness" преждевременно. Корректная формулировка после pilot: full search coverage is operationally validated.

См. также: [first-detail-backlog.md](/home/yurizinyakov/projects/hh_collector/docs/ops/first-detail-backlog.md).
