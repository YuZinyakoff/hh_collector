# First-Detail Backlog

Цель: постепенно довести найденные через search вакансии до состояния, где каждая активная vacancy имеет хотя бы один successful detail payload.

## Semantics

- Backlog не хранится отдельной таблицей.
- Backlog выводится из `vacancy_current_state`.
- Item считается открытым, если он не находится в закрытом detail outcome.
- Закрытые outcomes: `succeeded` и `terminal_404`.
- `terminal_404` означает, что detail endpoint вернул HTTP 404. Это не создаёт detail snapshot, но закрывает first-detail backlog item, чтобы worker не ретраил протухшую вакансию бесконечно.
- По умолчанию worker берёт только `is_probably_inactive = false`.
- Для исторического догребания можно включить `--include-inactive yes`, но это отдельный режим с большей долей 404/архивных вакансий.

Detail attempts из этого контура пишутся с `reason=first_detail_backlog` и `crawl_run_id=null`. Это осознанно отделяет глобальный first-detail drain от repair backlog конкретного `crawl_run`.

## One-Shot Local Drain

```bash
make drain-first-detail-backlog ARGS="--limit 25 --triggered-by local-detail-smoke"
```

Ожидаемые ключевые поля:

- `backlog_size_before`
- `ready_backlog_size_before`
- `cooldown_skipped_before`
- `selected_count`
- `detail_fetch_succeeded`
- `detail_fetch_terminal`
- `detail_fetch_failed`
- `backlog_size_after`
- `ready_backlog_size_after`
- `cooldown_skipped_after`

Exit code:

- `0`, если выбранный batch прошёл без retryable item failures; `terminal_404` не считается retryable failure;
- `1`, если хотя бы один item завершился ошибкой, но остальные items всё равно были обработаны.

## Worker Loop

Один tick:

```bash
make worker-detail ARGS="--once --batch-size 25 --triggered-by local-detail-smoke"
```

Долгий локальный drain:

```bash
make worker-detail ARGS="--batch-size 100 --interval-seconds 300 --triggered-by local-detail-worker"
```

Compose service:

```bash
docker compose --profile ops up -d detail-worker
docker compose logs -f detail-worker
```

Для первого VPS `search-only` pilot service лучше не включать. После успешного baseline можно включать detail worker отдельно и смотреть на скорость уменьшения backlog.

## Supervised Measurement

Для следующего controlled шага не запускать worker "вслепую"; использовать tmux launcher, который сохраняет preflight/postflight snapshot, worker log и summary:

```bash
BATCH_SIZE=100 MAX_TICKS=1 make detail-worker-measurement
```

Пути печатаются в stdout, а последние значения сохраняются в:

- `.state/reports/detail-worker-measurement.tmux-log`
- `.state/reports/detail-worker-measurement.summary`

Если host Python не может подключиться к Postgres через `localhost`, launcher по умолчанию переопределяет `HHRU_DB_HOST=127.0.0.1`. Для другой схемы подключения можно задать:

```bash
HOST_DB_HOST=localhost BATCH_SIZE=100 MAX_TICKS=1 make detail-worker-measurement
```

Что смотреть после завершения:

- `selected_total`
- `succeeded_total`
- `terminal_total`
- `failed_total`
- `active_backlog_delta`
- `db_size_delta_bytes`
- `first_detail_attempt_status.*` в postflight report

## VPS First Measurement

После successful VPS `search-only` baseline следующий безопасный шаг - bounded one-shot drain на `100` items, без включения long-running `detail-worker` service.

Baseline context на 2026-05-15:

- `run_id=c7e7d8c6-6813-454c-845e-ca44539da1e8`
- `vacancy=865868`
- `short snapshots=872201`
- `raw_payload=129008`
- `detail_stage_status=skipped`
- `coverage_ratio=1.0000`

Команды для первого VPS measurement:

```bash
cd /opt/hh_collector

LIMIT=100 make vps-first-detail-measurement
```

Эквивалентный ручной вариант:

```bash
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_DIR=".state/reports/vps-first-detail-measurement/$RUN_TS"
mkdir -p "$REPORT_DIR"

docker compose --profile ops run --rm --entrypoint python app \
  scripts/dev/write_detail_backlog_report.py \
  | tee "$REPORT_DIR/preflight.txt"

docker compose --profile ops run --rm app drain-first-detail-backlog \
  --limit 100 \
  --triggered-by "vps-first-detail-measurement-$RUN_TS" \
  | tee "$REPORT_DIR/drain.txt"

docker compose --profile ops run --rm --entrypoint python app \
  scripts/dev/write_detail_backlog_report.py \
  | tee "$REPORT_DIR/postflight.txt"
```

Для первого run ожидаем не скорость любой ценой, а измерение:

- `selected_count`
- `detail_fetch_succeeded`
- `detail_fetch_terminal`
- `detail_fetch_failed`
- `active_backlog_size` delta
- `detail_snapshot_rows` delta
- `raw_payload_rows` delta
- `db_size_bytes` delta

Если `detail_fetch_failed > 0`, не расширять batch. Сначала смотреть error mix и cooldown counters.

## Latest Controlled Local Run

2026-04-27 после добавления dashboard panels и cooldown metrics был выполнен один controlled worker tick:

```bash
make worker-detail ARGS="--once --batch-size 25 --triggered-by controlled-detail-worker-smoke-20260427"
```

Итог:

- `selected_count=25`
- `detail_fetch_succeeded=24`
- `detail_fetch_terminal=1`
- `detail_fetch_failed=0`
- `backlog_size`: `766414 -> 766389`
- `ready_backlog_size`: `766414 -> 766389`
- `cooldown_skipped=0`
- duration: `13.292s`, примерно `1.88 detail req/s`
- DB size delta: `270336 bytes`

На этом sample storage growth составил примерно `10.8 KB` на selected item. Это не финальная capacity-константа, но полезная нижняя оценка для планирования первого bounded drain.

2026-04-28 был выполнен supervised measurement через `make detail-worker-measurement`:

```bash
BATCH_SIZE=100 MAX_TICKS=1 make detail-worker-measurement
```

Итог:

- `selected_total=100`
- `succeeded_total=100`
- `terminal_total=0`
- `failed_total=0`
- `active_backlog`: `766389 -> 766289`
- duration: `39.333s`, примерно `2.54 detail req/s`
- DB size delta: `2277376 bytes`
- artifact: `.state/reports/detail-worker-measurement/20260428T111507Z/summary.md`

На этом sample storage growth составил примерно `22.8 KB` на selected item. Это ближе к practical local measurement, чем batch `25`, но всё ещё не финальная capacity-константа для VPS.

## Retry Cooldown

Retryable/non-terminal failures не выбираются заново сразу.

Политика по умолчанию:

- base cooldown: `3600s`.
- repeated failed attempts double the cooldown: `1h`, `2h`, `4h`, ...
- max cooldown cap: `86400s`.
- `terminal_404` не входит в retry cooldown, потому что закрывает item.

CLI overrides:

```bash
make drain-first-detail-backlog ARGS="--limit 100 --retry-cooldown-seconds 3600 --max-retry-cooldown-seconds 86400"
```

Для диагностики one-shot и worker выводят:

- `ready_backlog_size_*`: сколько открытых items можно брать прямо сейчас;
- `cooldown_skipped_*`: сколько открытых items временно пропущено из-за последнего retryable failure.

## Metrics

`drain-first-detail-backlog` и `detail_worker` публикуют:

- `hhru_first_detail_backlog_size{scope="active"}` для стандартного режима.
- `hhru_first_detail_backlog_size{scope="all"}` для режима `--include-inactive yes`.
- `hhru_first_detail_ready_backlog_size{scope}` для items, которые можно брать прямо сейчас: без active lease и без retry cooldown.
- `hhru_first_detail_cooldown_backlog_size{scope}` для backlog items, временно пропущенных cooldown-ом или active lease.
- `hhru_first_detail_drain_attempt_total{scope,outcome}`.
- `hhru_first_detail_drain_selected_total{scope}`.
- `hhru_first_detail_drain_succeeded_total{scope}`.
- `hhru_first_detail_drain_terminal_total{scope}`.
- `hhru_first_detail_drain_failed_total{scope}`.

Для alerting важно разделять `terminal` и `failed`: рост `terminal` ожидаем для протухших вакансий, рост `failed` означает retryable/non-terminal проблемы.

## Current Limits

- Это MVP без Redis queue: admission идёт через DB claim/lease поверх
  `vacancy_current_state`.
- Есть first-detail backlog metrics, alert rules и Grafana panels.
- Есть per-vacancy exponential cooldown после repeated non-terminal failures.
- HTTP 404 detail responses закрываются как `terminal_404`.
- Нет отдельной политики для archived/inactive vacancies кроме исключения `is_probably_inactive=false` по умолчанию.
- Atomic claim/lease реализован через короткую transaction с row lock /
  `SKIP LOCKED`; network fetch выполняется после commit, чтобы не держать locks
  весь batch.
- `first_detail_lease_expires_at` возвращает crashed/aborted rows в ready backlog
  после timeout; успешный/terminal/failed detail outcome очищает lease.

## Parallel Worker Gate

Перед длительным запуском двух и более `detail-worker` нужно провести controlled
parallel worker test на VPS.

Текущая claim/lease semantics:

- короткая transaction выбирает candidates через row lock / `SKIP LOCKED`;
- выбранные rows помечаются `running`, `first_detail_lease_owner` и
  `first_detail_lease_expires_at`;
- network fetch выполняется после commit, чтобы не держать locks весь batch;
- lease имеет timeout, чтобы crash worker-а не оставлял rows навсегда занятыми;
- retry cooldown продолжает применяться к retryable failures;
- `succeeded` и `terminal_404` остаются закрывающими outcomes.

Go/no-go для parallelism:

- два worker-а не делают duplicate detail fetches для одного selected row;
- crash/restart worker-а возвращает leased rows в ready backlog после timeout;
- `ready_backlog_size` и `cooldown_backlog_size` остаются объяснимыми;
- metrics/log summary показывают selected/claimed/fetched/resolved counts.

VPS observation 2026-05-23:

- `batch=500`, `interval=60` дал устойчивый single-worker baseline без роста
  retryable failures;
- sustained duration после нескольких часов: около `950-1130s` на `500` selected;
- restart worker-а не сбросил duration, поэтому это больше похоже на sustained
  upstream/time-of-day latency, чем на локальный leak;
- следующий hardening slice: controlled 2-worker measurement на claim/lease.

VPS observation 2026-05-24:

- atomic claim/lease прошёл controlled 2-worker safety test: два разных
  `first_detail_lease_owner`, `expired_leases=0`, `failed_states=0`;
- duplicate selection blocker снят, но 2 worker-а не дали throughput gain;
- observed backlog drain за несколько часов был около `800-900/hour`, то есть не
  лучше single-worker baseline;
- likely bottleneck находится не в DB claim и не в явном client-side throttle:
  `HHApiClient` выполняет sync `urlopen` без общего rate limiter; единственный
  `sleep` в detail path - `5s` transport retry backoff, который не активен при
  `detail_fetch_failed=0`;
- hypothesis: HH/upstream/IP/auth/network path даёт общий sustained budget, который
  несколько worker-ов делят между собой.

## Parallelism Experiment Plan

Пока не делим backlog на lanes/run/priority. Это отдельное решение с риском
изменить research semantics, его нужно принимать после измерений.

Эксперименты должны отвечать на один вопрос за раз:

1. Measure baseline with better telemetry.
   - режим: `scale=1`, `batch=100`, `interval=60`, application token enabled;
   - цель: короткие cycles, быстрый feedback;
   - метрики: batch duration, selected/hour, `detail_fetch_failed`,
     terminal_404 rate, p50/p95 `api_request_log.latency_ms`, gap between
     consecutive detail requests.
2. Compare scale without changing batch.
   - режимы: `scale=1`, `scale=2`, optionally `scale=3`;
   - batch одинаковый, measurement window минимум 60-90 минут на режим;
   - stop condition: `failed_states > 0`, `expired_leases > 0`,
     sustained `drain_first_detail_backlog.failed`, captcha/403/5xx growth.
3. Compare batch size.
   - режимы: `batch=50`, `batch=100`, `batch=250`, `batch=500`;
   - цель: понять, есть ли degradation от long batch lifetime или DB/write
     accumulation внутри process.
4. Auth vs anonymous.
   - application token легален и доступен, поэтому отдельно сравнить authenticated
     detail contour с anonymous только коротким bounded test;
   - не смешивать с scale test в одном window.
5. Search interference test.
   - перед production weekly schedule проверить, ухудшает ли detail-worker search
     latency/error mix;
   - режим: controlled search-only or low-detail run with detail-worker on/off.

Decision rules:

- Если `scale=2/3` не увеличивает selected/hour минимум на `25%` без роста
  failures, production default остаётся `scale=1`.
- Если small `batch=100` даёт такой же throughput, но быстрее обнаруживает
  failures, использовать его для experiments; для steady drain можно вернуть
  `batch=500`.
- Если p50/p95 latency растёт пропорционально scale, bottleneck считается upstream
  sustained budget, а не local worker implementation.
- Если между `fetch_vacancy_detail.succeeded` и следующим
  `fetch_vacancy_detail.started` есть large unexplained gaps, искать local DB/logging
  overhead.
- Если `.state/metrics/metrics.json` быстро растёт, проверить upstream metric
  cardinality. Detail endpoint metrics должны агрегироваться как
  `/vacancies/{vacancy_id}`, а не как `/vacancies/<hh_id>`, иначе каждый detail
  request создаёт новую time series и file-backed registry начинает всё дороже
  читать/перезаписывать state-файл.
