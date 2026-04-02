# HH API Completeness Implementation Plan

Статус: active plan  
Дата: 2026-04-03

Этот документ фиксирует конкретный порядок работ после уточнения research goal:

- полный `search` coverage;
- хотя бы один успешный `detail` для каждой vacancy, хотя бы раз увиденной через `search`;
- проверка не только HH policy, но и готовности системы к длинным baseline и follow-up прогонам.

## 1. Current Facts

### 1.1. Search baseline capacity

По текущему live snapshot:

- visible vacancies: `885266`;
- lower-bound search-only pages: `44264`;
- lower-bound search-only sweep at current baseline: `~9.34h`.

Источник:

- [capacity snapshot](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T140325Z-capacity-snapshot.json)

Практическая operator estimate:

- первый реальный full `search` baseline run лучше планировать не как `9.34h`, а как окно `12-16h`;
- `9.34h` это lower bound без tree split overhead, retries, unresolved handling, operator pauses и прочих реальных потерь.

### 1.2. Detail drain capacity

Measured detail envelope на `application_token` уже есть:

- sequential `2000` detail requests: `~179.9 req/min`, без `403/captcha`, с `37x404` и `3` connection resets;
- conservative batched `workers=3`, `burst_pause=1s`, `1200` detail requests: `~119.5 req/min`, без `403/captcha`, с `20x404`.

Источник:

- [detail drain capacity summary](/home/yurizinyakov/projects/hh_collector/.state/reports/hh-api-probe/20260331T154609Z-detail-drain-capacity-summary.md)

Практический вывод:

- сам `detail` endpoint пока не выглядит главным blocker;
- главный blocker сейчас это backlog semantics и отсутствие steady-state measurement.

### 1.3. Local readiness snapshot on 2026-04-01

Фактически проверено:

- `health-check` зелёный по env/settings:
  - `hh_api_application_token_configured=yes`
  - `hh_api_default_auth_mode=application_token`
  - `hh_api_user_agent_live_search_valid=yes`
- `docker compose ps` на clean reset path показывает healthy `postgres`, `redis`, `metrics`, без висящего `scheduler`;
- `compose-health` теперь тоже видит `application_token`, то есть system path больше не расходится с host path;
- targeted `ruff` и `mypy src` зелёные;
- весь `tests/unit` green (`127 passed`);
- весь `tests/integration` green (`19 passed`);
- backup path проверен end-to-end:
  - `make backup` succeeded;
  - `make verify-backup` succeeded;
  - `make restore-drill` succeeded;
- guarded live smoke на отдельном `crawl_run` прошёл clean:
  - `plan-run-v2` создал `9` root areas;
  - `run-list-engine-v2 --partition-limit 1` clean обработал `1` terminal partition;
  - `66` search pages, `1319` seen events, `395` created vacancies;
  - `failed_partitions=0`, `unresolved_partitions=0`.
- после этого local DB была заново сброшена, мигрирована и `areas` synced на чистом состоянии;
- снят свежий clean-state backup:
  - [hhru-platform_hhru_platform_20260331T214853Z.dump](/home/yurizinyakov/projects/hh_collector/.state/backups/hhru-platform_hhru_platform_20260331T214853Z.dump)

Что ещё не проверено в этом конкретном preflight:

- полный локальный `prometheus + grafana` observability profile без Docker daemon/network noise;
- реальная alert delivery;
- unattended soak history на длинном прогоне.

Практический вывод:

- для первого supervised full `search` baseline run этого preflight уже достаточно;
- для unattended/soak-grade эксплуатации этого ещё недостаточно, пока observability profile и alert delivery не доведены до нормального ops состояния.

Первый фактический full `search-only` baseline run на этом preflight вскрыл отдельный planner completeness blocker:

- [hh-api-search-baseline-blocker-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-search-baseline-blocker-plan.md)

По состоянию на 2026-04-03 minimal `time_window` fallback slice уже не только реализован, но и validated живым длинным baseline run.

### 1.4. Near-complete search baseline result on 2026-04-02

Первый действительно длинный local `search-only` baseline дал уже не smoke-signal, а почти финальный search corpus.

Run:

- run id: `5943c659-cd02-48c6-8296-c4ccbd46be73`
- started at: `2026-04-02 01:19:37 MSK`
- finished at: `2026-04-02 14:52:55 MSK`
- terminal status: `failed`

Причина terminal failure:

- не planner;
- не memory pressure;
- не внутренний crash collector;
- а внешний outage/transport failure:
  - `URLError: [Errno -3] Temporary failure in name resolution`

Фактически собрано:

- unique vacancies in `vacancy_current_state`: `767451`
- `vacancy_seen_event`: `880556`
- HH API requests: `57101`
- `detail_fetch_attempt`: `0`

Tree outcome:

- `total_partitions=16527`
- `covered_terminal_partitions=14985`
- `split_partitions=1334`
- `pending_terminal_partitions=207`
- `failed_partitions=1`
- `unresolved_partitions=0`
- `coverage_ratio=0.9863`

Практический вывод из этого run:

- planner completeness blocker реально снят;
- memory blocker реально снят;
- текущий search contour уже выдерживает `~13.5h` живого baseline run;
- новый blocker теперь не baseline viability, а resilience к transient transport/outage и ability either to resume or to avoid losing near-complete run.

## 2. Execution Order

Рекомендуемый порядок:

1. Transport/outage hardening and operator-visible resume semantics after the near-complete baseline result.
2. VPS pilot for the next full `search-only` baseline rerun on a more stable host.
3. Persistent first-detail backlog MVP.
4. Serious completeness measurements on several full sweeps.

Не делать наоборот:

- не запускать long completeness study до persistent first-detail backlog;
- не считать `detail_limit=20` достаточной гарантией полноты;
- не запускать первый VPS-scale full sweep без transport/outage recovery plan.

## 3. Phase A: Small Hardening Before Full Baseline

Цель:

- сделать длинный baseline sweep безопасным и operator-readable.

Scope:

1. `search` transport circuit breaker
2. terminal status integration

Источник:

- [hh-api-policy-v1-next-implementation-steps.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-policy-v1-next-implementation-steps.md)

Acceptance criteria:

- runtime больше не продолжает blind list execution после первого hard failed search partition;
- terminal status честно различает `failed`, `completed_with_unresolved`, `completed_with_detail_errors`;
- tests green на targeted slices;
- docs/runbook синхронизированы.

Текущий practical reading:

- для первого full `search` baseline run этого уже достаточно;
- более сложный thresholded transport budget `3 consecutive / 5 total` остаётся полезным follow-up hardening, но больше не является immediate blocker.

Почему это нужно до baseline run:

- иначе мы измеряем не baseline policy, а смесь baseline и отсутствующего degraded-stop logic;
- для длинного run это уже operational risk, а не мелкая косметика.

## 4. Phase B: Preflight For First Full Search Baseline

### 4.1. DB reset

Локальную БД можно чистить. Важные research artifacts лежат в `.state/reports`.

Recommended reset path:

```bash
docker compose down -v
make up
make migrate-compose
```

### 4.2. Preflight checklist

До первого baseline run должно быть зелёным:

```bash
./.venv/bin/python -m ruff check .
./.venv/bin/python -m mypy src
./.venv/bin/python -m pytest tests/unit -q
./.venv/bin/python -m pytest tests/integration -q
make backup
BACKUP_FILE=.state/backups/<latest>.dump make verify-backup
BACKUP_FILE=.state/backups/<latest>.dump make restore-drill
make compose-health
docker compose ps
```

Дальше один ручной smoke:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main sync-dictionaries --name areas
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main trigger-run-now --sync-dictionaries no --detail-limit 0 --detail-refresh-ttl-days 30 --triggered-by baseline-preflight-smoke
```

Acceptance criteria:

- planner v2 стартует;
- `areas` synced;
- scheduler/runtime path не падает на первом guarded run;
- baseline path (`postgres`, `redis`, `metrics`, logs, health-checks) жив;
- backup свежий.

Важно:

- полный `prometheus/grafana` profile не является blocker для первого ручного baseline run;
- но он остаётся blocker для unattended long-run и system-wide soak readiness.

## 5. Phase C: First Full Search Baseline Run

Цель:

- измерить чистый `search` contour без detail backlog noise;
- проверить, что planner v2 теперь проходит hot leaf areas через `time_window` fallback, а не завершает run на `unresolved`.

Статус на 2026-04-03:

- локально эта фаза уже практически доказана near-complete run'ом;
- следующий rerun нужен уже не для проверки planner viability, а для получения clean successful terminal outcome на более стабильном host-е.

Command:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main run-once-v2 \
  --sync-dictionaries no \
  --detail-limit 0 \
  --detail-refresh-ttl-days 30 \
  --triggered-by search-baseline-2026-04-01
```

Safer detached launcher for manual operator runs:

```bash
./scripts/dev/launch_search_baseline_rerun.sh
```

Этот launcher нужен именно для длинного локального baseline, чтобы не зависеть от multiline paste в shell.

Для VPS/local WSL long-run safer operator path теперь такой:

```bash
./scripts/dev/launch_search_baseline_tmux.sh
```

А для параллельного наблюдения:

```bash
./scripts/dev/launch_host_watch_tmux.sh
./scripts/dev/launch_process_watch_tmux.sh
```

Почему именно так:

- `run-once-v2`, а не scheduler-loop, чтобы получить один чистый measurement run;
- `detail-limit=0`, чтобы померить именно list coverage и sweep duration;
- dictionaries уже подготовлены отдельно.

Что собирать по результату:

- wall clock;
- terminal status;
- `coverage_ratio`;
- `total_partitions`;
- `covered_terminal_partitions`;
- `pending_terminal_partitions`;
- `unresolved_partitions`;
- `failed_partitions`;
- `vacancies_found` / unique seen vacancies;
- HH API latency/error panels.

Go / no-go interpretation:

- `succeeded`: baseline contour годится для follow-up completeness study;
- `completed_with_unresolved`: baseline contour ещё требует planner/resume hardening до completeness study;
- `failed`: не идём дальше в backlog slice как в production-like plan, сначала разбираем reliability gap.

## 6. Phase D: Persistent First-Detail Backlog MVP

Это следующий обязательный implementation slice после чистого search baseline.

### 6.1. Product semantics

Нужно реализовать:

- `first_detail_mandatory`
- `refresh_optional`

Priority order:

1. vacancy without successful detail
2. `short_changed`
3. `ttl_refresh`

### 6.2. Minimal MVP shape

Не обязательно сразу Celery.

Достаточный MVP:

1. new selection command for persistent first-detail backlog
2. dedicated drain command
3. operator metrics for backlog size/age/drain rate
4. later integration into scheduler

### 6.3. Concrete runtime changes

1. Add a new terminal/non-retryable detail outcome for `404 not_found`.

Почему:

- иначе backlog будет бессмысленно ретраить permanently gone vacancies.

Minimal preferred shape:

- расширить `DetailFetchStatus` новым terminal status, например `NOT_FOUND`;
- при `404` записывать именно terminal non-retryable outcome;
- backlog selection не должна повторно брать такие vacancies.

2. Add persistent backlog selector not scoped to one `crawl_run`.

Current gap:

- existing `select_detail_candidates` смотрит только на vacancies, наблюдённые в последнем run.

Need:

- новый selector поверх `vacancy_current_state`, не ограниченный `last_seen_run_id`;
- выбирать vacancies с `last_detail_fetched_at is null` или retryable failed status;
- отдельно поддержать optional refresh contour.

3. Add drain command.

Suggested command shape:

```bash
PYTHONPATH=src ./.venv/bin/python -m hhru_platform.interfaces.cli.main drain-first-details \
  --limit <N> \
  --triggered-by first-detail-drain
```

Command semantics:

- берёт верхушку backlog;
- делает detail fetch;
- обновляет `vacancy_current_state`;
- пишет attempts/metrics;
- печатает backlog_before / attempted / succeeded / terminal_not_found / retryable_failed / backlog_after.

4. Add backlog metrics.

Required metrics:

- `first_detail_backlog_open`
- `first_detail_backlog_oldest_age_seconds`
- `first_detail_drain_attempts_total`
- `first_detail_drain_success_total`
- `first_detail_drain_not_found_total`
- `first_detail_drain_retryable_fail_total`

### 6.4. Files likely involved

- [enums.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/domain/value_objects/enums.py)
- [vacancy_current_state_repo.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/infrastructure/db/repositories/vacancy_current_state_repo.py)
- [fetch_vacancy_detail.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/fetch_vacancy_detail.py)
- [select_detail_candidates.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/select_detail_candidates.py)
- [detail.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/interfaces/cli/commands/detail.py)
- [scheduler_loop.py](/home/yurizinyakov/projects/hh_collector/src/hhru_platform/application/commands/scheduler_loop.py)

### 6.5. Tests required

- selector picks missing-detail vacancies regardless of current run id;
- selector excludes terminal `not_found`;
- selector prioritizes `missing_detail > short_changed > ttl_refresh`;
- drain command updates backlog metrics correctly;
- repeated drain converges when backlog is finite.

## 7. Phase E: Serious Completeness Measurements

Только после Phase D.

### 7.1. Measurement program

1. Full sweep on clean DB, `detail-limit=0`
2. Full sweep with persistent first-detail drain enabled
3. Next full sweep on same DB
4. One more full sweep if backlog trend remains unclear

### 7.2. Core metrics

- `new_vacancies_per_full_sweep`
- `first_detail_backlog_before`
- `first_detail_backlog_after`
- `backlog_delta_per_sweep`
- `first_detail_success_rate`
- `first_detail_lag_p50`
- `first_detail_lag_p95`
- `terminal_not_found_before_first_detail`
- impact on search wall-clock and coverage outcomes

### 7.3. Decision rule

Policy is defendable for research completeness if:

- full search sweep closes reliably;
- first-detail backlog does not grow without bound in steady state;
- first-detail lag remains within an agreed window;
- drain contour does not materially destabilize search baseline.

## 8. Recommended Immediate Next Move

Следующий лучший шаг:

1. Доделать два small hardening tasks из current queue.
2. После этого сделать clean-DB full `search-only` baseline run.

То есть не идти прямо сейчас в backlog implementation, не имея честного baseline run по системе в целом.
