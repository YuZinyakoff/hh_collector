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
- `hhru_first_detail_ready_backlog_size{scope}` для items, которые можно брать прямо сейчас.
- `hhru_first_detail_cooldown_backlog_size{scope}` для retryable failures, временно пропущенных cooldown-ом.
- `hhru_first_detail_drain_attempt_total{scope,outcome}`.
- `hhru_first_detail_drain_selected_total{scope}`.
- `hhru_first_detail_drain_succeeded_total{scope}`.
- `hhru_first_detail_drain_terminal_total{scope}`.
- `hhru_first_detail_drain_failed_total{scope}`.

Для alerting важно разделять `terminal` и `failed`: рост `terminal` ожидаем для протухших вакансий, рост `failed` означает retryable/non-terminal проблемы.

## Current Limits

- Это MVP без Redis queue: admission идёт через deterministic DB selection.
- Есть first-detail backlog metrics, alert rules и Grafana panels.
- Есть per-vacancy exponential cooldown после repeated non-terminal failures.
- HTTP 404 detail responses закрываются как `terminal_404`.
- Нет отдельной политики для archived/inactive vacancies кроме исключения `is_probably_inactive=false` по умолчанию.

Следующий hardening slice: production alert delivery и более длинный supervised `detail-worker` run для уточнения throughput/storage growth.
