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
- `selected_count`
- `detail_fetch_succeeded`
- `detail_fetch_terminal`
- `detail_fetch_failed`
- `backlog_size_after`

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

## Metrics

`drain-first-detail-backlog` и `detail_worker` публикуют:

- `hhru_first_detail_backlog_size{scope="active"}` для стандартного режима.
- `hhru_first_detail_backlog_size{scope="all"}` для режима `--include-inactive yes`.
- `hhru_first_detail_drain_attempt_total{scope,outcome}`.
- `hhru_first_detail_drain_selected_total{scope}`.
- `hhru_first_detail_drain_succeeded_total{scope}`.
- `hhru_first_detail_drain_terminal_total{scope}`.
- `hhru_first_detail_drain_failed_total{scope}`.

Для alerting важно разделять `terminal` и `failed`: рост `terminal` ожидаем для протухших вакансий, рост `failed` означает retryable/non-terminal проблемы.

## Current Limits

- Это MVP без Redis queue: admission идёт через deterministic DB selection.
- Есть базовые first-detail backlog metrics и alert rules; dashboard panels ещё нужно добавить в monitoring assets.
- Нет per-vacancy cooldown после repeated non-terminal failures.
- HTTP 404 detail responses закрываются как `terminal_404`.
- Нет отдельной политики для archived/inactive vacancies кроме исключения `is_probably_inactive=false` по умолчанию.

Следующий hardening slice: metrics/alerts для first-detail backlog и retry cooldown для repeated non-terminal failures.
