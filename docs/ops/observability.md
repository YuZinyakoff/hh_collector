# Observability Note

Короткая памятка по observability baseline текущего MVP collector.

## Что поднимается

- `metrics` service экспортирует `GET /metrics` и `GET /healthz`
- `prometheus` profile скрейпит `metrics:8001`
- `grafana` profile автоматически подключает Prometheus datasource и dashboards из репозитория
- приложение пишет JSON structured logs в stderr
- file-backed metrics сохраняются в `HHRU_METRICS_STATE_PATH`

## Основные команды

Локальный snapshot метрик:

```bash
make show-metrics
```

Поднять Compose baseline с metrics endpoint:

```bash
make up
```

Поднять baseline вместе с Prometheus и Grafana:

```bash
make up-observability
```

Проверить endpoint’ы:

```bash
curl http://127.0.0.1:8001/healthz
curl http://127.0.0.1:8001/metrics
curl http://127.0.0.1:9090/-/ready
```

Открыть UI:

- Grafana: `http://127.0.0.1:3000`
- Prometheus: `http://127.0.0.1:9090`

Локальные default credentials для Grafana берутся из `.env`:

- `HHRU_GRAFANA_ADMIN_USER`
- `HHRU_GRAFANA_ADMIN_PASSWORD`

Посмотреть metrics snapshot из app container:

```bash
make compose-show-metrics
```

## Что смотреть

Provisioned dashboards:

- `Collector Overview`
- `HH API / Ingest Health`

Ключевые метрики:

- `hhru_operation_total{operation,status}`
- `hhru_operation_duration_seconds{operation,status}`
- `hhru_operation_last_success_timestamp_seconds{operation}`
- `hhru_records_written_total{operation,record_type}`
- `hhru_run_tree_coverage_ratio{run_id,run_type}`
- `hhru_run_tree_covered_terminal_partitions{run_id,run_type}`
- `hhru_run_tree_pending_terminal_partitions{run_id,run_type}`
- `hhru_run_tree_split_partitions{run_id,run_type}`
- `hhru_run_tree_unresolved_partitions{run_id,run_type}`
- `hhru_upstream_request_total{endpoint,status_class}`
- `hhru_upstream_request_duration_seconds{endpoint,status_class}`

Новый orchestration-lite flow пишет отдельную operation metric:

- `run_collection_once`

Tree-aware orchestration v2 пишет отдельную operation metric:

- `run_collection_once_v2`

Planner-v2 execution path пишет отдельные operation metrics:

- `process_partition_v2`
- `run_list_engine_v2`

### Collector Overview

Полезен как первый экран оператора. Показывает:

- totals по `operation/status`
- p95 duration по операциям
- rows written по `record_type`
- last success timestamps
- writes по vacancy, snapshots и reconciliation activity
- `Failures In Range` теперь считает именно число failed operations за выбранный dashboard interval, а не выглядит как накопительный total
- `Planner V2 Coverage By Run` показывает последний reporting snapshot coverage ratio по `crawl_run`
- отдельные stat panels показывают covered terminal, pending terminal, split и unresolved counts

### HH API / Ingest Health

Нужен для быстрой диагностики upstream и ingest degradation. Показывает:

- upstream request totals по `endpoint/status_class`
- upstream latency p95
- failures по `process_list_page`, `fetch_vacancy_detail`, `sync_dictionary`
- error mix по endpoint
- last success timestamps критичных ingest-операций

## Как интерпретировать панели

- `Failures In Range` и `Upstream Errors In Range` теперь показывают total count событий именно за выбранный time range. Это stat panels на базе `increase(...[$__range])` с instant query, поэтому значение не должно выглядеть как бегущий cumulative график.
- `Last Success Timestamps` и `Last Success By Critical Operation` теперь показывают две колонки: `Operation` и `Last Success`. Значение `Last Success` рендерится как обычная дата/время из gauge `hhru_operation_last_success_timestamp_seconds`.
- Если last-success таблица пустая для операции, это означает не "1970", а отсутствие успешного sample для этой операции в текущем metrics state.
- Для planner v2 path `process_partition_v2` success означает обработанный terminal leaf: либо `done + covered`, либо `split_done + split`. Проверять различие нужно по текстовому выводу CLI и по данным в `crawl_partition`, а не только по одному success counter.
- `run_list_engine_v2` success означает, что текущий CLI-проход не встретил failed/unresolved partition results. Полноту tree coverage нужно интерпретировать вместе с `remaining_pending_terminal_partitions` и partition statuses.
- Coverage gauges обновляются командами `show-run-coverage` и `show-run-tree`: они считают tree state для конкретного `crawl_run` и публикуют текущий snapshot в file-backed metrics registry.
- `run-once-v2` использует тот же coverage reporting внутри orchestration loop, поэтому после каждого операционного прохода summary и gauges можно читать без дополнительного SQL.
- `coverage_ratio` нужно читать как долю уже покрытых terminal leaves от текущего множества terminal partitions этого run.
- `split_partitions > 0` само по себе не является ошибкой: это сигнал, что часть coverage делегирована child scopes.
- `unresolved_partitions > 0` означает, что часть дерева не удалось refine'ить текущей split-policy и этот run нельзя считать полностью покрытым.
- Для `run_collection_once_v2` операторская интерпретация такая:
  `status=succeeded` означает полный list coverage плюс завершённые selective detail и reconcile stages.
  `status=completed_with_unresolved` означает, что list tree остановился на unresolved scopes и detail/reconcile не запускались.
  `status=failed` означает либо failed partitions в tree, либо ошибку orchestration step, либо detail stage с неуспешными fetches.

## Alert rules baseline

- `HHRUPlatformMetricsEndpointDown`
- `HHRUPlatformOperationFailures`
- `HHRUPlatformNoRecentReconciliation`

## Что считать тревожным

- рост `hhru_operation_total{status="failed"}` для `process_list_page`, `fetch_vacancy_detail`, `sync_dictionary`
- рост `hhru_operation_total{status="failed"}` для `process_partition_v2` и `run_list_engine_v2`
- sustained `4xx`/`5xx`/`timeout`/`network_error` на dashboard `HH API / Ingest Health`
- отсутствие свежего `reconcile_run` success timestamp
- заметное падение `records_written_total` при том, что run-операции продолжают стартовать

## State file

По умолчанию метрики пишутся в `.state/metrics/metrics.json`.
В Compose эта директория монтируется и разделяется между `app` и `metrics`, поэтому counters не теряются между короткими CLI invocations.
