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
- `hhru_upstream_request_total{endpoint,status_class}`
- `hhru_upstream_request_duration_seconds{endpoint,status_class}`

Новый orchestration-lite flow пишет отдельную operation metric:

- `run_collection_once`

### Collector Overview

Полезен как первый экран оператора. Показывает:

- totals по `operation/status`
- p95 duration по операциям
- rows written по `record_type`
- last success timestamps
- writes по vacancy, snapshots и reconciliation activity

### HH API / Ingest Health

Нужен для быстрой диагностики upstream и ingest degradation. Показывает:

- upstream request totals по `endpoint/status_class`
- upstream latency p95
- failures по `process_list_page`, `fetch_vacancy_detail`, `sync_dictionary`
- error mix по endpoint
- last success timestamps критичных ingest-операций

## Alert rules baseline

- `HHRUPlatformMetricsEndpointDown`
- `HHRUPlatformOperationFailures`
- `HHRUPlatformNoRecentReconciliation`

## Что считать тревожным

- рост `hhru_operation_total{status="failed"}` для `process_list_page`, `fetch_vacancy_detail`, `sync_dictionary`
- sustained `4xx`/`5xx`/`timeout`/`network_error` на dashboard `HH API / Ingest Health`
- отсутствие свежего `reconcile_run` success timestamp
- заметное падение `records_written_total` при том, что run-операции продолжают стартовать

## State file

По умолчанию метрики пишутся в `.state/metrics/metrics.json`.
В Compose эта директория монтируется и разделяется между `app` и `metrics`, поэтому counters не теряются между короткими CLI invocations.
