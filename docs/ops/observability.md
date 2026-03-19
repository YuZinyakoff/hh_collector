# Observability Note

Короткая памятка по observability baseline текущего MVP collector.

## Что поднимается

- `metrics` service экспортирует `GET /metrics` и `GET /healthz`
- `prometheus` profile скрейпит `metrics:8001`
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

Поднять baseline вместе с Prometheus:

```bash
make up-observability
```

Проверить endpoint’ы:

```bash
curl http://127.0.0.1:8001/healthz
curl http://127.0.0.1:8001/metrics
curl http://127.0.0.1:9090/-/ready
```

Посмотреть metrics snapshot из app container:

```bash
make compose-show-metrics
```

## Что смотреть

- `hhru_operation_total{operation,status}`
- `hhru_operation_duration_seconds{operation,status}`
- `hhru_operation_last_success_timestamp_seconds{operation}`
- `hhru_records_written_total{operation,record_type}`
- `hhru_upstream_request_total{endpoint,status_class}`
- `hhru_upstream_request_duration_seconds{endpoint,status_class}`

Новый orchestration-lite flow пишет отдельную operation metric:

- `run_collection_once`

## Alert rules baseline

- `HHRUPlatformMetricsEndpointDown`
- `HHRUPlatformOperationFailures`
- `HHRUPlatformNoRecentReconciliation`

## State file

По умолчанию метрики пишутся в `.state/metrics/metrics.json`.
В Compose эта директория монтируется и разделяется между `app` и `metrics`, поэтому counters не теряются между короткими CLI invocations.
