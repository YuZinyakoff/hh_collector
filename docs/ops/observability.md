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

- `Scheduler / Recovery Health`
- `Collector Overview`
- `HH API / Ingest Health`

Ключевые метрики:

- `hhru_operation_total{operation,status}`
- `hhru_operation_duration_seconds{operation,status}`
- `hhru_operation_last_success_timestamp_seconds{operation}`
- `hhru_records_written_total{operation,record_type}`
- `hhru_run_tree_coverage_ratio{run_id,run_type}`
- `hhru_run_tree_total_partitions{run_id,run_type}`
- `hhru_run_tree_covered_terminal_partitions{run_id,run_type}`
- `hhru_run_tree_pending_terminal_partitions{run_id,run_type}`
- `hhru_run_tree_split_partitions{run_id,run_type}`
- `hhru_run_tree_unresolved_partitions{run_id,run_type}`
- `hhru_run_tree_failed_partitions{run_id,run_type}`
- `hhru_run_terminal_status_total{run_type,status}`
- `hhru_scheduler_tick_total{outcome}`
- `hhru_scheduler_last_tick_timestamp_seconds`
- `hhru_scheduler_last_triggered_run_timestamp_seconds`
- `hhru_scheduler_last_run_finished_timestamp_seconds`
- `hhru_scheduler_last_observed_run_status{status}`
- `hhru_resume_run_v2_attempt_total{run_type,outcome}`
- `hhru_detail_repair_backlog_size{run_id,run_type}`
- `hhru_detail_repair_attempt_total{run_type,outcome}`
- `hhru_detail_repair_repaired_total{run_type}`
- `hhru_detail_repair_still_failing_total{run_type}`
- `hhru_housekeeping_run_total{mode,status}`
- `hhru_housekeeping_last_run_timestamp_seconds`
- `hhru_housekeeping_last_run_status{status}`
- `hhru_housekeeping_last_run_mode{mode}`
- `hhru_housekeeping_last_action_count{target,mode}`
- `hhru_housekeeping_deleted_total{target}`
- `hhru_backup_run_total{status}`
- `hhru_backup_last_success_timestamp_seconds`
- `hhru_restore_drill_run_total{status}`
- `hhru_restore_drill_last_success_timestamp_seconds`
- `hhru_upstream_request_total{endpoint,status_class}`
- `hhru_upstream_request_duration_seconds{endpoint,status_class}`
- recording rules `hhru:scheduler_tick_age_seconds`, `hhru:scheduler_last_triggered_run_age_seconds`, `hhru:coverage_failed_partitions_open`, `hhru:coverage_unresolved_partitions_open`, `hhru:detail_repair_backlog_open`, `hhru:housekeeping_last_run_age_seconds`, `hhru:backup_last_success_age_seconds`, `hhru:restore_drill_last_success_age_seconds`

Новый orchestration-lite flow пишет отдельную operation metric:

- `run_collection_once`

Tree-aware orchestration v2 пишет отдельную operation metric:

- `run_collection_once_v2`
- `resume_run_v2`
- `retry_failed_details`

Planner-v2 execution path пишет отдельные operation metrics:

- `process_partition_v2`
- `run_list_engine_v2`

### Scheduler / Recovery Health

Это первый экран оператора. Он должен отвечать на вопросы:

- жив ли scheduler;
- есть ли открытый coverage debt;
- есть ли зависший repair backlog;
- повторяются ли безуспешные resume attempts.
- не протух ли housekeeping контур.
- есть ли свежий backup и когда последний restore drill.

Показывает:

- `Scheduler Tick Age` и `Last Triggered Run Age` как понятные freshness signals;
- `Open Failed Partitions`, `Open Unresolved Partitions`, `Open Detail Repair Backlog`;
- `Resume Unresolved Again In 12h`;
- debt-tables по run-ам с failed / unresolved / repair backlog;
- outcome tables для scheduler, resume и detail repair;
- `Housekeeping Last Run Age`, `Housekeeping Last Run Status`, `Housekeeping Last Run Mode`, `Housekeeping Deletions In Range`.
- `Backup Last Success Age`, `Backup Runs In Range`, `Restore Drill Last Success Age`, `Restore Drill Runs In Range`.

### Collector Overview

Это второй экран оператора после `Scheduler / Recovery Health`. Показывает:

- totals по `operation/status`
- p95 duration по операциям
- rows written по `record_type`
- last success timestamps
- writes по vacancy, snapshots и reconciliation activity
- `Failures In Range` теперь считает именно число failed operations за выбранный dashboard interval, а не выглядит как накопительный total
- `Planner V2 Coverage By Run` показывает последний lifecycle-published coverage snapshot по `crawl_run`
- coverage block теперь показывает total, covered terminal, pending terminal, split, unresolved и failed counts
- scheduler block теперь показывает overlap skips, active-run skips, last tick, last triggered run, last finished run и terminal status последнего scheduler-admitted run
- recovery block показывает terminal run statuses в range, текущий detail repair backlog, repaired/still failing volume и resume/detail-repair activity

### HH API / Ingest Health

Нужен для быстрой диагностики upstream и ingest degradation. Показывает:

- upstream request totals по `endpoint/status_class`
- upstream latency p95
- failures по `process_list_page`, `fetch_vacancy_detail`, `sync_dictionary`
- error mix по endpoint
- last success timestamps критичных ingest-операций

## Какие dashboard смотреть в первую очередь

1. `Scheduler / Recovery Health`
   Это главный operator dashboard для liveness, coverage debt и repair debt.
2. `Collector Overview`
   Нужен для общего operational контекста, terminal statuses и write activity.
3. `HH API / Ingest Health`
   Открывать, когда health dashboard показывает деградацию и нужно понять, это upstream/API или collector execution.

## Как интерпретировать панели

- `Failures In Range` и `Upstream Errors In Range` теперь показывают total count событий именно за выбранный time range. Это stat panels на базе `increase(...[$__range])` с instant query, поэтому значение не должно выглядеть как бегущий cumulative график.
- `Last Success Timestamps` и `Last Success By Critical Operation` теперь показывают две колонки: `Operation` и `Last Success`. Значение `Last Success` рендерится как обычная дата/время из gauge `hhru_operation_last_success_timestamp_seconds`.
- Если last-success таблица пустая для операции, это означает не "1970", а отсутствие успешного sample для этой операции в текущем metrics state.
- Для planner v2 path `process_partition_v2` success означает обработанный terminal leaf: либо `done + covered`, либо `split_done + split`. Проверять различие нужно по текстовому выводу CLI и по данным в `crawl_partition`, а не только по одному success counter.
- `run_list_engine_v2` success означает, что текущий CLI-проход не встретил failed/unresolved partition results. Полноту tree coverage нужно интерпретировать вместе с `remaining_pending_terminal_partitions` и partition statuses.
- Coverage gauges обновляются не только через `show-run-coverage` / `show-run-tree`, но и автоматически внутри lifecycle points `run-once-v2` и `resume-run-v2`, потому что они используют тот же `report_run_coverage`.
- `hhru_run_tree_total_partitions` и `hhru_run_tree_failed_partitions` читаются как тот же lifecycle snapshot, что и coverage ratio: это не отдельная stored aggregate, а моментальный срез текущего tree state run.
- `run-once-v2` публикует detail repair backlog gauge автоматически: `0` для clean run и `detail_fetch_failed` для terminal `completed_with_detail_errors`.
- `resume-run-v2` публикует тот же coverage snapshot и отдельный `hhru_resume_run_v2_attempt_total{outcome=...}`, поэтому можно видеть не только текущее дерево, но и сколько resume попыток снова закончилось `completed_with_unresolved`.
- Recording rules `hhru:coverage_failed_partitions_open`, `hhru:coverage_unresolved_partitions_open` и `hhru:detail_repair_backlog_open` агрегируют текущий file-backed open debt по всем опубликованным run snapshots. Эти сигналы не "протухают" сами по себе от времени: alert уйдёт только после того, как соответствующий run будет реально дочинен и gauge опустится до `0`.
- `coverage_ratio` нужно читать как долю уже покрытых terminal leaves от текущего множества terminal partitions этого run.
- `split_partitions > 0` само по себе не является ошибкой: это сигнал, что часть coverage делегирована child scopes.
- `unresolved_partitions > 0` означает, что часть дерева не удалось refine'ить текущей split-policy и этот run нельзя считать полностью покрытым.
- `failed_partitions > 0` означает уже не деградацию repair path, а список tree branches, которые list contour не смог обработать; это требует разбирательства в planner/list execution, а не `retry-failed-details`.
- Для `run_collection_once_v2` операторская интерпретация такая:
  `status=succeeded` означает полный list coverage плюс завершённые selective detail и reconcile stages.
  `status=completed_with_detail_errors` означает, что list coverage завершён и run завершён честно, но часть selective detail fetches была неуспешной.
  `status=completed_with_unresolved` означает, что list tree остановился на unresolved scopes и detail/reconcile не запускались.
  `status=failed` означает либо failed partitions в tree, либо критическую orchestration/list ошибку.
- `hhru_operation_total{operation="run_collection_once_v2",status="succeeded"}` включает и чистый success, и `completed_with_detail_errors`; различать их нужно по CLI summary, structured logs и полю `final_status`.
- `hhru_run_terminal_status_total{status=...}` нужен именно для operator-facing run outcomes: он публикуется в `reconcile_run`, `finalize_crawl_run` и status-changing detail repair path и поэтому не смешивает terminal run status с coarse operation success/failure.
- `hhru_operation_total{operation="resume_run_v2",status="succeeded"}` означает, что resume path дошёл до terminal reconciled outcome; детали смотри в fields `initial_run_status`, `unresolved_before_resume`, `resumed_unresolved_partitions`, `final_status`.
- `hhru_operation_total{operation="retry_failed_details",status="failed"}` не означает crash команды автоматически: это нормальный сигнал, что repair backlog после retry всё ещё не пуст. Различать internal crash и remaining backlog нужно по `error_type` и полям `backlog_size`, `repaired_count`, `remaining_backlog_count`.
- Для `completed_with_unresolved` теперь есть operator path:
  `resume-run-v2` пытается снова пройти unresolved branches внутри того же run.
- Для `completed_with_detail_errors` теперь есть отдельный operator path:
  `retry-failed-details` чинит derived backlog без повторного list coverage.
- Scheduler baseline теперь даёт отдельные сигналы:
  `hhru_scheduler_tick_total{outcome=...}` для outcome-level counters,
  `hhru_scheduler_last_tick_timestamp_seconds`,
  `hhru_scheduler_last_triggered_run_timestamp_seconds`,
  `hhru_scheduler_last_run_started_timestamp_seconds`,
  `hhru_scheduler_last_run_finished_timestamp_seconds`,
  `hhru_scheduler_last_observed_run_status{status}` для liveness/timing и outcome visibility.
- `hhru_detail_repair_backlog_size` теперь показывает текущий remaining backlog по run; `hhru_detail_repair_attempt_total`, `hhru_detail_repair_repaired_total` и `hhru_detail_repair_still_failing_total` показывают repair activity без ручного CLI refresh.
- В `Collector Overview` панели `Scheduler Overlap Skips In Range`, `Scheduler Active-Run Skips In Range`, `Scheduler Last Tick`, `Scheduler Last Triggered Run` и `Scheduler Last Observed Run Status` позволяют быстро понять, жив ли loop, не упирается ли он в admission conflicts и чем закончился последний admitted run.
- Для recovery path операторское чтение теперь такое:
  `Detail Repair Backlog > 0` вместе с ростом `Detail Still Failing In Range` означает, что repair contour активен, но backlog ещё не очищен.
  `Resume Attempts In Range > 0` вместе с `Resume Unresolved Again In Range > 0` означает, что unresolved scopes повторно не закрываются и нужен разбор split-policy / upstream / partition shape.
- На `Scheduler / Recovery Health` stat-панели с `Age` уже показывают не timestamp, а возраст сигнала в секундах. Это быстрее для оператора: большой `Tick Age` значит scheduler stale, большой `Last Triggered Run Age` при живом tick обычно означает stuck admission или stuck active run.
- Таблицы `Runs With Failed Partitions`, `Runs With Unresolved Partitions` и `Runs With Detail Repair Backlog` показывают именно текущий open debt: если run остаётся в таблице, значит operator continuation для него ещё не завершён.
- Housekeeping нужно читать отдельно от collector failures:
  `Housekeeping Last Run Age` показывает freshness housekeeping contour,
  `Housekeeping Last Run Status` и `Housekeeping Last Run Mode` показывают, был ли последний прогон dry-run или execute,
  `Housekeeping Deletions In Range` показывает реальные cleanup actions по target type.
- `hhru_housekeeping_last_action_count{mode="dry_run"}` полезен как preview-signal: это не удаление, а последний dry-run plan per target.
- Backup / restore drill нужно читать как отдельный safety contour:
  `Backup Last Success Age` показывает, насколько свежий последний реально успешный backup.
  `Backup Runs In Range` показывает, есть ли недавние failed backup attempts.
  `Restore Drill Last Success Age` показывает, насколько давно последний раз проверялся restore path.
  `Restore Drill Runs In Range` показывает, был ли сам drill успешным или только падал.

## Alert rules baseline

- `HHRUPlatformMetricsEndpointDown`
- `HHRUPlatformOperationFailures`
- `HHRUPlatformNoRecentReconciliation`
- `HHRUPlatformSchedulerTickStale`
- `HHRUPlatformSchedulerTriggeredRunStale`
- `HHRUPlatformFailedPartitionsPresent`
- `HHRUPlatformUnresolvedPartitionsStuck`
- `HHRUPlatformDetailRepairBacklogStuck`
- `HHRUPlatformResumeUnresolvedRepeatedly`
- `HHRUPlatformHousekeepingStale`
- `HHRUPlatformBackupStale`

## Что считать тревожным

- рост `hhru_operation_total{status="failed"}` для `process_list_page`, `fetch_vacancy_detail`, `sync_dictionary`
- рост `hhru_operation_total{status="failed"}` для `process_partition_v2` и `run_list_engine_v2`
- устойчивый `hhru_run_tree_failed_partitions > 0`
- устойчивый `hhru_run_tree_unresolved_partitions > 0` после нескольких `hhru_resume_run_v2_attempt_total`
- рост `hhru_run_terminal_status_total{status="completed_with_detail_errors"}` или `...{status="failed"}` в выбранном интервале
- `hhru_detail_repair_backlog_size > 0` без снижения, особенно если одновременно растёт `hhru_detail_repair_still_failing_total`
- stale `hhru_scheduler_last_tick_timestamp_seconds` или рост `hhru_scheduler_tick_total{outcome="skipped_overlap"}` / `...{outcome="skipped_active_run"}`
- sustained `4xx`/`5xx`/`timeout`/`network_error` на dashboard `HH API / Ingest Health`
- отсутствие свежего `reconcile_run` success timestamp
- заметное падение `records_written_total` при том, что run-операции продолжают стартовать
- любое состояние, в котором open-debt панели на `Scheduler / Recovery Health` остаются ненулевыми дольше ожидаемого operator window: это означает не просто historical факт, а незакрытый operational долг по конкретным run-ам
- `hhru:housekeeping_last_run_age_seconds` уходит за неделю или `Housekeeping Deletions In Range` долго остаётся нулевым при явно растущих data volumes
- `hhru:backup_last_success_age_seconds` уходит за 72 часа или `Backup Runs In Range` показывает repeated failures

## Как действовать по алертам

- `HHRUPlatformMetricsEndpointDown`
  Проверить `metrics` service, `GET /healthz`, состояние shared metrics state volume и Compose/network.
- `HHRUPlatformOperationFailures`
  Открыть `Collector Overview` и `HH API / Ingest Health`, посмотреть какой operation падает и это upstream, schema/problem или execution bug.
- `HHRUPlatformNoRecentReconciliation`
  Проверить, запускаются ли вообще terminal runs и не застряли ли они до `reconcile_run`; при необходимости вручную пройти `run-once-v2` / scheduler flow.
- `HHRUPlatformSchedulerTickStale`
  Проверить `scheduler-loop` process/container, последние structured logs и не остановился ли whole worker; до ручного trigger важно понять причину остановки.
- `HHRUPlatformSchedulerTriggeredRunStale`
  Если ticks свежие, но runs давно не стартовали, проверить `Scheduler Active-Run Skips In Range`, active `crawl_run` и admission conflicts; если active run stuck, сначала разбирать его.
- `HHRUPlatformFailedPartitionsPresent`
  Не запускать blind retries вслепую. Открыть `Runs With Failed Partitions`, затем CLI/reporting для конкретного run и разбирать planner/list failure root cause.
- `HHRUPlatformUnresolvedPartitionsStuck`
  Открыть `Runs With Unresolved Partitions`, выбрать конкретный `run_id`, для repairable run выполнить `resume-run-v2`; если alert быстро возвращается, разбирать split-policy / upstream shape вместо повторных resume.
- `HHRUPlatformDetailRepairBacklogStuck`
  Открыть `Runs With Detail Repair Backlog`, выбрать конкретный `run_id`, выполнить `retry-failed-details` и проверить конкретные вакансии, которые продолжают падать.
- `HHRUPlatformResumeUnresolvedRepeatedly`
  Считать это сигналом, что automatic operator retry исчерпал пользу. Нужно остановить blind resume loop и отдельно исследовать unresolved scopes и planner refinement path.
- `HHRUPlatformHousekeepingStale`
  Сначала запустить `run-housekeeping` без `--execute`, проверить per-target plan и guardrails, затем повторить с `--execute`, если summary выглядит ожидаемо и не затрагивает лишние данные.
- `HHRUPlatformBackupStale`
  Запустить `run-backup`, затем `verify-backup-file`. Если backup всё ещё падает, разбирать PostgreSQL connectivity, disk space и backup/restore tooling до следующего unattended run.

## State file

По умолчанию метрики пишутся в `.state/metrics/metrics.json`.
В Compose эта директория монтируется и разделяется между `app` и `metrics`, поэтому counters не теряются между короткими CLI invocations.
