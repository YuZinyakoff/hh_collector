# Current Readiness

Состояние проекта на 2026-06-23 после successful VPS search baseline, полного
pilot first-detail drain, production research archive bootstrap, supervised
daily archive driver smoke, supervised backup systemd smokes и успешного
multi-week unattended storage/archive soak, включая первый guarded weekly S3
backup cleanup timer run.

Детальный статус и порядок работ:
[project-status-roadmap.md](/home/yurizinyakov/projects/hh_collector/docs/ops/project-status-roadmap.md).

## Коротко

Платформа уже находится не на стадии проверки архитектурной жизнеспособности, а
на стадии operational hardening перед unattended production.

Доказано на VPS:

- full `search-only` coverage с `coverage_ratio=1.0000`;
- recovery единичного transient failed partition через targeted resume;
- полный pilot first-detail backlog drain;
- PostgreSQL backup, local verify, local restore drill, S3 upload/verify,
  offsite restore drill, supervised daily/weekly systemd backup drivers и
  `3-7`-дневный unattended daily/weekly soak;
- Telegram alert delivery, Prometheus/Grafana и bounded Prometheus retention;
- canonical production research archive: local/S3 verify, checkpoint coverage
  audit и read-only retention preview;
- local archive analysis smoke: S3 sample прочитан в DataFrame-ready CSV,
  построен PNG и подтверждено наличие полного vacancy detail text;
- полный supervised `daily-research-archive` pipeline без destructive apply;
- guarded weekly S3 backup cleanup timer на VPS: destructive apply выполняется
  только после successful weekly restore drill marker и уже прошёл 2026-06-21.

## Готовность По Контурам

| Контур | Readiness |
| --- | --- |
| Search planner/runtime | VPS validated |
| Detail backlog/worker | supervised pilot catch-up validated |
| PostgreSQL backup/restore | manual end-to-end and daily/weekly systemd drivers VPS validated; unattended daily backup and weekly restore drill soak succeeded |
| S3 backup cleanup | guarded weekly timer VPS validated after successful restore drill |
| Research archive | production bootstrap validated; daily timer enabled; multi-day unattended archive soak succeeded |
| Archive housekeeping preview | production read-only validated |
| Destructive housekeeping | guarded in code, real production apply not yet proven |
| Observability and alert delivery | local non-blocking systemd failure delivery VPS validated; external Telegram transport optional/open |
| Whole-platform unattended routine | not yet validated |

## Что Не Даёт Пока Сказать "Включил И Забыл"

- Daily research archive timer включён 2026-06-04; первый unattended запуск
  2026-06-05 завершился успешно: local verify `1557/1557` manifests, offsite
  verify `1557/1557` manifests и `29` checkpoints, housekeeping preview
  `0` actions. Следующий checked daily run 2026-06-08 тоже завершился успешно:
  local verify `1557/1557`, offsite verify `1557/1557`, `32` checkpoints,
  coverage `complete`, housekeeping preview `0` actions. На 2026-06-23
  daily archive timer продолжает идти штатно: latest run подтвердил
  `1557/1557` manifests, `47` verified checkpoints, coverage `complete` и
  housekeeping preview `0` actions.
- Generic host-side timer failure notifier прошёл VPS smoke: direct notifier и
  systemd template завершились за `22-44 ms` с local queue acceptance. Direct
  Telegram egress с VPS недоступен; внешний transport остаётся optional
  отдельным решением.
- Daily backup и weekly offsite restore drill systemd drivers прошли supervised
  VPS smoke; timers включены 2026-06-05. Первый unattended daily backup
  2026-06-06 завершился успешно: create, local verify, offsite sync и offsite
  verify all succeeded. Следующий checked daily backup 2026-06-08 тоже
  завершился успешно: new dump uploaded, `198` parts and manifest verified as
  `199` offsite objects. Первый unattended weekly restore drill 2026-06-07
  тоже завершился успешно: offsite restore drill и cleanup temporary DB all
  succeeded, lingering restore DB отсутствует. Второй weekly restore drill
  2026-06-14 также прошёл: `198/198` parts downloaded, schema verified
  `5/5`, cleanup succeeded, lingering restore DB отсутствует.
- S3 backup retention apply прошёл 2026-06-15 после dry-run: удалены `7`
  verified generations, `1393` remote objects и `28` local sidecars; контрольный
  dry-run вернул `delete_candidate_count=0`. Остался один fail-safe skipped
  unverified backup generation `20260517`.
- Guarded weekly S3 backup cleanup path теперь доказан на VPS: 2026-06-21
  cleanup стартовал после successful weekly restore drill marker, удалил `5`
  verified generations, `995` remote objects и `20` local sidecars, сохранив
  latest/weekly/milestone generations. Старый unverified backup generation
  `20260517` остаётся fail-safe skipped.
- Production cadence для search/detail не зафиксирована. Текущий
  `scheduler-loop` является interval-based trigger loop, а не готовой weekly
  calendar policy.
- Confirmed gap on 2026-06-23: collection is not merely "not production
  labelled"; it is not running. `scheduler`/`detail-worker` containers are not
  up, and the only `crawl_run` is the May `vps-search-baseline`.
- Supervised recovery search run
  `bcf9ef54-27b0-4a90-bd33-728775053ea4` wrote fresh rows after
  `2026-06-23T12:00:00+00:00`, then failed on an hh.ru HTTP `503`. The code path
  now treats HTTP `5xx` as retryable transport responses; deploy this fix and
  resume that run before starting detail.
- Storage/archive routine прошла `3-7`-дневный unattended soak, но совместная
  работа production search/detail с archive/backup ещё не проверена.
- Текущий VPS corpus остаётся pilot/test evidence. Решение по boundary
  зафиксировано в
  [data-corpus-boundary.md](/home/yurizinyakov/projects/hh_collector/docs/ops/data-corpus-boundary.md):
  перед sustained production collection нужен `corpus_id` / `collection_epoch`
  или clean production start.
- Storage snapshot на 2026-06-23 зафиксирован в
  [current-state-2026-06-23.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-state-2026-06-23.md):
  DB `27 GB`, `.state=63G`, local backups `50G`, expected uploaded backup
  footprint `100.70 GiB`, current live corpus `865868` vacancies, post
  `2026-06-01` production epoch `0` rows because collection has not run since
  the May pilot flow.

## Следующий Рубеж

1. Deploy `5xx` retry fix and resume supervised production search run
   `bcf9ef54-27b0-4a90-bd33-728775053ea4`.
2. Verify terminal search coverage and fresh post-boundary rows.
3. Run detail smoke on fresh rows, then detail catch-up.
4. Зафиксировать календарную production cadence search/detail.
5. Проверить совместную работу production search/detail с archive/backup.
6. Перейти к месячному storage/archive window с alert-driven monitoring и
   недельным operator checklist.

Практический порядок восстановления collection зафиксирован в
[collection-recovery-2026-06-23.md](/home/yurizinyakov/projects/hh_collector/docs/ops/collection-recovery-2026-06-23.md).

## Практический Вывод

Storage architecture и archive safety contour завершены как production-capable
foundation. До режима "редко мониторю несколько месяцев" осталось не
перепроектирование данных, а сборка и доказательство общей unattended routine.

## Смежные Документы

- [project-status-roadmap.md](/home/yurizinyakov/projects/hh_collector/docs/ops/project-status-roadmap.md)
- [research-archive-v1.md](/home/yurizinyakov/projects/hh_collector/docs/ops/research-archive-v1.md)
- [storage-contours.md](/home/yurizinyakov/projects/hh_collector/docs/ops/storage-contours.md)
- [data-corpus-boundary.md](/home/yurizinyakov/projects/hh_collector/docs/ops/data-corpus-boundary.md)
- [current-state-2026-06-23.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-state-2026-06-23.md)
- [collection-recovery-2026-06-23.md](/home/yurizinyakov/projects/hh_collector/docs/ops/collection-recovery-2026-06-23.md)
- [archive-analysis-smoke.md](/home/yurizinyakov/projects/hh_collector/docs/ops/archive-analysis-smoke.md)
- [backup-restore-drill.md](/home/yurizinyakov/projects/hh_collector/docs/ops/backup-restore-drill.md)
- [unattended-operations.md](/home/yurizinyakov/projects/hh_collector/docs/ops/unattended-operations.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
- [observability.md](/home/yurizinyakov/projects/hh_collector/docs/ops/observability.md)
