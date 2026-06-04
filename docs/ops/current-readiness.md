# Current Readiness

Состояние проекта на 2026-06-04 после successful VPS search baseline, полного
pilot first-detail drain, production research archive bootstrap и supervised
daily archive driver smoke.

Детальный статус и порядок работ:
[project-status-roadmap.md](/home/yurizinyakov/projects/hh_collector/docs/ops/project-status-roadmap.md).

## Коротко

Платформа уже находится не на стадии проверки архитектурной жизнеспособности, а
на стадии operational hardening перед unattended production.

Доказано на VPS:

- full `search-only` coverage с `coverage_ratio=1.0000`;
- recovery единичного transient failed partition через targeted resume;
- полный pilot first-detail backlog drain;
- PostgreSQL backup, local verify, local restore drill, S3 upload/verify и
  offsite restore drill;
- Telegram alert delivery, Prometheus/Grafana и bounded Prometheus retention;
- canonical production research archive: local/S3 verify, checkpoint coverage
  audit и read-only retention preview;
- полный supervised `daily-research-archive` pipeline без destructive apply.

## Готовность По Контурам

| Контур | Readiness |
| --- | --- |
| Search planner/runtime | VPS validated |
| Detail backlog/worker | supervised pilot catch-up validated |
| PostgreSQL backup/restore | manual end-to-end VPS validated; unattended drivers code-ready, VPS smoke pending |
| Research archive | production bootstrap validated; daily timer enabled, first unattended run pending |
| Archive housekeeping preview | production read-only validated |
| Destructive housekeeping | guarded in code, real production apply not yet proven |
| Observability and alert delivery | foundation validated; generic systemd failure notifier code-ready, VPS smoke pending |
| Whole-platform unattended routine | not yet validated |

## Что Не Даёт Пока Сказать "Включил И Забыл"

- Daily research archive timer включён 2026-06-04; первый unattended запуск и
  несколько последующих успешных запусков ещё нужно наблюдать.
- Generic host-side timer failure notifier реализован, но non-blocking local
  acceptance smoke на VPS ещё не выполнен. Direct Telegram egress с VPS
  недоступен; внешний transport остаётся optional отдельным решением.
- Daily backup и weekly offsite restore drivers/timers реализованы, но сначала
  должны пройти supervised VPS smoke и только затем быть включены.
- S3 backup retention apply остаётся manual/dry-run-first, поэтому remote
  storage ещё не ограничен для многомесячного режима.
- Production cadence для search/detail не зафиксирована. Текущий
  `scheduler-loop` является interval-based trigger loop, а не готовой weekly
  calendar policy.
- Совместная работа search, detail, archive и backup не прошла `3-7`-дневный
  unattended soak.
- Текущий VPS corpus остаётся pilot/test evidence. Нужна явная стратегия
  clean production start или маркированного продолжения истории.

## Следующий Рубеж

1. Проверить первый unattended daily research archive timer run.
2. Выполнить synthetic failure-notification smoke на VPS.
3. Выполнить supervised daily backup и weekly offsite restore drill smoke,
   затем включить их timers.
4. Зафиксировать календарную production cadence search/detail.
5. Доказать bounded S3 backup retention apply на реальном безопасном кандидате.
6. Провести `3-7` дней supervised unattended soak.
7. После successful soak перейти к месячному окну с alert-driven monitoring и
   недельным operator checklist.

## Практический Вывод

Storage architecture и archive safety contour завершены как production-capable
foundation. До режима "редко мониторю несколько месяцев" осталось не
перепроектирование данных, а сборка и доказательство общей unattended routine.

## Смежные Документы

- [project-status-roadmap.md](/home/yurizinyakov/projects/hh_collector/docs/ops/project-status-roadmap.md)
- [research-archive-v1.md](/home/yurizinyakov/projects/hh_collector/docs/ops/research-archive-v1.md)
- [storage-contours.md](/home/yurizinyakov/projects/hh_collector/docs/ops/storage-contours.md)
- [backup-restore-drill.md](/home/yurizinyakov/projects/hh_collector/docs/ops/backup-restore-drill.md)
- [unattended-operations.md](/home/yurizinyakov/projects/hh_collector/docs/ops/unattended-operations.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
- [observability.md](/home/yurizinyakov/projects/hh_collector/docs/ops/observability.md)
