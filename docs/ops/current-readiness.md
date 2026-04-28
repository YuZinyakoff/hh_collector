# Current Readiness

Состояние проекта на 2026-04-28 после длинного локального `search-only` baseline run, bounded `first-detail` drain validation и transport hardening slice.

Короткая дорожная карта и текущий порядок работ: [project-status-roadmap.md](/home/yurizinyakov/projects/hh_collector/docs/ops/project-status-roadmap.md).

## Коротко

- Planner completeness blocker закрыт: `area -> time_window` fallback выдержал большой живой run.
- Memory blocker закрыт: длинный run больше не раздувал WSL/collector до аварийного pressure.
- Почти-полный `search-only` baseline фактически доказан:
  - duration: `~13h 33m`
  - run id: `5943c659-cd02-48c6-8296-c4ccbd46be73`
  - unique vacancies: `767451`
  - seen events: `880556`
  - HH requests: `57101`
- Run завершился не из-за внутренней ошибки collector, а на внешнем outage:
  - terminal error: `URLError: [Errno -3] Temporary failure in name resolution`
  - момент: `2026-04-02 14:52 MSK`
- Практически это означает: baseline contour уже жизнеспособен, а следующий blocker теперь не planner/memory, а resilience к transient transport/power/network failures.
- Для этого blocker-а добавлен in-run search transport budget: `run-once-v2` и `resume-run-v2` переочередят failed search partitions после transient transport failure до лимитов `3` consecutive / `5` total.

## Что уже можно считать доказанным

- Stateful search collection на масштабе `high six figures` вакансий принципиально выполнима.
- `run-once-v2` с planner v2 способен пройти почти весь live search tree без `unresolved` веток как системного blocker-а.
- Runtime больше не упирается в прежний локальный memory wall при длинном `search-only` run.
- Текущий корпус уже имеет правильный порядок величины для HH search snapshot-like сбора.
- `resume-run-v2` умеет переочередить `failed` terminal search partitions из `failed` run, то есть единичный transport leaf failure больше не обязан обнулять почти готовый baseline.
- `run-once-v2` теперь также имеет in-run transport budget: search partition с transport failure переочередится без ручного full rerun, пока не исчерпан лимит `3` consecutive / `5` total.
- Начат storage-tiering contour:
  - short snapshot churn снижен до `first_seen/hash_changed`
  - появился local retention archive export
  - housekeeping умеет `archive-before-delete` для `raw_api_payload` и `vacancy_snapshot`
  - появился off-host sync contour для готовых archive chunks через WebDAV + local upload receipts
- Добавлен MVP `first-detail` backlog contour:
  - backlog выводится из `vacancy_current_state`, без новой таблицы
  - есть one-shot команда `drain-first-detail-backlog`
  - есть тонкий `detail_worker` loop для bounded background drain
  - локальный batch `1000` доказал рабочий путь `backlog -> hh detail API -> raw payload/snapshot/state`
  - HTTP 404 detail responses закрываются как `terminal_404`, чтобы не ретраить протухшие вакансии бесконечно
  - добавлены first-detail backlog metrics и alert rules
  - repeated non-terminal detail failures проходят через exponential cooldown перед следующим retry
  - добавлены Grafana panels для open/ready/cooldown backlog и drain outcome mix
  - controlled `detail-worker --once --batch-size 25` после dashboard-слайса прошёл успешно: `24` detail snapshots, `1` terminal_404, `0` retryable failures, `~1.88 req/s`, DB delta `270336 bytes`

## Что ещё не доказано

- Полностью успешный terminal `search-only` baseline без внешнего обрыва.
- Live proof нового run-level transport budget на полном `search-only` baseline.
- `first-detail` backlog на масштабе полного baseline: bounded batch доказан, но полный drain ещё не завершён.
- Многодневная unattended production stability.

## Текущий practical reading

На 2026-04-27 система уже выглядит готовой не к "первой попытке baseline", а к следующему operational этапу:

1. Провести более длинный supervised `detail-worker` run для уточнения throughput и storage growth.
2. Оформить production alert delivery на реальном Telegram receiver.
3. Проверить новый transport budget на VPS `search-only` baseline.
4. Затем снять полный successful baseline report: DB size, backup size, request count, duration, coverage, unique vacancies.
5. После baseline включить supervised `search + detail drain` contour.

## Практический вывод

Базовая жизнеспособность long search collection уже доказана.

Пока ещё рано заявлять "месяцы стабильного unattended production" или "полная completeness доказана". Но уже корректно говорить, что главный архитектурный риск снят, локальный baseline contour работает, и проект готов двигаться из local validation в VPS pilot и operational hardening.

## Смежные документы

- [project-status-roadmap.md](/home/yurizinyakov/projects/hh_collector/docs/ops/project-status-roadmap.md)
- [vps-pilot-checklist.md](/home/yurizinyakov/projects/hh_collector/docs/ops/vps-pilot-checklist.md)
- [first-detail-backlog.md](/home/yurizinyakov/projects/hh_collector/docs/ops/first-detail-backlog.md)
- [hh-api-completeness-implementation-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-implementation-plan.md)
- [hh-api-search-baseline-blocker-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-search-baseline-blocker-plan.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
- [deployment-runbook.md](/home/yurizinyakov/projects/hh_collector/docs/ops/deployment-runbook.md)
