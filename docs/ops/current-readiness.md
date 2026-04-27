# Current Readiness

Состояние проекта на 2026-04-27 после длинного локального `search-only` baseline run и bounded `first-detail` drain validation.

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

## Что уже можно считать доказанным

- Stateful search collection на масштабе `high six figures` вакансий принципиально выполнима.
- `run-once-v2` с planner v2 способен пройти почти весь live search tree без `unresolved` веток как системного blocker-а.
- Runtime больше не упирается в прежний локальный memory wall при длинном `search-only` run.
- Текущий корпус уже имеет правильный порядок величины для HH search snapshot-like сбора.
- `resume-run-v2` теперь умеет переочередить `failed` terminal search partitions из `failed` run, то есть единичный transport leaf failure больше не обязан обнулять почти готовый baseline.
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

## Что ещё не доказано

- Полностью успешный terminal `search-only` baseline без внешнего обрыва.
- Run-level resilience к transient transport/DNS outage без потери почти готового baseline.
- Автоматический bounded run-level retry budget для repeated transport failures; сейчас есть operator recovery path, но не полный self-healing contour.
- `first-detail` backlog на масштабе полного baseline: bounded batch доказан, но полный drain ещё не завершён.
- Dashboard panels для first-detail backlog metrics.
- Cooldown/backoff для repeated non-terminal detail failures.
- Многодневная unattended production stability.

## Текущий practical reading

На 2026-04-03 система уже выглядит готовой не к "первой попытке baseline", а к следующему operational этапу:

1. Довести локальный first-detail contour до наблюдаемого `detail-worker` run с понятной скоростью и storage growth.
2. Добавить dashboard panels для first-detail backlog.
3. Transport/resume hardening, чтобы не терять почти завершённый run из-за единичного outage.
4. Затем VPS pilot на более стабильном хосте и полный `search-only` baseline.
5. После baseline включить supervised `search + detail drain` contour.

## Практический вывод

Базовая жизнеспособность long search collection уже доказана.

Пока ещё рано заявлять "месяцы стабильного unattended production" или "полная completeness доказана". Но уже корректно говорить, что главный архитектурный риск снят, локальный baseline contour работает, и проект готов двигаться из local validation в VPS pilot и operational hardening.

## Смежные документы

- [vps-pilot-checklist.md](/home/yurizinyakov/projects/hh_collector/docs/ops/vps-pilot-checklist.md)
- [first-detail-backlog.md](/home/yurizinyakov/projects/hh_collector/docs/ops/first-detail-backlog.md)
- [hh-api-completeness-implementation-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-implementation-plan.md)
- [hh-api-search-baseline-blocker-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-search-baseline-blocker-plan.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
- [deployment-runbook.md](/home/yurizinyakov/projects/hh_collector/docs/ops/deployment-runbook.md)
