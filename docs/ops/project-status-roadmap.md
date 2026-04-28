# Project Status And Roadmap

Дата среза: 2026-04-27.

Этот документ является короткой точкой входа после перерывов между сессиями. Детальные runbook-и остаются в соседних ops-документах, но текущий статус и следующий порядок работ фиксируются здесь.

## 1. Где Мы Сейчас

Проект находится между local validation и VPS pilot.

Уже доказано:

- stateful модель сбора работает на масштабе `high six figures` вакансий;
- planner v2 с `area -> time_window` fallback прошёл near-complete live `search-only` baseline;
- прежний WSL memory blocker снят;
- `search-only` baseline фактически показал порядок `~767k` unique vacancies и `~880k` seen events за `~13h 33m`;
- остановка baseline 2026-04-02 была внешней: `URLError: [Errno -3] Temporary failure in name resolution`, а не planner/memory/internal crash;
- `resume-run-v2` умеет переочередить failed terminal search partitions;
- backup/verify/restore-drill path есть;
- retention archive и WebDAV offsite sync проверены на Yandex Disk;
- first-detail backlog MVP реализован: backlog selector, drain command, `detail_worker`, terminal `404`, retry cooldown, metrics, alerts, Grafana panels;
- controlled first-detail worker tick прошёл чисто: `24` successful detail snapshots, `1` terminal_404, `0` retryable failures.

Текущий статус не равен production readiness. Корректная формулировка: базовая жизнеспособность search collection и first-detail backlog contour доказана, но полный production operating mode ещё не доказан.

## 2. Что Ещё Не Доказано

- Полностью successful terminal `search-only` baseline без внешнего outage.
- Автоматический run-level transport budget `3 consecutive / 5 total`; сейчас есть bounded request retries и operator recovery, но не полный self-healing run state machine.
- Полный first-detail drain на масштабе baseline.
- Sustained detail throughput/storage growth на длинном supervised run.
- Production alert delivery, а не только metrics/rules/dashboards.
- Многодневная unattended stability на VPS.
- Месячный production режим с backup, housekeeping, offsite archive и operator routine.

## 3. Модули По Готовности

| Контур | Статус | Комментарий |
| --- | --- | --- |
| DB schema / migrations | ready for MVP | core operational tables есть, миграции и tests покрывают основной путь |
| Search planner v2 | locally validated | near-complete baseline снял planner completeness blocker |
| Search runtime | viable, needs hardening | нужен run-level transport budget и clean terminal baseline |
| Resume failed search run | MVP ready | умеет продолжать failed terminal search branches |
| Detail same-run budget | ready as bounded contour | не является completeness guarantee |
| First-detail backlog | MVP ready | нужен длинный drain measurement |
| Detail worker | MVP ready | есть one-shot и loop, пока без full-scale unattended proof |
| Backup / restore drill | ready for pilot | path реализован и документирован |
| Retention archive / offsite sync | ready for pilot | WebDAV path проверен, нужен production routine |
| Observability | foundation ready | metrics, dashboards, alert rules есть |
| Alert delivery | foundation ready | Alertmanager + webhook receiver добавлены; на VPS нужно настроить Telegram credentials и synthetic test |
| VPS deploy | documented, not executed here | первый VPS pilot должен быть search-only |
| Research enrichment | intentionally out of scope | не начинать до стабилизации collection layer |

## 4. Следующий Порядок Работ

### Stage 1. Закрыть локальную операционную ясность

1. Провести более длинный supervised `detail-worker` run.
2. Снять detail throughput, retryable failure mix, terminal_404 долю и DB growth.
3. Зафиксировать estimate для cold-start drain и steady-state weekly operation.

Команда для controlled local measurement:

```bash
BATCH_SIZE=100 MAX_TICKS=1 make detail-worker-measurement
```

Первый measurement 2026-04-28 уже прошёл clean:

- `100/100` successful details;
- `0` terminal_404;
- `0` retryable failures;
- active backlog `766389 -> 766289`;
- DB delta `2277376 bytes`, примерно `22.8 KB` на selected item;
- artifact: `.state/reports/detail-worker-measurement/20260428T111507Z/summary.md`.

Go/no-go:

- retryable failures не растут неконтролируемо;
- cooldown backlog не накапливается неожиданно;
- storage growth попадает в ожидаемый порядок;
- worker можно безопасно остановить и продолжить.

### Stage 2. Production alert delivery

1. Настроить Telegram bot token и chat id в VPS `.env`.
2. Проверить хотя бы один synthetic alert через `alert-webhook`.
3. Проверить один реальный Prometheus -> Alertmanager -> webhook route.
4. Документировать operator action для scheduler stale, failed partitions, backup stale, first-detail failures.

Status на 2026-04-28:

- Alertmanager route добавлен.
- `alert-webhook` receiver добавлен.
- Без Telegram credentials receiver логирует payloads.
- С Telegram credentials receiver отправляет сообщения в Telegram.

Go/no-go:

- alerts реально доходят вне Grafana UI;
- alert message даёт run id / scope / следующий operator action.

### Stage 3. Transport / resume hardening

1. Довести search transport budget до policy target `3 consecutive / 5 total`.
2. Уточнить terminal status mapping для `completed_with_unresolved` и `completed_with_detail_errors`.
3. Не превращать transient DNS/network outage в потерю почти готового baseline.

Go/no-go:

- единичный outage не требует full rerun;
- operator summary объясняет, что именно произошло;
- targeted resume path остаётся штатным.

### Stage 4. VPS search-only pilot

1. Взять VPS не как final production, а как стабильный host для first successful terminal baseline.
2. Запустить `search-only` baseline с `detail-limit=0`.
3. После успешного run снять DB size, backup size, request count, duration, coverage, unique vacancies.

Go/no-go:

- terminal status `succeeded`;
- backup/verify/restore-drill после baseline успешны;
- offsite archive path не падает.

### Stage 5. VPS supervised detail drain

1. Включить bounded first-detail drain отдельно от baseline.
2. Расширять batch/interval только после измерения storage и failure mix.
3. Считать first-detail backlog trend.

Go/no-go:

- first-detail backlog убывает быстрее, чем растёт;
- search baseline не деградирует из-за detail drain;
- alert delivery уже включена.

### Stage 6. Week / Month unattended

1. Провести supervised week.
2. Проверить backup, housekeeping, offsite sync, alerts, disk growth.
3. Только после этого переходить к месячному unattended окну.

## 5. Практический Вывод

Проект уже прошёл главный feasibility risk: собрать near-snapshot hh.ru search-space в рамках одного длинного sweep принципиально возможно.

Следующий риск уже не архитектурный, а операционный: устойчивость к внешним outage, понятная recovery semantics, storage planning, alert delivery и proof, что first-detail backlog можно дренировать без деградации search contour.

Связанные документы:

- [current-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-readiness.md)
- [vps-pilot-checklist.md](/home/yurizinyakov/projects/hh_collector/docs/ops/vps-pilot-checklist.md)
- [first-detail-backlog.md](/home/yurizinyakov/projects/hh_collector/docs/ops/first-detail-backlog.md)
- [hh-api-completeness-implementation-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-implementation-plan.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
