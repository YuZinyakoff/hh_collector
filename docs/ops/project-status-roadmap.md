# Project Status And Roadmap

Дата среза: 2026-05-15.

Этот документ является короткой точкой входа после перерывов между сессиями. Детальные runbook-и остаются в соседних ops-документах, но текущий статус и следующий порядок работ фиксируются здесь.

## 1. Где Мы Сейчас

Проект находится после successful VPS `search-only` baseline и перед первым VPS `first-detail` measurement.

Уже доказано:

- stateful модель сбора работает на масштабе `high six figures` вакансий;
- planner v2 с `area -> time_window` fallback прошёл full VPS `search-only` baseline;
- прежний WSL memory blocker снят;
- VPS baseline `c7e7d8c6-6813-454c-845e-ca44539da1e8` завершён со статусом `succeeded`, `coverage_ratio=1.0000`, `covered_terminal_partitions=16108/16108`;
- baseline дал `865868` unique vacancies, `2309443` seen events, `872201` short snapshots и `129008` raw payload rows;
- один transient HH `502` был восстановлен через targeted resume: `resumed_failed_partitions=1`, итоговых failed partitions `0`;
- `resume-run-v2` умеет переочередить failed terminal search partitions;
- backup/verify/restore-drill path есть;
- post-baseline backup/verify/restore-drill на VPS прошёл успешно; backup `.state/backups/hhru-platform_hhru_platform_20260515T084422Z.dump`, `1596355292` bytes, sha256 `192cd44693f49bbdcf76832a011b1648b0b5ed1ff748a912eb0c324cb27513cf`;
- retention archive и WebDAV offsite sync проверены на Yandex Disk;
- first-detail backlog MVP реализован: backlog selector, drain command, `detail_worker`, terminal `404`, retry cooldown, metrics, alerts, Grafana panels;
- controlled first-detail worker tick прошёл чисто: `24` successful detail snapshots, `1` terminal_404, `0` retryable failures;
- local detail-worker measurement `100` items прошёл clean: `100/100` successful details, active backlog `766389 -> 766289`, DB delta около `2.28 MB`;
- Alertmanager + `alert-webhook` delivery до Telegram проверены на VPS;
- in-run search transport budget добавлен для `run-once-v2` и `resume-run-v2`: transient failed search partitions переочередятся до лимитов `3` consecutive / `5` total.

Текущий статус не равен production readiness. Корректная формулировка: full search coverage operationally validated, но `first-detail` drain, backup offsite для DB dumps и unattended production routine ещё не доказаны.

## 2. Что Ещё Не Доказано

- Полный first-detail drain на масштабе baseline.
- Sustained detail throughput/storage growth на длинном supervised run.
- Production-quality Telegram alert payloads: текущие alerts доходят, но мало объясняют причину и scope.
- Offsite sync именно свежих DB backups: retention archive offsite работает, но backup `.dump` пока остаётся локальным артефактом VPS.
- Многодневная unattended stability на VPS.
- Месячный production режим с backup, housekeeping, offsite archive и operator routine.

## 3. Модули По Готовности

| Контур | Статус | Комментарий |
| --- | --- | --- |
| DB schema / migrations | ready for MVP | core operational tables есть, миграции и tests покрывают основной путь |
| Search planner v2 | VPS validated | full VPS baseline снял planner completeness blocker |
| Search runtime | validated, partially hardened | search-only baseline successful; HH `502` показал gap в 5xx retry/classification |
| Resume failed search run | MVP ready | умеет продолжать failed terminal search branches |
| Detail same-run budget | ready as bounded contour | не является completeness guarantee |
| First-detail backlog | MVP ready | следующий шаг: VPS bounded measurement |
| Detail worker | MVP ready | есть one-shot и loop, пока без full-scale unattended proof |
| Backup / restore drill | VPS validated | post-baseline backup, verify и restore-drill прошли |
| Retention archive / offsite sync | partially validated | retention bundle sync работает; DB backup offsite ещё нужно добавить/проверить |
| Observability | foundation ready | metrics, dashboards, alert rules есть |
| Alert delivery | foundation ready | delivery до Telegram проверен; payloads нужно сделать информативнее |
| VPS deploy | validated | search-only pilot completed on Timeweb VPS |
| Research enrichment | intentionally out of scope | не начинать до стабилизации collection layer |

## 4. Следующий Порядок Работ

### Stage 1. VPS first-detail measurement

1. Запустить bounded `first-detail` drain на VPS после successful search baseline.
2. Снять detail throughput, retryable failure mix, terminal_404 долю и DB growth.
3. Зафиксировать estimate для cold-start drain и steady-state weekly operation.

Безопасный первый measurement:

```bash
LIMIT=100 make vps-first-detail-measurement
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

### Stage 2. Alert payload hardening

1. Добавить в Telegram message run id, failed step, error type/message, coverage, pending/failed partitions.
2. Для grouped alerts показывать distinct labels и top failing operations, а не только `alerts: N`.
3. Для backup/offsite alerts показывать последний artifact и operator command.
4. Документировать operator action для scheduler stale, failed partitions, backup stale, first-detail failures.

Status на 2026-05-15:

- Alertmanager route добавлен.
- `alert-webhook` receiver добавлен.
- Telegram delivery на VPS проверен.
- Payloads слишком общие: по `HHRUPlatformOperationFailures` и `HHRUPlatformFailedPartitionsPresent` без CLI/log context тяжело понять root cause.

Go/no-go:

- alerts реально доходят вне Grafana UI;
- alert message даёт run id / scope / следующий operator action.

### Stage 3. Transport / resume hardening

1. Классифицировать HH `5xx` search responses как retryable transport failures.
2. Уточнить terminal status mapping для `completed_with_unresolved` и `completed_with_detail_errors`.
3. Не превращать transient DNS/network outage в потерю почти готового baseline.

Status на 2026-05-15:

- `run-once-v2` переочередит failed search partitions только для transport failures, пока не достигнет `3` consecutive или `5` total failures.
- `resume-run-v2` использует тот же budget при повторном прохождении failed/unresolved branches.
- CLI summary показывает `search_transport_failures_total` и `search_captcha_failures_total`.
- На VPS единичный HH `502` прошёл как `VacancySearchNormalizationError` / `bad_gateway`, а `search_transport_failures_total` остался `0`; это нужно исправить, чтобы 5xx не требовали ручного resume.

Go/no-go:

- единичный transport leaf failure не требует full rerun;
- operator summary объясняет, что именно произошло;
- targeted resume path остаётся штатным.

### Stage 4. Backup offsite gap

1. Команда `sync-backup-offsite` / `make backup-offsite` добавлена для `.state/backups/*.dump`.
2. Проверить upload свежего post-baseline DB backup в offsite storage.
3. Зафиксировать retention policy: сколько backup dumps держим локально и offsite.

Наблюдение 2026-05-15: `export-retention-archive` и `sync-retention-archive-offsite` успешно отработали, но `candidate_bundle_count=0`. Это проверяет retention archive path, а не offsite-копию свежего DB backup.

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
