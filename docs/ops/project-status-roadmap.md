# Project Status And Roadmap

Дата среза: 2026-05-23.

Этот документ является короткой точкой входа после перерывов между сессиями. Детальные runbook-и остаются в соседних ops-документах, но текущий статус и следующий порядок работ фиксируются здесь.

## 0. Как Читать Ops Docs

Если нужно быстро восстановить контекст, читать в таком порядке:

1. `project-status-roadmap.md` - текущий статус, порядок работ и ближайшие решения.
2. `backup-restore-drill.md` - operator runbook для backup, S3 offsite и restore drill.
3. `storage-contours.md` - разделение provider snapshot, PostgreSQL backup и research archive.
4. `first-detail-backlog.md` - detail backlog и worker semantics.
5. `observability.md` - alerts, metrics, Prometheus/Grafana contour.

Остальные документы являются detail plans или historical context. Если они конфликтуют
с этим roadmap, приоритет у этого файла.

## 1. Где Мы Сейчас

Проект находится после successful VPS `search-only` baseline, S3 backup/offsite
restore validation и в фазе supervised `first-detail` throughput/storage experiments.

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
- in-run search transport budget добавлен для `run-once-v2` и `resume-run-v2`: transient failed search partitions переочередятся до лимитов `3` consecutive / `5` total;
- backup offsite для DB dumps проверен end-to-end на Timeweb cold S3: 2.2 GiB dump
  загружен частями за ~82s, повторный запуск idempotent (`skipped_backup_count=1`),
  remote size verification прошёл (`verified_object_count=35`), offsite restore drill
  из S3 copy успешно восстановил core schema в отдельную DB.

Текущий статус не равен production readiness. Корректная формулировка: full search
coverage operationally validated, backup/offsite restore contour operationally
validated, но `first-detail` throughput, storage routine, research archive contour
и unattended production routine ещё не доказаны.

## 2. Что Ещё Не Доказано

- Полный first-detail drain на масштабе baseline.
- Sustained detail throughput/storage growth на длинном supervised run.
- Production-quality Telegram alert payloads: текущие alerts доходят, но мало объясняют причину и scope.
- Backup retention и cleanup routine: backup/offsite contour работает, но retention
  policy и operator cleanup ещё нужно зафиксировать.
- Prometheus retention: текущий Prometheus volume уже может съедать десятки GiB,
  retention должен быть ограничен как технический observability контур.
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
| DB backup offsite | S3 end-to-end validated | Timeweb cold S3 upload, idempotency, remote size verify и offsite restore drill работают |
| Retention archive / offsite sync | partially validated | retention bundle sync работает; нужен S3 backend, inventory и readback drill |
| Observability | foundation ready | metrics, dashboards, alert rules есть |
| Alert delivery | foundation ready | delivery до Telegram проверен; payloads нужно сделать информативнее |
| VPS deploy | validated | search-only pilot completed on Timeweb VPS |
| Research enrichment | intentionally out of scope | не начинать до стабилизации collection layer |

## 4. Current Execution Plan

Непосредственный порядок работ после S3 offsite restore drill:

1. Закрыть backup contour hygiene:
   - факт successful VPS offsite restore drill записан;
   - удалить `hhru_platform_restore_drill`, если DB не нужна для расследования;
   - зафиксировать local/S3 backup retention policy.
2. Ограничить Prometheus retention:
   - Prometheus является техническим observability contour, не research archive;
   - старые графики можно потерять, чтобы не сжигать диск;
   - compose поддерживает `HHRU_PROMETHEUS_RETENTION_TIME=7d` и
     `HHRU_PROMETHEUS_RETENTION_SIZE=8GB`; применить change на VPS и проверить
     размер `hh_collector_prometheus_data`.
3. Вернуться к detail throughput experiments:
   - зафиксировать sustained single-worker baseline для `batch=500`, `interval=60`;
   - применить migration с first-detail claim/lease на VPS;
   - провести controlled 2-worker test только после проверки `ready_backlog`
     и отсутствия duplicate selected rows;
   - фиксировать HH latency, retryable failures, terminal_404, DB growth, disk growth;
   - не включать production search schedule до понимания устойчивого detail режима.
4. Спроектировать research archive v1:
   - raw payload archive как canonical `jsonl.gz`;
   - Parquet только для аналитических normalized datasets;
   - S3 layout, manifests, inventory и readback safety before delete.
5. После этого переходить к supervised week/month unattended routine.

## 5. Detailed Roadmap

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
4. Remote verification добавлен: manifest exists, all parts exist, remote sizes match.
5. Offsite restore drill command добавлен: download parts from S3, assemble dump,
   verify sha256, restore into separate DB.

Наблюдение 2026-05-15: `export-retention-archive` и `sync-retention-archive-offsite` успешно отработали, но `candidate_bundle_count=0`. Это проверяет retention archive path, а не offsite-копию свежего DB backup.

Наблюдение 2026-05-20: одиночный WebDAV `PUT` свежего DB dump-а размером 2.2 GiB
упирался в timeout/зависание. `sync-backup-offsite` переведён на загрузку dump-а
fixed-size частями с manifest v2 и receipt, который учитывает `chunk_size_bytes` и
`part_count`.

Наблюдение 2026-05-23: Timeweb cold S3 снял WebDAV blocker. Dump `2269000643`
bytes загружен в `34` parts по `67108864` bytes примерно за `82s`. Повторный запуск
не грузил данные заново: `uploaded_backup_count=0`, `skipped_backup_count=1`.
Remote verify прошёл: `verified_object_count=35`. Offsite restore drill из S3 copy
прошёл: все `34` parts скачались, assembled dump прошёл checksum, restore drill
incremented `hhru_restore_drill_run_total{status="succeeded"}`, core tables в
`hhru_platform_restore_drill` проверены.

### Stage 5. VPS supervised detail drain

1. Включить bounded first-detail drain отдельно от baseline.
2. Расширять batch/interval только после измерения storage и failure mix.
3. First-detail claim/lease реализован как prerequisite для нескольких worker-ов:
   - короткая transaction выбирает candidates через row lock / `SKIP LOCKED`;
   - выбранные rows помечаются `running`, `first_detail_lease_owner` и
     `first_detail_lease_expires_at`;
   - fetch идёт уже после commit, чтобы не держать locks на весь network batch;
   - lease timeout возвращает crashed/aborted rows в ready backlog.
4. Следующий шаг: controlled 2-worker measurement на VPS после migration.
5. Считать first-detail backlog trend.

VPS observation 2026-05-23:

- `batch=500`, `interval=60` стабилен по error mix: retryable failures не растут,
  terminal_404 около `1-2%`;
- sustained duration после нескольких часов около `950-1130s` на batch `500`,
  то есть примерно `1.6k-2k selected/hour`;
- restart worker-а не сбросил duration, поэтому это больше похоже на sustained
  upstream/time-of-day latency, чем на локальный leak;
- claim/lease снимает known duplicate-selection blocker, но длительная
  параллельность ещё должна быть проверена controlled 2-worker run-ом.

VPS observation 2026-05-24:

- controlled 2-worker run после claim/lease прошёл safety checks:
  `expired_leases=0`, `failed_states=0`, duplicate selection не наблюдался;
- throughput gain от `scale=2` не подтвердился: observed drain остался около
  `800-900/hour`, то есть не лучше single-worker baseline;
- в client code нет явного общего detail rate limiter: `HHApiClient` использует
  sync `urlopen`, а `fetch_vacancy_detail` делает `5s` sleep только после
  transport failure retry;
- текущая leading hypothesis: HH/upstream/IP/auth/network path имеет общий
  sustained budget, который несколько worker-ов делят между собой.

VPS observation 2026-05-25:

- `detail-worker --once --batch-size 100` under profiler completed `100` details
  in about `15s`, so the active fetch path is not inherently limited to
  `~600-1300/hour`;
- sustained service-mode still showed lower throughput (`~1260/hour`) and growing
  gaps, while profile exposed significant `metrics._mutating_state` overhead;
- root cause candidate tightened to high-cardinality upstream metrics:
  detail requests were recorded under real `/vacancies/<hh_id>` endpoints, causing
  the file-backed metrics state to grow with every vacancy. The fix is to collapse
  detail upstream metrics to `/vacancies/{vacancy_id}` and compact existing state
  on read/write.

Next experiment plan:

1. Не делить backlog на lanes/run/priority до ясной telemetry картины.
2. Измерить latency/gap baseline на `scale=1`, `batch=100`, application token.
3. Сравнить `scale=1/2/3` при одинаковом batch и 60-90 минутном window.
4. Сравнить `batch=50/100/250/500` отдельно от scale.
5. Проверить search interference: detail-worker on/off during controlled search.

Go/no-go:

- first-detail backlog убывает быстрее, чем растёт;
- search baseline не деградирует из-за detail drain;
- alert delivery уже включена.
- parallelism не создаёт duplicate detail fetches for the same selected rows.

### Stage 6. Week / Month unattended

1. Провести supervised week.
2. Проверить backup, housekeeping, offsite sync, alerts, disk growth.
3. Только после этого переходить к месячному unattended окну.

## 6. Практический Вывод

Проект уже прошёл главный feasibility risk: собрать near-snapshot hh.ru search-space в рамках одного длинного sweep принципиально возможно.

Следующий риск уже не архитектурный, а операционный: устойчивость к внешним outage, понятная recovery semantics, storage planning, alert delivery и proof, что first-detail backlog можно дренировать без деградации search contour.

Связанные документы:

- [current-readiness.md](/home/yurizinyakov/projects/hh_collector/docs/ops/current-readiness.md)
- [vps-pilot-checklist.md](/home/yurizinyakov/projects/hh_collector/docs/ops/vps-pilot-checklist.md)
- [first-detail-backlog.md](/home/yurizinyakov/projects/hh_collector/docs/ops/first-detail-backlog.md)
- [hh-api-completeness-implementation-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/hh-api-completeness-implementation-plan.md)
- [testing-plan.md](/home/yurizinyakov/projects/hh_collector/docs/ops/testing-plan.md)
